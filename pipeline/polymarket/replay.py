"""Replay saved raw lines in order, rebuild every order book, check it, and emit table rows.

One engine is used by both the validator (validate.py: counts and examples) and the table builder
(tables.py: rows). Feeding the same lines always gives the same result, and the whole state can be
saved and loaded between hours (state()/from_state()), so building hour by hour gives exactly the
same tables as one long pass.

Connections. Every raw line written since format 2 carries "session" (one collector run) and
"conn" (one WebSocket connection in that run). Books are kept per connection: books[conn][token].
For each token one live connection is the "owner"; only its messages become table rows and are
checked. Other connections for the same token (an overlapping connection during a universe switch,
or a hot standby) keep their own books silently, so when the owner dies, ownership moves to a
standby that already has a correct book and nothing is missed ("failover"). Older lines have no
connection id: they all belong to one connection called "legacy", reset on every new subscription.

Rules learned from real data (each one has a regression test in tests/test_regressions.py):
- Changes made in the same exchange millisecond can arrive spread over several entries and
  messages; each reports Polymarket's best prices after ALL of them. So the reported prices are
  checked once that token's batch (same timestamp) is finished.
- A REST copy can arrive before or after live updates from later than its own moment: rewind the
  book to the copy's moment, or wait until an update from after it shows we caught up.
- A REST copy stamped t may have been taken between two changes stamped t: any state the book
  passed through during millisecond t counts as a match.
- A full "book" message can arrive after a change that is newer than it (2026-11-05 02:36:46): it
  is checked against the book at its own moment instead of replacing newer data.
"""
from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal

from ..book import ASK, BID, Book, diff
from .events import (
    BookEvent,
    PriceChange,
    TickSizeChange,
    Trade,
    Unsupported,
    parse_message,
    parse_rest_books,
)

LEGACY = "legacy"
MAX_EXAMPLES = 5
# REST copies are stamped by a different Polymarket server than the live feed: on 2026-11-03/05
# about 0.4% of them matched our book only at a moment 1 to 8 ms away from their own timestamp
# (for example a copy stamped ...680000 that already held the change stamped ...680001). A copy
# that matches any state within this many milliseconds of its stamp is not a disagreement.
REST_TOLERANCE_MS = 10
LATE_MS = 30_000  # a message received this long after its exchange time came late (sleep, backlog)
HOUR_NS = 3_600_000_000_000
BATCH_SETTLED_NS = 5_000_000_000  # a batch untouched this long is finished when a new hour starts
STATE_VERSION = 1

# Problem names (counted as failures by the validator)
P_BEST = "rebuilt book differs from Polymarket's best prices"
P_REST = "rebuilt book differs from Polymarket's REST copy"
P_CROSSED = "crossed book (best buy >= best sell)"
P_TICK = "price not on the tick"
P_EDGE = "price at 0 or 1"
P_TRADE = "trade price outside 0 to 1"
P_BAD = "message with a bad field (saved to unsupported_events)"
P_CLOCK = "receive clock went backwards by more than 1 s"
P_STALE_DIFF = "out-of-order full book disagreed with our book (corrected)"
C_HEALED = "best-price mismatches gone at that token's next update (a one-off in Polymarket's report)"
C_PERSISTENT = "best-price mismatches still there at the next update"
# Warnings (listed, not failures)
W_UNKNOWN = "unknown message type (saved to unsupported_events)"
W_LATE = "messages received more than 30 s after their exchange time"
W_CLOCK = "receive clock stepped back by up to 1 s (time sync adjusting the computer's clock)"


def when(ns: int | None) -> str:
    if ns is None:
        return "?"
    return datetime.fromtimestamp(ns / 1e9, tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def short(token: str | None) -> str:
    return f"{(token or '?')[:12]}..."


@dataclass
class Conn:
    """What we know about one WebSocket connection."""

    live: bool = True
    opened_ns: int = 0
    last_ns: int = 0  # last line received on it
    last_exchange_ms: int = 0  # newest exchange time seen on it

    def to_state(self) -> list:
        return [self.live, self.opened_ns, self.last_ns, self.last_exchange_ms]

    @classmethod
    def from_state(cls, data: list) -> Conn:
        return cls(*data)


@dataclass
class Outage:
    """A time when some tokens had no live book."""

    who: str
    reason: str
    start_ns: int  # best estimate of when data stopped (exchange time of the last thing seen)
    note_ns: int  # when the drop was noticed
    tokens: int = 0
    end_ns: int | None = None  # first fresh book afterwards
    remaining: int = 0  # tokens still without a book

    def to_state(self) -> list:
        return [self.who, self.reason, self.start_ns, self.note_ns, self.tokens, self.end_ns, self.remaining]

    @classmethod
    def from_state(cls, data: list) -> Outage:
        return cls(*data)


@dataclass
class Stats:
    """Counts and examples for the validator. Not part of the saved state."""

    count: Counter = field(default_factory=Counter)
    problems: Counter = field(default_factory=Counter)
    warnings: Counter = field(default_factory=Counter)
    examples: dict = field(default_factory=dict)
    lag_buckets: Counter = field(default_factory=Counter)  # exchange-to-receive delay histogram
    outages: list = field(default_factory=list)  # finished Outage objects
    first_ns: int | None = None
    last_ns: int | None = None
    open_mismatch: dict = field(default_factory=dict)  # token -> example, waiting for its next batch
    clock_behind: bool = False  # inside a stretch where the receive clock went backwards
    subscribed: set = field(default_factory=set)  # every token we asked for
    booked: set = field(default_factory=set)  # every token that got at least one full book

    def note(self, counter: Counter, name: str, text: str) -> None:
        counter[name] += 1
        examples = self.examples.setdefault(name, [])
        if len(examples) < MAX_EXAMPLES:
            examples.append(text)

    def problem(self, name: str, text: str) -> None:
        self.note(self.problems, name, text)

    def warning(self, name: str, text: str) -> None:
        self.note(self.warnings, name, text)


LAG_EDGES_MS = [0, 50, 100, 250, 500, 1000, 2000, 5000, 30_000, 300_000]


def lag_bucket(lag_ms: int) -> int:
    for edge in reversed(LAG_EDGES_MS):
        if lag_ms >= edge:
            return edge
    return -1  # negative: our clock was behind the exchange's


class Engine:
    """Feed raw envelopes in order with feed(); rows go to `sink.add(table, row_tuple)`."""

    def __init__(self, sink=None):
        self.sink = sink
        self.stats = Stats()
        self.books: dict[str, dict[str, Book]] = {}  # conn -> token -> Book
        self.owner: dict[str, str] = {}  # token -> conn whose book is used
        self.conns: dict[str, Conn] = {}
        self.pending: dict[str, list] = {}  # token -> [stamp, recv_ns, best_bid, best_ask, conn, src, line]
        self.waiting: dict[str, list] = {}  # token -> [REST copy state, recv_ns]
        self.ticks: dict[str, Decimal] = {}
        self.open_outages: dict[int, Outage] = {}  # outage number -> outage still going on
        self.blind: dict[str, int] = {}  # token -> number of the outage it is blind in
        self.next_outage = 0
        self.session: str | None = None
        self.stopped_at: int | None = None
        self.last_recv = 0
        self.src = ("", 0)  # (source_file, source_line) of the line being processed

    # ---------- saving and loading ----------

    def state(self) -> dict:
        return {
            "version": STATE_VERSION,
            "books": {c: {t: b.to_state() for t, b in sorted(bs.items())} for c, bs in sorted(self.books.items())},
            "owner": dict(sorted(self.owner.items())),
            "conns": {c: v.to_state() for c, v in sorted(self.conns.items())},
            "pending": {t: [p[0], p[1], _s(p[2]), _s(p[3]), p[4], p[5], p[6]] for t, p in sorted(self.pending.items())},
            "waiting": {t: w for t, w in sorted(self.waiting.items())},
            "ticks": {t: str(v) for t, v in sorted(self.ticks.items())},
            "open_outages": {str(i): o.to_state() for i, o in sorted(self.open_outages.items())},
            "blind": dict(sorted(self.blind.items())),
            "next_outage": self.next_outage,
            "session": self.session,
            "stopped_at": self.stopped_at,
            "last_recv": self.last_recv,
        }

    @classmethod
    def from_state(cls, data: dict, sink=None) -> Engine:
        if data.get("version") != STATE_VERSION:
            raise ValueError(f"saved replay state has version {data.get('version')}, expected {STATE_VERSION}")
        engine = cls(sink)
        engine.books = {c: {t: Book.from_state(b) for t, b in bs.items()} for c, bs in data["books"].items()}
        engine.owner = dict(data["owner"])
        engine.conns = {c: Conn.from_state(v) for c, v in data["conns"].items()}
        engine.pending = {t: [p[0], p[1], _d(p[2]), _d(p[3]), p[4], p[5], p[6]] for t, p in data["pending"].items()}
        engine.waiting = {t: list(w) for t, w in data["waiting"].items()}
        engine.ticks = {t: Decimal(v) for t, v in data["ticks"].items()}
        engine.open_outages = {int(i): Outage.from_state(o) for i, o in data["open_outages"].items()}
        engine.blind = dict(data["blind"])
        engine.next_outage = data["next_outage"]
        engine.session = data["session"]
        engine.stopped_at = data["stopped_at"]
        engine.last_recv = data["last_recv"]
        return engine

    # ---------- the main entry point ----------

    def feed(self, envelope: dict, source_file: str = "", source_line: int = 0) -> None:
        recv, kind, raw = envelope["recv_ts_ns"], envelope["kind"], envelope["raw"]
        self.src = (source_file, source_line)
        stats = self.stats
        stats.count["lines read"] += 1
        if stats.first_ns is None:
            stats.first_ns = recv
        stats.last_ns = recv
        if recv < self.last_recv - 1_000_000:  # more than 1 ms backwards
            if not stats.clock_behind:  # count each step back once, not every line until it catches up
                behind = (self.last_recv - recv) / 1e9
                text = f"{when(recv)} is {behind:.3f}s before the latest line before it ({source_file}:{source_line})"
                if behind > 1:
                    stats.problem(P_CLOCK, text)
                else:
                    stats.warning(W_CLOCK, text)
            stats.clock_behind = True
        else:
            stats.clock_behind = False
        if recv // HOUR_NS > self.last_recv // HOUR_NS:
            self.settle(recv // HOUR_NS * HOUR_NS)
        self.last_recv = max(self.last_recv, recv)
        session = envelope.get("session")
        conn_id = envelope.get("conn")
        if session is None:
            conn = LEGACY
        else:
            conn = f"{session}:{conn_id}" if conn_id is not None else None
        if kind == "message":
            self.message(raw, recv, conn or LEGACY, session)
        elif kind == "rest_books":
            self.rest_books(raw, recv, session)
        else:
            self.note(kind, raw, recv, session, conn)

    def settle(self, hour_start_ns: int) -> None:
        """A new hour starts: finish every batch received more than BATCH_SETTLED_NS before it, so
        its row lands in the hour it belongs to (a token's batch otherwise waits for that token's
        next change, maybe hours later). Batches still open near the boundary carry on."""
        for token in sorted(t for t, p in self.pending.items() if p[1] < hour_start_ns - BATCH_SETTLED_NS):
            self.finish_batch(token)

    def finish(self) -> None:
        """End of the data: run every check still waiting for more data."""
        # Sorted everywhere rows can come out of a loop: a saved and reloaded state has its dicts in
        # sorted order, a live run in arrival order, and both must give the same rows
        for token in sorted(self.pending):
            self.finish_batch(token)
        for token in sorted(self.waiting):
            self.copy_due(token, None)
        for outage in self.open_outages.values():
            self.stats.outages.append(outage)
        if self.stopped_at is not None:
            self.stats.outages.append(Outage("collector", "stopped (not restarted yet)", self.stopped_at, self.stopped_at))
        self.open_outages, self.blind = {}, {}

    # ---------- rows ----------

    def row(self, table: str, *values) -> None:
        if self.sink is not None:
            self.sink.add(table, values + self.src_cols())

    def src_cols(self) -> tuple:
        return self.src

    # ---------- collector notes ----------

    def note(self, kind: str, raw, recv: int, session: str | None, conn: str | None) -> None:
        stats = self.stats
        stats.count[f"note: {kind}"] += 1
        details = raw if isinstance(raw, str) else json.dumps(raw, sort_keys=True, ensure_ascii=False)
        info = raw if isinstance(raw, dict) else {}
        if self.sink is not None:
            self.sink.add("collector_events", (recv, kind, details, session, conn) + self.src)
        if kind == "subscribed":
            request = info.get("request")
            if isinstance(request, dict) and isinstance(request.get("assets_ids"), list):
                stats.subscribed.update(str(t) for t in request["assets_ids"])
        if session is None:
            self.legacy_note(kind, info, recv)
            return
        if kind == "started":
            self.session_started(session, recv)
        elif kind == "connected" and conn:
            self.session_started(session, recv)
            self.conns[conn] = Conn(True, recv, recv, 0)
            self.books.setdefault(conn, {})
        elif kind in ("disconnected", "retired") and conn:
            self.conn_down(conn, recv, info.get("reason") or kind, expected=kind == "retired")
        elif kind == "stopped":
            self.stop_everything(recv, info.get("reason") or "stopped")
            self.stopped_at = recv
        elif kind == "paused":
            seconds = info.get("seconds")
            if isinstance(seconds, (int, float)) and seconds > 0:
                outage = Outage("collector", "collector paused (computer asleep?)", recv - int(seconds * 1e9), recv, 0, recv)
                stats.outages.append(outage)

    def session_started(self, session: str, recv: int) -> None:
        if self.session == session:
            return
        # A new collector run. Anything still live from an older run died without a goodbye.
        crashed = [c for c, v in self.conns.items() if v.live and not c.startswith(f"{session}:")]
        if crashed and self.stopped_at is None:
            self.stop_everything(recv, "collector restarted without a stop note (killed or crashed)", crash=True)
        elif self.stopped_at is not None:
            self.stats.outages.append(Outage("collector", "collector was not running", self.stopped_at, self.stopped_at, 0, recv))
        self.stopped_at = None
        self.session = session

    def legacy_note(self, kind: str, info: dict, recv: int) -> None:
        """Notes written before connection ids existed (one connection, called "legacy")."""
        conn = self.conns.get(LEGACY)
        if kind == "connected":
            if conn and conn.live and self.stopped_at is None:
                # Connected again with no drop or stop note: the collector was killed or crashed
                self.stop_everything(recv, "collector restarted without a stop note (killed or crashed)", crash=True)
            elif self.stopped_at is not None:
                self.stats.outages.append(Outage("collector", "collector was not running", self.stopped_at, self.stopped_at, 0, recv))
            self.stopped_at = None
            self.conns[LEGACY] = Conn(True, recv, recv, 0)
            self.books.setdefault(LEGACY, {})
        elif kind == "subscribed":
            # A fresh connection: books from before are stale until its fresh full copy arrives
            request = info.get("request") if isinstance(info.get("request"), dict) else {}
            tokens = request.get("assets_ids") or []
            books = self.books.setdefault(LEGACY, {})
            for token in tokens:
                if token in books:
                    self.forget(LEGACY, token)
        elif kind == "disconnected":
            self.conn_down(LEGACY, recv, info.get("reason") or "disconnected")
        elif kind == "stopped":
            self.stop_everything(recv, info.get("reason") or "stopped")
            self.stopped_at = recv
        elif kind == "universe_changed":
            # The old collector reconnected with a new list: every book is stale
            if conn:
                self.conn_down(LEGACY, recv, "market list changed", expected=True)

    # ---------- connections coming and going ----------

    def stop_everything(self, recv: int, reason: str, crash: bool = False) -> None:
        for conn in sorted(c for c, v in self.conns.items() if v.live):
            # For a crash the drop really happened at the last line saved before this one
            when_noticed = self.conns[conn].last_ns if crash else recv
            self.conn_down(conn, when_noticed, reason, expected=not crash)

    def conn_down(self, conn: str, recv: int, reason: str, expected: bool = False) -> None:
        info = self.conns.get(conn)
        if info is None or not info.live:
            return
        info.live = False
        books = self.books.get(conn, {})
        orphans = []
        for token in sorted(books):
            if self.owner.get(token) != conn:
                continue
            self.finish_batch(token)
            saved = self.waiting.get(token)
            if saved is not None and info.last_exchange_ms > saved[0]["stamp"] + REST_TOLERANCE_MS:
                self.copy_due(token, None)  # we saw data past the copy's moment: it can be checked
            elif self.waiting.pop(token, None) is not None:
                self.stats.count["REST copies dropped (connection lost before they could be checked)"] += 1
            heir = self.standby_for(token)
            if heir:
                self.owner[token] = heir
                self.stats.count["failovers to a standby connection"] += 1
                self.top_row(token, self.books[heir][token].last_stamp, recv, "failover", heir)
            else:
                del self.owner[token]
                orphans.append(token)
        self.books.pop(conn, None)
        self.conns.pop(conn, None)
        if orphans and not expected:
            start = recv
            if info.last_exchange_ms:
                start = min(recv, info.last_exchange_ms * 1_000_000)
            self.open_outage(Outage("connection", reason, start, recv), orphans)
        elif orphans:
            self.stats.count["tokens dropped on purpose (list change or retired connection)"] += len(orphans)
            for token in orphans:
                number = self.blind.pop(token, None)
                if number is not None:
                    self.release(number)

    def open_outage(self, outage: Outage, tokens: list[str]) -> None:
        outage.tokens = outage.remaining = len(tokens)
        number = self.next_outage
        self.next_outage += 1
        self.open_outages[number] = outage
        for token in tokens:
            old = self.blind.get(token)
            if old is not None:
                self.release(old)
            self.blind[token] = number

    def release(self, number: int) -> None:
        outage = self.open_outages[number]
        outage.remaining -= 1
        if outage.remaining <= 0:
            del self.open_outages[number]
            self.stats.outages.append(outage)

    def covered(self, token: str, recv: int) -> None:
        """A token has a live book again. Its outage ends when the first token is back (that is
        when the new connection started delivering), and is closed once every token is back."""
        number = self.blind.pop(token, None)
        if number is None:
            return
        outage = self.open_outages[number]
        if outage.end_ns is None:
            outage.end_ns = recv
        self.release(number)

    def standby_for(self, token: str) -> str | None:
        for conn, info in sorted(self.conns.items()):
            if info.live and conn != self.owner.get(token):
                book = self.books.get(conn, {}).get(token)
                if book is not None and book.started:
                    return conn
        return None

    def forget(self, conn: str, token: str) -> None:
        if self.owner.get(token) == conn:
            self.finish_batch(token)
            self.waiting.pop(token, None)
        book = self.books.get(conn, {}).get(token)
        if book is not None:
            book.started = False

    def touch(self, conn: str, recv: int, stamp: int | None) -> Conn:
        info = self.conns.get(conn)
        if info is None:
            # Messages with no "connected" note before them (data starting mid-stream)
            info = self.conns[conn] = Conn(True, recv, recv, 0)
            self.books.setdefault(conn, {})
        info.last_ns = recv
        if stamp and stamp > info.last_exchange_ms:
            info.last_exchange_ms = stamp
        return info

    def owns(self, conn: str, token: str) -> bool:
        """Is `conn` the token's owner? A token with no owner is claimed by the first connection
        that has a started book for it."""
        owner = self.owner.get(token)
        return owner is None or owner == conn

    # ---------- live messages ----------

    def message(self, raw, recv: int, conn: str, session: str | None) -> None:
        for event in parse_message(raw):
            if isinstance(event, PriceChange):
                self.price_change(event, recv, conn)
            elif isinstance(event, BookEvent):
                self.live_book(event, recv, conn)
            elif isinstance(event, Trade):
                self.trade(event, recv, conn)
            elif isinstance(event, TickSizeChange):
                self.tick_change(event, recv, conn)
            else:
                self.unsupported(event, "message", recv, session, conn)

    def late(self, stamp: int, recv: int) -> None:
        lag = recv // 1_000_000 - stamp
        self.stats.lag_buckets[lag_bucket(lag)] += 1
        if lag > LATE_MS:
            self.stats.warning(W_LATE, f"{when(recv)} received {lag / 1000:,.0f}s after its exchange time")

    def unsupported(self, event: Unsupported, envelope_kind: str, recv: int, session, conn) -> None:
        known = event.kind in ("book", "price_change", "last_trade_price", "tick_size_change", "rest_books")
        text = f"{when(recv)} {event.kind}: {event.reason}"
        if known or event.kind in ("not_json", "not_an_object"):
            self.stats.problem(P_BAD, text)
        else:
            self.stats.warning(W_UNKNOWN, text)
        if self.sink is not None:
            self.sink.add("unsupported_events",
                          (recv, envelope_kind, event.kind, event.reason, event.payload, session, conn) + self.src)

    def price_change(self, event: PriceChange, recv: int, conn: str) -> None:
        stamp = event.stamp
        self.stats.count["price_change"] += 1
        self.touch(conn, recv, stamp)
        self.late(stamp, recv)
        for change in event.changes:
            token = change.token
            book = self.books[conn].get(token)
            if not self.owns(conn, token):
                # A standby connection: keep its book right, but it is not the record
                if book is not None and book.started:
                    book.apply(change.side, change.price, change.size, stamp)
                self.stats.count["standby changes (not recorded twice)"] += 1
                continue
            self.row("book_deltas", stamp, recv, token, event.market, change.side, change.price, change.size,
                     change.best_bid, change.best_ask, change.hash, change.index, conn)
            if book is None or not book.started:
                self.stats.count["changes before first full book (skipped)"] += 1
                continue
            self.copy_due(token, stamp)
            waiting = self.pending.get(token)
            if waiting and waiting[0] != stamp:
                self.finish_batch(token)
            self.check_price(token, book, change.price, recv)
            book.apply(change.side, change.price, change.size, stamp)
            self.pending[token] = [stamp, recv, change.best_bid, change.best_ask, conn, *self.src]

    def check_price(self, token: str, book: Book, price: Decimal, recv: int) -> None:
        tick = book.tick or self.ticks.get(token)
        if price <= 0 or price >= 1:
            self.stats.problem(P_EDGE, f"{when(recv)} token {short(token)} price {price}")
        elif tick and price % tick != 0:
            self.stats.problem(P_TICK, f"{when(recv)} token {short(token)} price {price}, tick {tick}")

    def live_book(self, event: BookEvent, recv: int, conn: str) -> None:
        token, stamp = event.token, event.stamp
        self.stats.count["book"] += 1
        self.touch(conn, recv, stamp)
        # No lateness check: a full book is stamped with the time of its last change, not the time
        # it was sent (the initial dump has books last changed minutes or hours ago)
        books = self.books[conn]
        book = books.get(token)
        self.stats.booked.add(token)
        if event.tick is not None:
            self.ticks[token] = event.tick
        if book is not None and book.started and stamp < book.last_stamp:
            self.stale_book(event, book, recv, conn)
            return
        if book is None:
            book = books[token] = Book()
        claimed = self.owner.get(token) is None
        if claimed:
            self.owner[token] = conn
        if self.owner[token] == conn:
            self.copy_due(token, stamp)
            self.finish_batch(token)
            book.replace(event.bids, event.asks, stamp, event.tick or self.ticks.get(token))
            self.snapshot_rows("live", event, recv, conn)
            self.top_row(token, stamp, recv, "full_book", conn)
            self.covered(token, recv)
            self.sanity(token, book, recv)
        else:
            book.replace(event.bids, event.asks, stamp, event.tick or self.ticks.get(token))
            self.stats.count["standby full books (not recorded twice)"] += 1

    def stale_book(self, event: BookEvent, book: Book, recv: int, conn: str) -> None:
        """A full book older than changes we already applied. Check it against our book at its own
        moment; if it disagrees, trust it and replay our newer changes on top."""
        self.stats.count["out-of-order full books"] += 1
        token, stamp = event.token, event.stamp
        owner = self.owner.get(token) == conn
        if owner:
            self.snapshot_rows("live", event, recv, conn)
        if not book.can_rewind_to(stamp):
            self.stats.count["out-of-order full books too old to check (ignored)"] += 1
            return
        if any(bids == event.bids and asks == event.asks for bids, asks in book.states_at(stamp)):
            self.stats.count["out-of-order full books that matched"] += 1
            return
        ours = book.as_of(stamp)
        wrong = diff(ours, (event.bids, event.asks))
        if owner:
            self.stats.problem(P_STALE_DIFF, f"{when(recv)} token {short(token)} book at {stamp}: {len(wrong)} levels differ: {'; '.join(wrong[:3])}")
            self.finish_batch(token)
            book.resync(event.bids, event.asks, stamp)
            self.row("resyncs", stamp, recv, token, len(wrong), "; ".join(wrong[:3]), "live_book", conn)
            self.top_row(token, stamp, recv, "resync", conn)
        else:
            book.resync(event.bids, event.asks, stamp)

    def trade(self, event: Trade, recv: int, conn: str) -> None:
        self.stats.count["last_trade_price"] += 1
        self.touch(conn, recv, event.stamp)
        self.late(event.stamp, recv)
        if event.price <= 0 or event.price >= 1:
            self.stats.problem(P_TRADE, f"{when(recv)} token {short(event.token)} price {event.price}")
        if not self.owns(conn, event.token):
            self.stats.count["standby trades (not recorded twice)"] += 1
            return
        self.row("trades", event.stamp, recv, event.token, event.market, event.price, event.size,
                 event.side, event.fee_rate_bps, event.transaction_hash, conn)

    def tick_change(self, event: TickSizeChange, recv: int, conn: str) -> None:
        self.stats.count["tick_size_change"] += 1
        self.touch(conn, recv, event.stamp)
        self.ticks[event.token] = event.new
        for books in self.books.values():
            book = books.get(event.token)
            if book is not None:
                book.tick = event.new

    # ---------- REST copies ----------

    def rest_books(self, raw, recv: int, session: str | None) -> None:
        self.stats.count["REST replies"] += 1
        for copy in parse_rest_books(raw):
            if isinstance(copy, Unsupported):
                self.unsupported(copy, "rest_books", recv, session, None)
                continue
            self.stats.count["REST book copies"] += 1
            self.snapshot_rows("rest", copy, recv, None)
            token = copy.token
            conn = self.owner.get(token)
            book = self.books.get(conn, {}).get(token) if conn else None
            if book is None or not book.started:
                self.stats.count["REST copies with no live book to compare"] += 1
                continue
            self.copy_due(token, None)  # an older copy still waiting: check it first
            saved = [_copy_state(copy), recv]
            if book.last_stamp > copy.stamp + REST_TOLERANCE_MS:
                self.check_copy(token, saved)
            else:
                self.waiting[token] = saved

    def copy_due(self, token: str, next_stamp: int | None) -> None:
        """Check a waiting REST copy once an update from after its moment arrives (or at the end)."""
        saved = self.waiting.get(token)
        if saved is None:
            return
        if next_stamp is not None and next_stamp <= saved[0]["stamp"] + REST_TOLERANCE_MS:
            return  # this update is not clearly after the copy's moment: apply it first
        del self.waiting[token]
        self.check_copy(token, saved)

    def check_copy(self, token: str, saved: list) -> None:
        copy, recv = saved
        stamp = copy["stamp"]
        conn = self.owner.get(token)
        book = self.books.get(conn, {}).get(token) if conn else None
        if book is None or not book.started:
            return
        stats = self.stats
        if not book.can_rewind_to(stamp):
            stats.count["REST copies too old to compare"] += 1
            return
        theirs = (_levels(copy["bids"]), _levels(copy["asks"]))
        stats.count["REST books compared"] += 1
        states = book.states_at(stamp)
        first = next(states, None)
        if first == theirs:
            stats.count["REST books matched exactly"] += 1
            return
        if any(state == theirs for state in states):
            stats.count["REST books matched exactly"] += 1
            stats.count["REST books matched a state inside their millisecond"] += 1
            return
        low = max(stamp - REST_TOLERANCE_MS, book.horizon)
        if any(state == theirs for state in book.states_between(low, stamp + REST_TOLERANCE_MS)):
            stats.count["REST books matched exactly"] += 1
            stats.count[f"REST books matched a state within {REST_TOLERANCE_MS} ms of their timestamp"] += 1
            return
        wrong = diff(first, theirs)
        stats.problem(P_REST, f"copy at {stamp} token {short(token)} {len(wrong)} levels differ: {'; '.join(wrong[:3])}")
        self.finish_batch(token)
        book.resync(theirs[0], theirs[1], stamp)
        self.row("resyncs", stamp, recv, token, len(wrong), "; ".join(wrong[:3]), "rest", conn)
        self.top_row(token, stamp, recv, "resync", conn)

    # ---------- checks and top-of-book rows ----------

    def finish_batch(self, token: str) -> None:
        """A token's batch of same-timestamp changes is complete: check it and write its row."""
        pending = self.pending.pop(token, None)
        if pending is None:
            return
        stamp, recv, theirs_bid, theirs_ask, conn, src_file, src_line = pending
        book = self.books.get(conn, {}).get(token)
        if book is None:
            return
        stats = self.stats
        stats.count["price changes checked"] += 1
        ours_bid, ours_ask = book.best_bid(), book.best_ask()
        same_bid = ours_bid == theirs_bid or (ours_bid is None and theirs_bid in (None, 0))
        same_ask = ours_ask == theirs_ask or (ours_ask is None and theirs_ask in (None, 0, 1))
        earlier = stats.open_mismatch.pop(token, None)
        if same_bid and same_ask:
            stats.count["matched Polymarket's best prices"] += 1
            if earlier is not None:
                # Our book agrees again at the very next update: the earlier report was a one-off
                # inconsistency in Polymarket's own best prices, not a wrong book
                stats.count[C_HEALED] += 1
        else:
            text = f"{when(recv)} token {short(token)} ours {ours_bid}/{ours_ask}, Polymarket {theirs_bid}/{theirs_ask} ({src_file}:{src_line})"
            stats.problem(P_BEST, text)
            if earlier is not None:
                stats.count[C_PERSISTENT] += 1
            stats.open_mismatch[token] = text
        self.sanity(token, book, recv)
        saved = self.src
        self.src = (src_file, src_line)
        self.top_row(token, stamp, recv, "change", conn)
        self.src = saved

    def sanity(self, token: str, book: Book, recv: int) -> None:
        bid, ask = book.best_bid(), book.best_ask()
        if bid is not None and ask is not None and bid >= ask:
            self.stats.problem(P_CROSSED, f"{when(recv)} token {short(token)} buy {bid} sell {ask}")

    def top_row(self, token: str, stamp: int, recv: int, cause: str, conn: str | None) -> None:
        if self.sink is None:
            return
        book = self.books.get(conn, {}).get(token)
        if book is None:
            return
        bid, ask = book.best_bid(), book.best_ask()
        both = bid is not None and ask is not None
        self.row("top_of_book", stamp, recv, token,
                 bid, book.bids[bid] if bid is not None else None,
                 ask, book.asks[ask] if ask is not None else None,
                 (bid + ask) / 2 if both else None, ask - bid if both else None,
                 len(book.bids), len(book.asks), cause, conn)

    def snapshot_rows(self, source: str, event: BookEvent, recv: int, conn: str | None) -> None:
        if self.sink is None:
            return
        sid = f"{source}:{event.token}:{event.stamp}"
        for side, levels in ((BID, event.bid_levels), (ASK, event.ask_levels)):
            for price, size in levels:
                self.row("book_snapshots", sid, source, event.stamp, recv, event.token, event.market,
                         side, price, size, event.tick, event.hash, conn)


def _s(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _d(value: str | None) -> Decimal | None:
    return None if value is None else Decimal(value)


def _copy_state(copy: BookEvent) -> dict:
    return {"stamp": copy.stamp,
            "bids": [[str(p), str(s)] for p, s in sorted(copy.bids.items())],
            "asks": [[str(p), str(s)] for p, s in sorted(copy.asks.items())]}


def _levels(pairs: list) -> dict:
    return {Decimal(p): Decimal(s) for p, s in pairs}
