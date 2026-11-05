"""One order book (one token), with exact Decimal prices and sizes, that can rewind and resync.

Polymarket sends absolute sizes: "the bids at price p now total s" (s = 0 removes the level). So a
book is two dicts {price: size}. Every change is also remembered in a short history
(timestamp, side, price, size before, size after) so the book can be:

- rewound to an earlier exchange time (as_of), to compare with a REST copy that was taken a moment
  before updates we already applied;
- resynced to a trusted copy taken at time t, replaying our own changes from after t on top
  (resync). Used when a REST copy disagrees, and when a full "book" message arrives that is older
  than updates we already applied (Polymarket sometimes sends those out of order; see
  tests/test_regressions.py, the 2026-11-05 02:36:46 case).

The history only keeps HISTORY_MS of exchange time (plus a hard cap), so memory stays bounded at
any number of markets. `horizon` is the earliest time we can still rewind to.

Exchange timestamps are milliseconds and several changes can share one. A copy stamped t may have
been taken between two changes stamped t, so states_at(t) yields every state the book passed
through during millisecond t.
"""
from __future__ import annotations

from collections import deque
from collections.abc import Iterator
from decimal import Decimal

HISTORY_MS = 10 * 60 * 1000  # REST copies arrive within seconds; 10 minutes is plenty
HISTORY_CAP = 50_000  # hard cap on remembered changes per book

Levels = dict  # {Decimal price: Decimal size}
BID, ASK = "bid", "ask"


class Book:
    __slots__ = ("asks", "bids", "history", "horizon", "last_stamp", "started", "tick")

    def __init__(self) -> None:
        self.bids: Levels = {}
        self.asks: Levels = {}
        self.tick: Decimal | None = None
        self.started = False  # only trusted after a full book on the current connection
        self.history: deque = deque()  # (stamp, side, price, before or None, after)
        self.horizon = 0  # cannot rewind to before this exchange time
        self.last_stamp = 0  # newest exchange time applied

    # ---------- changing the book ----------

    def replace(self, bids: Levels, asks: Levels, stamp: int, tick: Decimal | None = None) -> None:
        """A full fresh book: forget everything before it."""
        self.bids, self.asks = dict(bids), dict(asks)
        if tick is not None:
            self.tick = tick
        self.started = True
        self.history.clear()
        self.horizon = self.last_stamp = stamp

    def apply(self, side: str, price: Decimal, size: Decimal, stamp: int) -> None:
        """One level now has this total size (0 removes it)."""
        levels = self.bids if side == BID else self.asks
        self.history.append((stamp, side, price, levels.get(price), size))
        if size == 0:
            levels.pop(price, None)
        else:
            levels[price] = size
        if stamp > self.last_stamp:
            self.last_stamp = stamp
        self._prune()

    def _prune(self) -> None:
        history = self.history
        cutoff = self.last_stamp - HISTORY_MS
        while history and (history[0][0] < cutoff or len(history) > HISTORY_CAP):
            # Forgetting a change means we can no longer rewind to before it
            stamp = history.popleft()[0]
            if stamp > self.horizon:
                self.horizon = stamp

    def resync(self, bids: Levels, asks: Levels, stamp: int) -> None:
        """Match a trusted copy taken at exchange time `stamp`, then replay our own changes from
        after `stamp` (they happened after the copy was taken)."""
        later = [(s, side, p, after) for s, side, p, _before, after in self.history if s > stamp]
        newest = self.last_stamp
        self.bids, self.asks = dict(bids), dict(asks)
        self.history.clear()
        self.horizon = self.last_stamp = stamp
        for s, side, p, after in later:
            self.apply(side, p, after, s)
        self.last_stamp = max(self.last_stamp, newest)

    # ---------- looking back ----------

    def can_rewind_to(self, stamp: int) -> bool:
        return self.started and stamp >= self.horizon

    def as_of(self, stamp: int) -> tuple[Levels, Levels] | None:
        """The book as it was at exchange time `stamp` (after every change stamped <= stamp).
        None if we cannot rewind that far."""
        if not self.can_rewind_to(stamp):
            return None
        bids, asks = dict(self.bids), dict(self.asks)
        for s, side, price, before, _after in reversed(self.history):
            if s <= stamp:
                break
            _undo(bids if side == BID else asks, price, before)
        return bids, asks

    def states_at(self, stamp: int) -> Iterator[tuple[Levels, Levels]]:
        """Every state the book was in during millisecond `stamp`: first after every change stamped
        `stamp` (same as as_of), then with the last of them undone, and so on back to the state
        before the first one. Yields nothing if we cannot rewind that far."""
        state = self.as_of(stamp)
        if state is None:
            return
        bids, asks = state
        yield dict(bids), dict(asks)
        for s, side, price, before, _after in reversed(self.history):
            if s > stamp:
                continue
            if s < stamp:
                break
            _undo(bids if side == BID else asks, price, before)
            yield dict(bids), dict(asks)

    def states_between(self, low: int, high: int) -> Iterator[tuple[Levels, Levels]]:
        """Every state the book was in from exchange time `low` to `high`, newest first (as far
        back as the history allows)."""
        state = self.as_of(high)
        if state is None:
            return
        bids, asks = state
        yield dict(bids), dict(asks)
        for s, side, price, before, _after in reversed(self.history):
            if s > high:
                continue
            if s < low:
                break
            _undo(bids if side == BID else asks, price, before)
            yield dict(bids), dict(asks)

    # ---------- reading ----------

    def best_bid(self) -> Decimal | None:
        return max(self.bids) if self.bids else None

    def best_ask(self) -> Decimal | None:
        return min(self.asks) if self.asks else None

    def same_as(self, bids: Levels, asks: Levels) -> bool:
        return self.bids == bids and self.asks == asks

    # ---------- saving between runs ----------

    def to_state(self) -> dict:
        return {
            "bids": [[str(p), str(s)] for p, s in sorted(self.bids.items())],
            "asks": [[str(p), str(s)] for p, s in sorted(self.asks.items())],
            "tick": None if self.tick is None else str(self.tick),
            "started": self.started,
            "horizon": self.horizon,
            "last_stamp": self.last_stamp,
            "history": [[s, side, str(p), None if b is None else str(b), str(a)]
                        for s, side, p, b, a in self.history],
        }

    @classmethod
    def from_state(cls, data: dict) -> Book:
        book = cls()
        book.bids = {Decimal(p): Decimal(s) for p, s in data["bids"]}
        book.asks = {Decimal(p): Decimal(s) for p, s in data["asks"]}
        book.tick = None if data["tick"] is None else Decimal(data["tick"])
        book.started = data["started"]
        book.horizon = data["horizon"]
        book.last_stamp = data["last_stamp"]
        book.history = deque((s, side, Decimal(p), None if b is None else Decimal(b), Decimal(a))
                             for s, side, p, b, a in data["history"])
        return book


def _undo(levels: Levels, price: Decimal, before: Decimal | None) -> None:
    if before is None:
        levels.pop(price, None)
    else:
        levels[price] = before


def diff(ours: tuple[Levels, Levels], theirs: tuple[Levels, Levels]) -> list[str]:
    """Human-readable list of every level that differs (bids first, then asks, by price)."""
    out = []
    for name, mine, other in (("bid", ours[0], theirs[0]), ("ask", ours[1], theirs[1])):
        for price in sorted(set(mine) | set(other)):
            if mine.get(price) != other.get(price):
                out.append(f"{name} {price}: ours {mine.get(price)}, theirs {other.get(price)}")
    return out
