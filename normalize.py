"""Turn the raw Polymarket files into clean tables (step 5).

Reads data/raw/polymarket/ (exactly what the exchange sent) and writes Parquet tables to
data/clean/polymarket/<table>/date=YYYY-MM-DD/hour=HH/part.parquet, one file per UTC hour.
DuckDB (see query.py) can then search all of them at once.

Tables:
  book_deltas      every change to a book: one row per price level that changed
  book_snapshots   every full book: from the live feed ("live") and from the 5-minute REST
                   copies ("rest"), one row per price level
  trades           every trade: price, size, which side started it, fee, transaction hash
  top_of_book      our rebuilt book after every batch of changes: best buy/sell, their sizes,
                   mid price and spread. When a REST copy disagrees with our rebuilt book, the
                   book is corrected to the copy (and our later changes replayed on top)
  resyncs          every correction like that, with how many levels were wrong
  collector_events the listener's own notes: connected, disconnected, stopped, list changes
  markets          (one file, not per hour) every market we picked, from every pick

Rules (from the README):
- The raw files are never changed. Fixing a bug here means re-running this.
- Every row has normalizer_version. Bump NORMALIZER_VERSION when the output changes.
- Re-running an hour gives the same result: each hour's files are rewritten, never appended.
- Books carry over between hours through a small state file per hour (data/clean/polymarket/_state).

    python3 normalize.py              # build every finished hour not built yet
    python3 normalize.py --rebuild    # rebuild everything from scratch
    python3 normalize.py --current    # also build the hour still being written (partial)
"""
import json
import shutil
import sys
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from check_data import RAW_DIR, Book, dec, events_in, file_order, read_lines

NORMALIZER_VERSION = 1
HERE = Path(__file__).resolve().parent
CLEAN_DIR = HERE / "data" / "clean" / "polymarket"
STATE_DIR = CLEAN_DIR / "_state"
UNIVERSE_DIR = HERE / "data" / "universe" / "polymarket"
VENUE = "polymarket"

SCHEMAS = {
    "book_deltas": pa.schema([
        ("exchange_ts", pa.int64()), ("recv_ts_ns", pa.int64()), ("token_id", pa.string()),
        ("market_id", pa.string()), ("side", pa.string()), ("price", pa.float64()),
        ("new_size", pa.float64()), ("reported_best_bid", pa.float64()),
        ("reported_best_ask", pa.float64()), ("hash", pa.string())]),
    "book_snapshots": pa.schema([
        ("snapshot_id", pa.string()), ("source", pa.string()), ("exchange_ts", pa.int64()),
        ("recv_ts_ns", pa.int64()), ("token_id", pa.string()), ("market_id", pa.string()),
        ("side", pa.string()), ("price", pa.float64()), ("size", pa.float64()),
        ("tick_size", pa.float64()), ("hash", pa.string())]),
    "trades": pa.schema([
        ("exchange_ts", pa.int64()), ("recv_ts_ns", pa.int64()), ("token_id", pa.string()),
        ("market_id", pa.string()), ("price", pa.float64()), ("size", pa.float64()),
        ("taker_side", pa.string()), ("fee_rate_bps", pa.float64()), ("transaction_hash", pa.string())]),
    "top_of_book": pa.schema([
        ("exchange_ts", pa.int64()), ("recv_ts_ns", pa.int64()), ("token_id", pa.string()),
        ("best_bid", pa.float64()), ("best_bid_size", pa.float64()), ("best_ask", pa.float64()),
        ("best_ask_size", pa.float64()), ("mid", pa.float64()), ("spread", pa.float64()),
        ("bid_levels", pa.int32()), ("ask_levels", pa.int32()), ("cause", pa.string())]),
    "resyncs": pa.schema([
        ("exchange_ts", pa.int64()), ("recv_ts_ns", pa.int64()), ("token_id", pa.string()),
        ("levels_wrong", pa.int32()), ("example", pa.string())]),
    "collector_events": pa.schema([
        ("recv_ts_ns", pa.int64()), ("kind", pa.string()), ("details", pa.string())]),
}


def num(text):
    try:
        return float(text) if text not in (None, "") else None
    except (TypeError, ValueError):
        return None


def stamp_of(text):
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


# ---------- which hours exist ----------

def raw_hours():
    """{(date, hour): [files in order]} for every hour with raw data."""
    hours = defaultdict(list)
    for folder in sorted(p for p in RAW_DIR.iterdir() if p.is_dir()):
        for path in sorted(folder.glob("*.jsonl*"), key=file_order):
            hours[(folder.name, f"{file_order(path)[0]:02d}")].append(path)
    return dict(sorted(hours.items()))


def partition(table, date, hour):
    return CLEAN_DIR / table / f"date={date}" / f"hour={hour}"


def state_file(date, hour):
    return STATE_DIR / f"{date}T{hour}.json"


# ---------- book state carried between hours ----------

def save_books(books, path):
    data = {}
    for token, b in books.items():
        if b.started:
            data[token] = {
                "bids": [[str(p), str(s)] for p, s in b.bids.items()],
                "asks": [[str(p), str(s)] for p, s in b.asks.items()],
                "tick": str(b.tick) if b.tick is not None else None,
                "last_stamp": b.last_stamp,
            }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, sort_keys=True))


def load_books(path):
    books = defaultdict(Book)
    if not path.exists():
        return books
    for token, d in json.loads(path.read_text()).items():
        b = books[token]
        b.bids = {Decimal(p): Decimal(s) for p, s in d["bids"]}
        b.asks = {Decimal(p): Decimal(s) for p, s in d["asks"]}
        b.tick = Decimal(d["tick"]) if d["tick"] else None
        b.started = True
        b.replaced_at = b.last_stamp = d["last_stamp"]  # can't rewind past the start of this hour
    return books


# ---------- one hour ----------

class HourBuilder:
    def __init__(self, books):
        self.books = books
        self.rows = {name: [] for name in SCHEMAS}
        self.pending = {}  # token -> (exchange_ts, recv_ns) of a batch not yet written to top_of_book
        self.waiting_copies = {}  # token -> (REST copy, recv_ns) until our book catches up to it

    # top_of_book rows are written once a batch of same-timestamp changes is finished
    def top_row(self, token, stamp, recv_ns, cause):
        b = self.books[token]
        bid, ask = b.best_bid(), b.best_ask()
        bid_f = float(bid) if bid is not None else None
        ask_f = float(ask) if ask is not None else None
        self.rows["top_of_book"].append({
            "exchange_ts": stamp, "recv_ts_ns": recv_ns, "token_id": token,
            "best_bid": bid_f, "best_bid_size": float(b.bids[bid]) if bid is not None else None,
            "best_ask": ask_f, "best_ask_size": float(b.asks[ask]) if ask is not None else None,
            "mid": (bid_f + ask_f) / 2 if bid_f is not None and ask_f is not None else None,
            "spread": ask_f - bid_f if bid_f is not None and ask_f is not None else None,
            "bid_levels": len(b.bids), "ask_levels": len(b.asks), "cause": cause})

    def finish_batch(self, token):
        if token in self.pending:
            stamp, recv_ns = self.pending.pop(token)
            self.top_row(token, stamp, recv_ns, "change")

    def snapshot_rows(self, source, event, recv_ns):
        token, stamp = event.get("asset_id"), stamp_of(event.get("timestamp"))
        sid = f"{source}:{token}:{stamp}"
        for side, levels in (("bid", event.get("bids", [])), ("ask", event.get("asks", []))):
            for level in levels:
                self.rows["book_snapshots"].append({
                    "snapshot_id": sid, "source": source, "exchange_ts": stamp, "recv_ts_ns": recv_ns,
                    "token_id": token, "market_id": event.get("market"), "side": side,
                    "price": num(level.get("price")), "size": num(level.get("size")),
                    "tick_size": num(event.get("tick_size")), "hash": event.get("hash")})

    def copy_due(self, token, next_stamp):
        """Check a waiting REST copy once an update from after its moment arrives."""
        waiting = self.waiting_copies.get(token)
        if not waiting:
            return
        copy, recv_ns = waiting
        if next_stamp is not None and next_stamp <= stamp_of(copy.get("timestamp")):
            return
        del self.waiting_copies[token]
        self.check_copy(token, copy, recv_ns)

    def check_copy(self, token, copy, recv_ns):
        """Compare a REST copy with our book at the copy's moment; if they differ, trust the copy."""
        b = self.books[token]
        stamp = stamp_of(copy.get("timestamp")) or 0
        ours = b.as_of(stamp)
        if ours is None:
            return
        theirs_bids = {dec(l["price"]): dec(l["size"]) for l in copy.get("bids", [])}
        theirs_asks = {dec(l["price"]): dec(l["size"]) for l in copy.get("asks", [])}
        bids, asks = ours
        if theirs_bids == bids and theirs_asks == asks:
            return
        wrong = [f"bid {p}: ours {bids.get(p)}, rest {theirs_bids.get(p)}" for p in set(theirs_bids) | set(bids) if theirs_bids.get(p) != bids.get(p)]
        wrong += [f"ask {p}: ours {asks.get(p)}, rest {theirs_asks.get(p)}" for p in set(theirs_asks) | set(asks) if theirs_asks.get(p) != asks.get(p)]
        self.finish_batch(token)
        b.resync(theirs_bids, theirs_asks, stamp)
        self.rows["resyncs"].append({"exchange_ts": stamp, "recv_ts_ns": recv_ns, "token_id": token,
                                     "levels_wrong": len(wrong), "example": "; ".join(sorted(wrong)[:3])})
        self.top_row(token, stamp, recv_ns, "resync")

    def forget_books(self, tokens=None):
        """A new connection or a stop: books are stale until the next full book arrives."""
        for token in list(self.pending) if tokens is None else [t for t in tokens if t in self.pending]:
            self.finish_batch(token)
        for token in (self.books if tokens is None else tokens):
            self.books[token].started = False
            self.waiting_copies.pop(token, None)

    def line(self, line):
        recv_ns, kind = line["recv_ts_ns"], line["kind"]
        if kind == "message":
            self.message(line["raw"], recv_ns)
        elif kind == "rest_books":
            for copy in json.loads(line["raw"]):
                token = copy.get("asset_id")
                if not token:
                    continue
                self.snapshot_rows("rest", copy, recv_ns)
                b = self.books[token]
                if not b.started:
                    continue
                self.copy_due(token, None)  # an older copy still waiting: check it first
                if b.last_stamp > (stamp_of(copy.get("timestamp")) or 0):
                    self.check_copy(token, copy, recv_ns)
                else:
                    self.waiting_copies[token] = (copy, recv_ns)
        else:
            raw = line["raw"]
            self.rows["collector_events"].append({"recv_ts_ns": recv_ns, "kind": kind,
                                                  "details": raw if isinstance(raw, str) else json.dumps(raw, sort_keys=True)})
            if kind == "subscribed" and isinstance(raw, dict):
                self.forget_books(raw.get("request", {}).get("assets_ids", []))
            elif kind in ("stopped", "universe_changed"):
                self.forget_books()

    def message(self, raw, recv_ns):
        for event in events_in({"raw": raw}):
            kind = event.get("event_type") or ("price_change" if "price_changes" in event else None)
            if kind == "book":
                token, stamp = event["asset_id"], stamp_of(event.get("timestamp"))
                self.copy_due(token, stamp)
                self.pending.pop(token, None)
                self.snapshot_rows("live", event, recv_ns)
                self.books[token].replace(event)
                self.top_row(token, stamp, recv_ns, "full_book")
            elif kind == "price_change":
                stamp = stamp_of(event.get("timestamp"))
                for change in event.get("price_changes", []):
                    token = change["asset_id"]
                    self.copy_due(token, stamp)
                    if token in self.pending and self.pending[token][0] != stamp:
                        self.finish_batch(token)
                    self.rows["book_deltas"].append({
                        "exchange_ts": stamp, "recv_ts_ns": recv_ns, "token_id": token,
                        "market_id": event.get("market"),
                        "side": "bid" if change.get("side") == "BUY" else "ask",
                        "price": num(change.get("price")), "new_size": num(change.get("size")),
                        "reported_best_bid": num(change.get("best_bid")),
                        "reported_best_ask": num(change.get("best_ask")), "hash": change.get("hash")})
                    b = self.books[token]
                    price, size = dec(change.get("price")), dec(change.get("size"))
                    if b.started and price is not None and size is not None:
                        b.change(change["side"], price, size, stamp or 0)
                        self.pending[token] = (stamp, recv_ns)
            elif kind == "last_trade_price":
                self.rows["trades"].append({
                    "exchange_ts": stamp_of(event.get("timestamp")), "recv_ts_ns": recv_ns,
                    "token_id": event.get("asset_id"), "market_id": event.get("market"),
                    "price": num(event.get("price")), "size": num(event.get("size")),
                    "taker_side": (event.get("side") or "").lower() or None,
                    "fee_rate_bps": num(event.get("fee_rate_bps")),
                    "transaction_hash": event.get("transaction_hash")})
            elif kind == "tick_size_change":
                self.books[event["asset_id"]].tick = dec(event.get("new_tick_size"))

    def finish(self):
        for token in list(self.pending):
            self.finish_batch(token)
        for token in list(self.waiting_copies):
            self.copy_due(token, None)


def write_hour(rows, date, hour):
    counts = {}
    for name, schema in SCHEMAS.items():
        folder = partition(name, date, hour)
        if folder.exists():
            shutil.rmtree(folder)  # rewrite the whole hour: re-running gives the same result
        table_rows = rows[name]
        counts[name] = len(table_rows)
        if not table_rows:
            continue
        table = pa.Table.from_pylist(table_rows, schema=schema)
        table = table.append_column("venue", pa.array([VENUE] * len(table)))
        table = table.append_column("normalizer_version", pa.array([NORMALIZER_VERSION] * len(table), pa.int32()))
        folder.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, folder / "part.parquet", compression="zstd")
    return counts


def build_markets():
    """One table of every market from every pick (small, rebuilt every time)."""
    rows = []
    for path in sorted(UNIVERSE_DIR.glob("20*.json")):
        u = json.loads(path.read_text())
        for m in u["markets"]:
            for i, o in enumerate(m["outcomes"]):
                rows.append({
                    "picked_at": u["picked_at"], "rank": m["rank"], "token_id": o["token_id"],
                    "outcome": o["name"], "outcome_index": i, "market_id": m["condition_id"],
                    "gamma_market_id": m["market_id"], "question": m["question"], "slug": m["slug"],
                    "event_id": m["event_id"], "event_slug": m["event_slug"], "event_title": m["event_title"],
                    "tick_size": num(m["tick_size"]), "min_order_size": num(m["min_order_size"]),
                    "neg_risk": bool(m["neg_risk"]), "end_date": m["end_date"],
                    "volume_24h": m["volume_24h"], "liquidity": m["liquidity"],
                    "venue": VENUE, "normalizer_version": NORMALIZER_VERSION})
    folder = CLEAN_DIR / "markets"
    folder.mkdir(parents=True, exist_ok=True)
    if rows:
        pq.write_table(pa.Table.from_pylist(rows), folder / "markets.parquet", compression="zstd")
    return len(rows)


def main():
    rebuild = "--rebuild" in sys.argv
    include_current = "--current" in sys.argv
    if rebuild and CLEAN_DIR.exists():
        shutil.rmtree(CLEAN_DIR)

    now = datetime.now(timezone.utc)
    current = (now.strftime("%Y-%m-%d"), now.strftime("%H"))
    hours = raw_hours()
    previous_state = None
    built = 0
    for (date, hour), files in hours.items():
        if (date, hour) == current and not include_current:
            print(f"{date} {hour}:00 UTC  still being written, skipped (use --current to include it)")
            continue
        done_marker = state_file(date, hour)
        if done_marker.exists() and not rebuild and (date, hour) != current:
            previous_state = done_marker
            continue  # already built
        builder = HourBuilder(load_books(previous_state) if previous_state else defaultdict(Book))
        for path in files:
            for line in read_lines(path):
                builder.line(line)
        builder.finish()
        counts = write_hour(builder.rows, date, hour)
        if (date, hour) != current:  # a partial current hour must be rebuilt next time
            save_books(builder.books, done_marker)
            previous_state = done_marker
        built += 1
        print(f"{date} {hour}:00 UTC  " + ", ".join(f"{n} {c:,}" for n, c in counts.items() if c))
    markets = build_markets()
    print(f"\nBuilt {built} hour(s). Markets table: {markets:,} rows. Tables are in {CLEAN_DIR}")


if __name__ == "__main__":
    main()
