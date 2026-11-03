"""Replay one normalized dataset and print final books as JSON.

Usage: python reconstruct.py data/clean/2026-11-03-v1
This offline development reader sorts records in memory. Input must be one
normalizer output from a single collector's chronologically named raw files.
"""

import argparse
import json
from dataclasses import dataclass, field
from decimal import Decimal, localcontext
from pathlib import Path

import pyarrow.parquet as pq

from normalize import VERSION, decimal_text, digest, identifier


def position(row):
    """Capture order, independent of exchange times and local clock adjustments."""
    return (row["source_file"], row["source_line"], row["event_index"],
            row.get("change_index", 0))


def levels(values):
    result = {}
    seen = set()
    for level in values:
        price = Decimal(decimal_text(level["price"], price=True))
        size = Decimal(decimal_text(level["size"]))
        if price in seen:
            raise ValueError(f"duplicate snapshot price: {price}")
        seen.add(price)
        if size:
            result[price] = size
    return result


@dataclass
class Book:
    bids: dict = field(default_factory=dict)
    asks: dict = field(default_factory=dict)
    synchronized: bool = False
    reason: str | None = "awaiting_snapshot"
    last_position: tuple | None = None
    recv_ts_ns: int | None = None
    exchange_ts_ns: int | None = None

    def invalidate(self, reason):
        self.synchronized = False
        self.reason = reason

    def summary(self):
        bid = max(self.bids, default=None) if self.synchronized else None
        ask = min(self.asks, default=None) if self.synchronized else None
        crossed = bid is not None and ask is not None and bid >= ask
        spread = midpoint = None
        if bid is not None and ask is not None and not crossed:
            with localcontext() as context:
                context.prec = max(28, len(bid.as_tuple().digits), len(ask.as_tuple().digits)) + 10
                spread, midpoint = ask - bid, (ask + bid) / 2
        def text(value):
            return None if value is None else format(value, "f")
        return dict(synchronized=self.synchronized,
                    status=self.reason or ("crossed_or_locked" if crossed else "ok"),
                    best_bid=text(bid), best_ask=text(ask), spread=text(spread),
                    midpoint=text(midpoint), last_position=self.last_position,
                    recv_ts_ns=self.recv_ts_ns, exchange_ts_ns=self.exchange_ts_ns,
                    bids=[dict(price=text(p), size=text(s)) for p, s in
                          sorted(self.bids.items(), reverse=True)] if self.synchronized else [],
                    asks=[dict(price=text(p), size=text(s)) for p, s in
                          sorted(self.asks.items())] if self.synchronized else [])


class Reconstructor:
    def __init__(self):
        self.books = {}
        self.last_position = None
        self.issues = []
        self.collection = {}

    def apply(self, table, row):
        if row["normalizer_version"] != VERSION:
            raise ValueError("unsupported normalizer version")
        current = position(row)
        if self.last_position is not None and current <= self.last_position:
            raise ValueError("duplicate or out-of-order capture position")
        if table == "collector_events":
            if row["kind"] in {"connected", "subscribed", "disconnected", "stopped", "universe_changed"}:
                self.collection[row["venue"]] = dict(
                    collection_status=row["kind"],
                    collection_status_recv_ts_ns=row["recv_ts_ns"])
            if row["kind"] in {"connected", "subscribed", "disconnected", "universe_changed"}:
                # Frames lack connection IDs: invalidate all books for this venue.
                for key, book in self.books.items():
                    if key[0] == row["venue"]:
                        book.invalidate(row["kind"])
        elif table == "unsupported_events":
            # Unknown events could alter book state. Fail closed until a snapshot.
            for key, book in self.books.items():
                if key[0] == row["venue"]:
                    book.invalidate("unsupported_event")
            self.issues.append(dict(position=current, reason="unsupported_event", kind=row["kind"]))
        elif table in {"books_snapshots", "book_deltas"}:
            key = tuple(identifier(row[name]) for name in ("venue", "market_id", "asset_id"))
            book = self.books.get(key, Book())
            if table == "books_snapshots":
                # Parse both sides before mutation so a bad snapshot cannot partly reset state.
                bids, asks = levels(row["bids"]), levels(row["asks"])
                book.bids, book.asks = bids, asks
                book.synchronized, book.reason = True, None
            else:
                if row["side"] not in {"bid", "ask"}:
                    raise ValueError("invalid delta side")
                price = Decimal(decimal_text(row["price"], price=True))
                size = Decimal(decimal_text(row["new_size"]))
                if self.collection.get(key[0], {}).get("collection_status") == "stopped":
                    # A later session must resnapshot before extending the old book.
                    book.invalidate("update_after_stop")
                if not book.synchronized:
                    self.issues.append(dict(position=current, key=key, reason="delta_without_snapshot"))
                else:
                    side = book.bids if row["side"] == "bid" else book.asks
                    if size:
                        side[price] = size
                    else:
                        side.pop(price, None)
            book.last_position = current
            book.recv_ts_ns = row["recv_ts_ns"]
            book.exchange_ts_ns = row["exchange_ts_ns"]
            self.books[key] = book
        elif table != "trades":
            raise ValueError(f"unknown normalized table: {table}")
        # Trades do not independently remove depth: the book update does that.
        self.last_position = current

    def result(self):
        return dict(books=[dict(venue=k[0], market_id=k[1], asset_id=k[2],
                               **self.collection.get(k[0], dict(collection_status="unknown",
                                                               collection_status_recv_ts_ns=None)),
                               **b.summary())
                           for k, b in sorted(self.books.items())], issues=self.issues)


def replay(records):
    engine = Reconstructor()
    for table, row in sorted(records, key=lambda item: position(item[1])):
        engine.apply(table, row)
    return engine


def load_records(root):
    root = Path(root).resolve()
    manifest = json.loads((root / "manifest.json").read_text())
    if manifest["normalizer_version"] != VERSION:
        raise ValueError("unsupported manifest version")
    records = []
    for relative, expected_hash in sorted(manifest["outputs"].items()):
        path = (root / relative).resolve()
        if root not in path.parents or digest(path) != expected_hash:
            raise ValueError(f"invalid or modified dataset file: {relative}")
        table = Path(relative).parts[0]
        records.extend((table, row) for row in pq.ParquetFile(path).read().to_pylist())
    if not any(table == "books_snapshots" for table, _ in records):
        raise ValueError("dataset contains no book snapshots")
    return records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", type=Path)
    args = parser.parse_args()
    print(json.dumps(replay(load_records(args.dataset)).result(), indent=2))


if __name__ == "__main__":
    main()
