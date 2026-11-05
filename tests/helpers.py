"""Small builders for raw envelopes and Polymarket messages used across the tests."""
from __future__ import annotations

import json

from pipeline.raw_store import RawStore

T0_NS = 1_793_700_000_000_000_000  # 2026-11-03 14:40:00 UTC
T0_MS = T0_NS // 1_000_000
TOKEN_A = "1" * 70
TOKEN_B = "2" * 70
MARKET = "0xmarket"


def book(token=TOKEN_A, stamp=T0_MS, bids=(("0.40", "10"),), asks=(("0.60", "20"),), tick="0.01", market=MARKET):
    # Polymarket lists bids lowest first and asks highest first (best price last)
    return {"event_type": "book", "market": market, "asset_id": token, "timestamp": str(stamp),
            "bids": [{"price": p, "size": s} for p, s in bids],
            "asks": [{"price": p, "size": s} for p, s in asks],
            "tick_size": tick, "hash": "h"}


def change(token=TOKEN_A, side="BUY", price="0.40", size="5", best_bid="0.40", best_ask="0.60"):
    return {"asset_id": token, "side": side, "price": price, "size": size,
            "best_bid": best_bid, "best_ask": best_ask, "hash": "c"}


def price_change(*changes, stamp=T0_MS, market=MARKET):
    return {"event_type": "price_change", "market": market, "timestamp": str(stamp),
            "price_changes": list(changes)}


def trade(token=TOKEN_A, stamp=T0_MS, price="0.5", size="10", side="BUY"):
    return {"event_type": "last_trade_price", "market": MARKET, "asset_id": token, "timestamp": str(stamp),
            "price": price, "size": size, "side": side, "fee_rate_bps": "0", "transaction_hash": "0xabc"}


def rest(*books):
    return json.dumps(list(books))


def env(kind, raw, ns, session=None, conn=None):
    """An envelope dict like the raw store writes."""
    out = {"recv_ts_ns": ns, "kind": kind}
    if session is not None:
        out["session"] = session
    if conn is not None:
        out["conn"] = conn
    out["raw"] = raw
    return out


def msg(event_or_list, ns, session=None, conn=None):
    return env("message", json.dumps(event_or_list), ns, session, conn)


class Feed:
    """Writes a scripted stream of envelopes to a RawStore with controlled timestamps."""

    def __init__(self, folder, compress=True, session="s1"):
        self.store = RawStore(folder, compress=compress, session=session)

    def write(self, kind, raw, ns, conn=None):
        self.store.write(kind, raw, ns, conn=conn)

    def close(self):
        self.store.close()


class ListSink:
    """Collects the engine's rows in memory: {table: [row tuples]}."""

    def __init__(self):
        self.rows = {}

    def add(self, table, row):
        self.rows.setdefault(table, []).append(row)

    def get(self, table):
        return self.rows.get(table, [])


def run_engine(envelopes, sink=True, finish=True):
    from pipeline.polymarket.replay import Engine
    engine = Engine(ListSink() if sink else None)
    for i, e in enumerate(envelopes, start=1):
        engine.feed(e, "test.jsonl", i)
    if finish:
        engine.finish()
    return engine


def scripted_session(folder, start_ns, minutes=3, session="s1", stop=True):
    """Write a small, self-consistent collector session (new format) to raw files under
    `folder`: started, one connection with an initial dump, changes every 5 s (with Polymarket's
    best prices computed from a model book), trades, a REST copy, a drop and a reconnect with a
    fresh dump, and a stopped note. Validating it must PASS. Returns the last time written."""
    from decimal import Decimal

    from pipeline.raw_store import RawStore
    store = RawStore(folder, compress=True, session=session)
    s = 1_000_000_000
    model = {"0.40": "10"}  # TOKEN_A bids; asks stay at 0.60 x 20
    store.write("started", {"session": session, "format": 2}, start_ns)

    def best():
        return str(max(Decimal(p) for p in model)) if model else "0"

    def connect(t, conn):
        store.write("connected", {"shard": 1, "url": "wss://test"}, t, conn=conn)
        store.write("subscribed", {"shard": 1, "request": {"type": "market", "assets_ids": [TOKEN_A, TOKEN_B]}}, t + 1, conn=conn)
        stamp = t // 1_000_000
        bids = sorted(model.items(), key=lambda kv: Decimal(kv[0]))
        store.write("message", json.dumps([book(TOKEN_A, stamp, bids=bids), book(TOKEN_B, stamp, bids=(("0.30", "5"),),
                                                                               asks=(("0.70", "5"),))]), t + 2, conn=conn)

    conn = 1
    connect(start_ns + 1, conn)
    t = start_ns
    for i in range(minutes * 12):  # every 5 seconds
        t = start_ns + (i + 1) * 5 * s
        stamp = t // 1_000_000 - 50
        price, size = f"0.{41 + i % 4}", str(i % 3)
        if size == "0":
            model.pop(price, None)
        else:
            model[price] = size
        store.write("message", json.dumps(price_change(change(TOKEN_A, "BUY", price, size, best(), "0.60"), stamp=stamp)), t, conn=conn)
        if i % 4 == 0:
            store.write("message", json.dumps(trade(TOKEN_A, stamp, "0.5", str(i + 1))), t + 10, conn=conn)
        if i == 7:
            store.write("rest_books", rest(book(TOKEN_B, stamp, bids=(("0.30", "5"),), asks=(("0.70", "5"),))), t + 20)
        if i == 15:
            store.write("disconnected", {"shard": 1, "reason": "no close frame received or sent"}, t + 30, conn=conn)
            conn += 1
            connect(t + 40, conn)
    if stop:
        store.write("stopped", {"reason": "SIGTERM"}, t + 100)
    store.close()
    return t + 100
