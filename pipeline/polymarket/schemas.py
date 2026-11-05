"""The clean tables: their columns, and checks that our own output is right (fail loud).

Number format. Prices and sizes are exact decimals, never floats:

    PRICE = SIZE = decimal128(18, 6)    MID = decimal128(18, 7)

Why scale 6: a scan of all 22 million raw messages from 2026-11-02 to 2026-11-05 found prices
with at most 3 decimals (tick 0.001), book sizes with at most 2 and trade sizes with at most 6.
Six is also the protocol's own limit: Polymarket amounts are USDC and outcome tokens, both with 6
decimals on chain, and its smallest tick is 0.0001. Why precision 18: the largest size seen had
8 digits before the point; 18 digits leaves 12 (up to 10**12 shares), and decimals of width 18 or
less are stored by Parquet and DuckDB as plain 64-bit integers (fast). mid = (bid + ask) / 2 can
need one more decimal than its inputs, so it gets scale 7; spread = ask - bid needs none.
A value that does not fit is never rounded: the parser turns its event into an unsupported_events
row instead (see events.py).
"""
from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

NORMALIZER_VERSION = 2

PRICE = pa.decimal128(18, 6)
SIZE = pa.decimal128(18, 6)
MID = pa.decimal128(18, 7)

_STR, _I64, _I32 = pa.string(), pa.int64(), pa.int32()

# Columns every hourly table ends with. venue and normalizer_version are the same for every row;
# source_file/source_line say exactly which raw line a row came from.
_TAIL = [("conn", _STR), ("source_file", _STR), ("source_line", _I64)]
CONSTANT_COLUMNS = [("venue", _STR), ("normalizer_version", _I32)]

# The columns of each hourly table, in order (rows are built as tuples in this order).
# The first columns are the same as in version 1, so old queries keep working.
COLUMNS: dict[str, list[tuple[str, pa.DataType]]] = {
    "book_deltas": [
        ("exchange_ts", _I64), ("recv_ts_ns", _I64), ("token_id", _STR), ("market_id", _STR),
        ("side", _STR), ("price", PRICE), ("new_size", SIZE), ("reported_best_bid", PRICE),
        ("reported_best_ask", PRICE), ("hash", _STR), ("change_index", _I32), *_TAIL],
    "book_snapshots": [
        ("snapshot_id", _STR), ("source", _STR), ("exchange_ts", _I64), ("recv_ts_ns", _I64),
        ("token_id", _STR), ("market_id", _STR), ("side", _STR), ("price", PRICE), ("size", SIZE),
        ("tick_size", PRICE), ("hash", _STR), *_TAIL],
    "trades": [
        ("exchange_ts", _I64), ("recv_ts_ns", _I64), ("token_id", _STR), ("market_id", _STR),
        ("price", PRICE), ("size", SIZE), ("taker_side", _STR), ("fee_rate_bps", SIZE),
        ("transaction_hash", _STR), *_TAIL],
    "top_of_book": [
        ("exchange_ts", _I64), ("recv_ts_ns", _I64), ("token_id", _STR),
        ("best_bid", PRICE), ("best_bid_size", SIZE), ("best_ask", PRICE), ("best_ask_size", SIZE),
        ("mid", MID), ("spread", PRICE), ("bid_levels", _I32), ("ask_levels", _I32),
        ("cause", _STR), *_TAIL],
    "resyncs": [
        ("exchange_ts", _I64), ("recv_ts_ns", _I64), ("token_id", _STR), ("levels_wrong", _I32),
        ("example", _STR), ("source", _STR), *_TAIL],
    "collector_events": [
        ("recv_ts_ns", _I64), ("kind", _STR), ("details", _STR), ("session", _STR), *_TAIL],
    "unsupported_events": [
        ("recv_ts_ns", _I64), ("envelope_kind", _STR), ("event_type", _STR), ("reason", _STR),
        ("payload", _STR), ("session", _STR), *_TAIL],
}

HOURLY_TABLES = list(COLUMNS)

SCHEMAS = {name: pa.schema(cols + CONSTANT_COLUMNS) for name, cols in COLUMNS.items()}

MARKETS_SCHEMA = pa.schema([
    ("picked_at", _STR), ("rank", _I64), ("token_id", _STR), ("outcome", _STR),
    ("outcome_index", _I64), ("market_id", _STR), ("gamma_market_id", _STR), ("question", _STR),
    ("slug", _STR), ("event_id", _STR), ("event_slug", _STR), ("event_title", _STR),
    ("tick_size", PRICE), ("min_order_size", SIZE), ("neg_risk", pa.bool_()), ("end_date", _STR),
    # Gamma's own floating-point statistics (dollars), not prices or sizes
    ("volume_24h", pa.float64()), ("liquidity", pa.float64()),
    ("venue", _STR), ("normalizer_version", _I32)])

# Columns that must never be empty, per table
REQUIRED = {
    "book_deltas": ["exchange_ts", "recv_ts_ns", "token_id", "side", "price", "new_size", "source_file", "source_line"],
    "book_snapshots": ["snapshot_id", "source", "exchange_ts", "recv_ts_ns", "token_id", "side", "price", "size"],
    "trades": ["exchange_ts", "recv_ts_ns", "token_id", "price"],
    "top_of_book": ["recv_ts_ns", "token_id", "bid_levels", "ask_levels", "cause"],
    "resyncs": ["exchange_ts", "recv_ts_ns", "token_id", "levels_wrong", "source"],
    "collector_events": ["recv_ts_ns", "kind"],
    "unsupported_events": ["recv_ts_ns", "envelope_kind", "reason", "payload"],
}
ALLOWED = {
    ("book_deltas", "side"): {"bid", "ask"},
    ("book_snapshots", "side"): {"bid", "ask"},
    ("book_snapshots", "source"): {"live", "rest"},
    ("trades", "taker_side"): {"buy", "sell"},
    ("top_of_book", "cause"): {"change", "full_book", "resync", "failover"},
    ("resyncs", "source"): {"rest", "live_book"},
}
PRICE_COLUMNS = {
    "book_deltas": ["price", "reported_best_bid", "reported_best_ask"],
    "book_snapshots": ["price", "tick_size"],
    "trades": ["price"],
    "top_of_book": ["best_bid", "best_ask", "mid"],
}


class OutputError(RuntimeError):
    """Our own output broke one of its rules. The build stops instead of writing it."""


def check_table(name: str, table: pa.Table) -> None:
    """Fail loud if a table we are about to write is not exactly what the schema promises."""
    if not table.schema.equals(SCHEMAS[name]):
        raise OutputError(f"{name}: schema differs from the expected schema:\n{table.schema}")
    if table.num_rows == 0:
        return
    for column in REQUIRED[name]:
        nulls = table.column(column).null_count
        if nulls:
            raise OutputError(f"{name}.{column}: {nulls} empty values in a required column")
    for (table_name, column), allowed in ALLOWED.items():
        if table_name == name:
            values = set(pc.unique(table.column(column).drop_null()).to_pylist())
            if not values <= allowed:
                raise OutputError(f"{name}.{column}: unexpected values {sorted(values - allowed)}")
    for column in PRICE_COLUMNS.get(name, []):
        values = table.column(column).drop_null()
        if len(values) and (pc.min(values).as_py() < 0 or pc.max(values).as_py() > 1):
            raise OutputError(f"{name}.{column}: a price outside 0 to 1")
    if name == "top_of_book":
        both = pc.and_(pc.is_valid(table.column("best_bid")), pc.is_valid(table.column("best_ask")))
        spread = pc.filter(table.column("spread"), both)
        if spread.null_count:
            raise OutputError("top_of_book.spread: missing where both best prices exist")
