"""Normalize closed Polymarket raw files into versioned Parquet tables."""

import argparse
import gzip
import hashlib
import json
import shutil
import tempfile
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

VERSION = "polymarket-ws-v1"


def decimal_text(value, *, price=False):
    if not isinstance(value, (str, int)) or isinstance(value, bool):
        raise ValueError("expected an exact decimal string or integer")
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("invalid decimal") from exc
    if not number.is_finite() or number < 0 or (price and number > 1):
        raise ValueError("decimal outside permitted range")
    return format(number, "f")


def identifier(value):
    if not isinstance(value, str) or not value:
        raise ValueError("missing or invalid identifier")
    return value


def timestamp(value):
    if value is None:
        return None
    if isinstance(value, bool) or not str(value).isdigit():
        raise ValueError("timestamp must be integer Unix milliseconds")
    result = int(value) * 1_000_000
    if result > 2**63 - 1:
        raise ValueError("timestamp exceeds nanosecond int64 range")
    return result


def normalize(envelope, source_file, source_line):
    """Return (table, row) pairs atomically for one raw envelope.

    Decimal values remain exact strings for lossless Parquet storage. Book
    updates contain absolute new sizes, including zero for level deletion.
    """
    recv = envelope["recv_ts_ns"]
    if type(recv) is not int or not 0 <= recv <= 2**63 - 1:
        raise ValueError("recv_ts_ns must be nonnegative int64")
    base = dict(normalizer_version=VERSION, venue="polymarket",
                source_file=source_file, source_line=source_line, recv_ts_ns=recv)
    if envelope["kind"] != "message":
        return [("collector_events", dict(base, event_index=0,
                 kind=envelope["kind"], payload=json.dumps(envelope["raw"], sort_keys=True)))]
    payload = json.loads(envelope["raw"])
    events = payload if isinstance(payload, list) else [payload]
    rows = []
    for index, event in enumerate(events):
        if not isinstance(event, dict):
            raise ValueError("exchange event must be an object")
        kind = event.get("event_type")
        common = dict(base, event_index=index)
        if kind not in {"book", "price_change", "last_trade_price"}:
            rows.append(("unsupported_events", dict(common, kind=str(kind),
                         payload=json.dumps(event, sort_keys=True))))
            continue
        common.update(market_id=identifier(event.get("market")),
                      exchange_ts_ns=timestamp(event.get("timestamp")))
        if kind == "book":
            levels = {}
            for side in ("bids", "asks"):
                if not isinstance(event[side], list):
                    raise ValueError("snapshot levels must be arrays")
                levels[side] = [dict(price=decimal_text(x["price"], price=True),
                                     size=decimal_text(x["size"])) for x in event[side]]
            rows.append(("books_snapshots", dict(common,
                         asset_id=identifier(event.get("asset_id")),
                         book_hash=event.get("hash"), **levels)))
        elif kind == "price_change":
            if not isinstance(event["price_changes"], list) or not event["price_changes"]:
                raise ValueError("price_changes must be a nonempty array")
            for change_index, change in enumerate(event["price_changes"]):
                if change["side"] not in {"BUY", "SELL"}:
                    raise ValueError("unknown book side")
                rows.append(("book_deltas", dict(common, change_index=change_index,
                             asset_id=identifier(change.get("asset_id")),
                             side={"BUY": "bid", "SELL": "ask"}[change["side"]],
                             price=decimal_text(change["price"], price=True),
                             new_size=decimal_text(change["size"]),
                             book_hash=change.get("hash"))))
        else:
            if event.get("side") not in {None, "BUY", "SELL"}:
                raise ValueError("unknown trade side")
            rows.append(("trades", dict(common,
                         asset_id=identifier(event.get("asset_id")),
                         price=decimal_text(event["price"], price=True),
                         size=None if event.get("size") is None else decimal_text(event["size"]),
                         reported_side=event.get("side"),
                         transaction_hash=event.get("transaction_hash"))))
    return rows


def read_raw(path: Path, source_file: str):
    """Fail with source context; never silently skip malformed raw records."""
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            try:
                rows = normalize(json.loads(line), source_file, line_number)
            except (ValueError, KeyError, TypeError) as exc:
                raise ValueError(f"{source_file}:{line_number}: {exc}") from exc
            yield from rows


def schemas():
    text, integer = pa.string(), pa.int64()
    common = [("normalizer_version", text), ("venue", text), ("source_file", text),
              ("source_line", integer), ("recv_ts_ns", integer), ("event_index", integer)]
    market = common + [("market_id", text), ("exchange_ts_ns", integer), ("asset_id", text)]
    level = pa.list_(pa.struct([("price", text), ("size", text)]))
    return {name: pa.schema(fields) for name, fields in {
        "books_snapshots": market + [("book_hash", text), ("bids", level), ("asks", level)],
        "book_deltas": market + [("change_index", integer), ("side", text),
                                ("price", text), ("new_size", text), ("book_hash", text)],
        "trades": market + [("price", text), ("size", text), ("reported_side", text),
                            ("transaction_hash", text)],
        "collector_events": common + [("kind", text), ("payload", text)],
        "unsupported_events": common + [("kind", text), ("payload", text)],
    }.items()}


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def build(raw_root, output):
    raw_root, output = Path(raw_root).resolve(), Path(output).resolve()
    if output == raw_root or raw_root in output.parents or output in raw_root.parents:
        raise ValueError("raw and output directories must be separate")
    files = sorted(p for p in raw_root.rglob("*")
                   if p.is_file() and (p.name.endswith(".jsonl") or p.name.endswith(".jsonl.gz")))
    if not files:
        raise ValueError("no raw JSONL files found")
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".normalize-", dir=output.parent))
    writers = {}
    buffers = {}
    counts = {}
    table_schemas = schemas()

    def flush(key):
        if key not in writers:
            table, date = key
            folder = stage / table / "venue=polymarket" / f"date={date}"
            folder.mkdir(parents=True)
            writers[key] = pq.ParquetWriter(folder / "part-00000.parquet",
                                            table_schemas[table], compression="zstd")
        writers[key].write_table(pa.Table.from_pylist(buffers[key], schema=table_schemas[key[0]]))
        buffers[key].clear()

    try:
        inputs = {}
        for path in files:
            source = path.relative_to(raw_root).as_posix()
            before = digest(path)
            for table, row in read_raw(path, source):
                date = datetime.fromtimestamp(row["recv_ts_ns"] // 1_000_000_000,
                                              timezone.utc).strftime("%Y-%m-%d")
                key = table, date
                buffers.setdefault(key, []).append(row)
                counts[table] = counts.get(table, 0) + 1
                if len(buffers[key]) >= 4096:
                    flush(key)
            if digest(path) != before:
                raise ValueError(f"raw file changed during normalization: {source}")
            inputs[source] = before
        for key, rows in buffers.items():
            if rows:
                flush(key)
        for writer in writers.values():
            writer.close()
        writers.clear()
        manifest = dict(normalizer_version=VERSION, inputs=inputs, counts=counts,
                        outputs={p.relative_to(stage).as_posix(): digest(p)
                                 for p in sorted(stage.rglob("*.parquet"))})
        (stage / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        if output.exists():
            existing = {p.relative_to(output).as_posix(): digest(p)
                        for p in output.rglob("*") if p.is_file()}
            generated = {p.relative_to(stage).as_posix(): digest(p)
                         for p in stage.rglob("*") if p.is_file()}
            if existing != generated:
                raise ValueError("output differs; use a new output directory for this rebuild")
        else:
            stage.rename(output)
        return counts
    finally:
        for writer in writers.values():
            writer.close()
        if stage.exists():
            shutil.rmtree(stage)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raw_root", type=Path, help="closed Polymarket WebSocket raw files only")
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    print(json.dumps(build(args.raw_root, args.output), sort_keys=True))


if __name__ == "__main__":
    main()
