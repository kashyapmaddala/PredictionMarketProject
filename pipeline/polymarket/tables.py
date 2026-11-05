"""Build the clean Parquet tables from the raw files, one UTC hour at a time.

    data/clean/polymarket/<table>/date=YYYY-MM-DD/hour=HH/part.parquet

How it stays correct and cheap:
- One replay engine (replay.py) rebuilds every book. After each hour its whole state is saved to
  data/clean/polymarket/_state/<day>T<hour>.json.gz together with a fingerprint of that hour's raw
  files (name, size, modification time) and of the files written. The next hour starts from that
  state, so building hour by hour gives exactly the same rows as one long pass.
- Only hours that changed are rebuilt: the first hour with no state, a different
  NORMALIZER_VERSION, or raw files that changed, and every hour after it (state carries forward).
- Memory is bounded: rows are written to Parquet in row groups as they are made, never held for a
  whole hour.
- Each hour is first written to a staging folder, checked (schema, required columns, allowed values,
  prices between 0 and 1, row counts on disk), then moved into place. The state file is written
  last, so a crash mid-build just means that hour is rebuilt next time.
- Rebuilding gives byte-identical files (fixed row-group size, no timestamps in the files).
- The hour still being written is skipped unless include_current is set; then it is built as a
  partial hour (and no state is saved, so it is rebuilt properly once finished).
"""
from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import shutil
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ..config import VENUE, Paths
from ..raw_reader import RawFile, raw_hours
from .events import BadField, number
from .replay import Engine
from .schemas import (
    COLUMNS,
    HOURLY_TABLES,
    MARKETS_SCHEMA,
    NORMALIZER_VERSION,
    SCHEMAS,
    OutputError,
    check_table,
)

log = logging.getLogger(__name__)

ROW_GROUP = 100_000  # rows per Parquet row group (fixed, so rebuilds are byte-identical)
CHUNK = 20_000  # rows held as Python tuples before turning them into compact Arrow arrays
FINISHED_AFTER = timedelta(minutes=2)  # an hour counts as finished this long after it ends
STATE_FORMAT = 2


class HourWriter:
    """Receives rows from the engine and streams them into one Parquet file per table.

    Rows arrive as Python tuples (about 1 KB each with their Decimals); every CHUNK rows they are
    turned into compact Arrow arrays, and every ROW_GROUP rows written out, so memory stays small
    at any hour size."""

    def __init__(self, folder: Path):
        self.folder = folder
        self.buffers: dict[str, list] = {name: [] for name in HOURLY_TABLES}
        self.chunks: dict[str, list] = {name: [] for name in HOURLY_TABLES}
        self.chunked: dict[str, int] = dict.fromkeys(HOURLY_TABLES, 0)
        self.writers: dict[str, pq.ParquetWriter] = {}
        self.counts: dict[str, int] = dict.fromkeys(HOURLY_TABLES, 0)

    def add(self, table: str, row: tuple) -> None:
        buffer = self.buffers[table]
        buffer.append(row)
        if len(buffer) >= CHUNK:
            self.convert(table)

    def convert(self, table: str) -> None:
        rows = self.buffers[table]
        if not rows:
            return
        arrow = to_arrow(table, rows)
        check_table(table, arrow)
        self.chunks[table].append(arrow)
        self.chunked[table] += len(rows)
        self.buffers[table] = []
        if self.chunked[table] >= ROW_GROUP:
            self.write(table)

    def write(self, table: str) -> None:
        if not self.chunks[table]:
            return
        arrow = pa.concat_tables(self.chunks[table])
        writer = self.writers.get(table)
        if writer is None:
            path = self.folder / table / "part.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            writer = self.writers[table] = pq.ParquetWriter(path, SCHEMAS[table], compression="zstd")
        writer.write_table(arrow, row_group_size=ROW_GROUP)
        self.counts[table] += arrow.num_rows
        self.chunks[table], self.chunked[table] = [], 0

    def close(self) -> dict[str, int]:
        for table in HOURLY_TABLES:
            self.convert(table)
            self.write(table)
        for writer in self.writers.values():
            writer.close()
        self.writers = {}
        return self.counts

    def abort(self) -> None:
        for writer in self.writers.values():
            try:
                writer.close()
            except Exception:  # noqa: BLE001 (already failing; keep the first error)
                pass
        self.writers = {}


def to_arrow(table: str, rows: list[tuple]) -> pa.Table:
    """Rows (tuples in COLUMNS order) -> an Arrow table with the exact schema, plus venue and version."""
    columns = list(zip(*rows, strict=True))
    arrays = [pa.array(values, type=kind) for values, (_name, kind) in zip(columns, COLUMNS[table], strict=True)]
    arrays.append(pa.array([VENUE] * len(rows), pa.string()))
    arrays.append(pa.array([NORMALIZER_VERSION] * len(rows), pa.int32()))
    return pa.Table.from_arrays(arrays, schema=SCHEMAS[table])


# ---------- state files ----------

def state_path(paths: Paths, day: str, hour: str) -> Path:
    return paths.state / f"{day}T{hour}.json.gz"


def fingerprint(files: list[Path]) -> list[list]:
    out = []
    for path in files:
        stat = path.stat()
        out.append([f"{path.parent.name}/{path.name}", stat.st_size, stat.st_mtime_ns])
    return out


def write_atomic(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with open(temporary, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temporary, path)


def save_state(path: Path, record: dict) -> None:
    text = json.dumps(record, sort_keys=True, separators=(",", ":")).encode()
    write_atomic(path, gzip.compress(text, mtime=0))


def load_state(path: Path) -> dict | None:
    try:
        return json.loads(gzip.decompress(path.read_bytes()))
    except (OSError, ValueError, EOFError):
        return None


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------- hours ----------

def partition(paths: Paths, table: str, day: str, hour: str) -> Path:
    return paths.clean / table / f"date={day}" / f"hour={hour}"


def hour_finished(day: str, hour: str, now: datetime) -> bool:
    start = datetime.strptime(f"{day} {hour}", "%Y-%m-%d %H").replace(tzinfo=UTC)
    return now >= start + timedelta(hours=1) + FINISHED_AFTER


def build_hour(paths: Paths, engine: Engine, day: str, hour: str, files: list[Path]) -> dict:
    """Feed one hour's raw files to the engine and write that hour's tables. Returns counts."""
    staging = paths.clean / ".staging" / f"{day}T{hour}"
    if staging.exists():
        shutil.rmtree(staging)
    writer = HourWriter(staging)
    engine.sink = writer
    damage = []
    try:
        for path in files:
            name = f"{path.parent.name}/{path.name}"
            raw = RawFile(path)
            for line_number, envelope in raw:
                engine.feed(envelope, name, line_number)
            damage += [d.describe() for d in raw.damage]
            if raw.bad_lines:
                damage.append(f"{name}: {raw.bad_lines} unreadable lines")
        start = datetime.strptime(f"{day} {hour}", "%Y-%m-%d %H").replace(tzinfo=UTC)
        engine.settle(int((start + timedelta(hours=1)).timestamp()) * 1_000_000_000)
        counts = writer.close()
    except BaseException:
        writer.abort()
        shutil.rmtree(staging, ignore_errors=True)
        raise
    finally:
        engine.sink = None
    outputs = {}
    for table in HOURLY_TABLES:
        built = staging / table / "part.parquet"
        if counts[table]:
            meta = pq.read_metadata(built)
            if meta.num_rows != counts[table] or not meta.schema.to_arrow_schema().equals(SCHEMAS[table]):
                raise OutputError(f"{table} {day} {hour}: file on disk does not match what was written")
            outputs[table] = {"rows": counts[table], "sha256": digest(built)}
    # Swap the new hour in, table by table (the state file, written after, marks the hour done)
    for table in HOURLY_TABLES:
        target = partition(paths, table, day, hour)
        if target.exists():
            shutil.rmtree(target)
        built = staging / table
        if counts[table]:
            target.parent.mkdir(parents=True, exist_ok=True)
            os.replace(built, target)
    shutil.rmtree(staging, ignore_errors=True)
    return {"counts": counts, "outputs": outputs, "damage": damage}


def build(paths: Paths, rebuild: bool = False, include_current: bool = False,
          now: datetime | None = None) -> dict:
    """Build every hour that needs it. Returns a summary."""
    now = now or datetime.now(UTC)
    started = time.monotonic()
    if rebuild:
        for table in [*HOURLY_TABLES, "markets", "_state", ".staging"]:
            shutil.rmtree(paths.clean / table, ignore_errors=True)
    hours = raw_hours(paths.raw)
    finished = {key: files for key, files in hours.items() if hour_finished(*key, now)}
    partial = [key for key in hours if key not in finished]
    summary = {"built": [], "skipped": 0, "partial": [], "rows": dict.fromkeys(HOURLY_TABLES, 0), "damage": []}

    engine = Engine()
    previous_state = None
    keys = list(finished)
    start = len(keys)
    for i, (day, hour) in enumerate(keys):
        record = load_state(state_path(paths, day, hour))
        ok = (record is not None and record.get("format") == STATE_FORMAT
              and record.get("normalizer_version") == NORMALIZER_VERSION
              and record.get("inputs") == fingerprint(finished[(day, hour)])
              and record.get("previous") == previous_state)
        if not ok:
            start = i
            break
        previous_state = f"{day}T{hour}"
    if start > 0:
        record = load_state(state_path(paths, *keys[start - 1]))
        engine = Engine.from_state(record["engine"])
    summary["skipped"] = start

    for day, hour in keys[start:]:
        files = finished[(day, hour)]
        before = fingerprint(files)
        result = build_hour(paths, engine, day, hour, files)
        if fingerprint(files) != before:
            raise RuntimeError(f"raw files for {day} {hour}:00 changed while being read; run again")
        record = {"format": STATE_FORMAT, "normalizer_version": NORMALIZER_VERSION, "hour": f"{day}T{hour}",
                  "previous": previous_state, "inputs": before, "outputs": result["outputs"],
                  "damage": result["damage"], "engine": engine.state()}
        save_state(state_path(paths, day, hour), record)
        previous_state = f"{day}T{hour}"
        summary["built"].append((day, hour, result["counts"]))
        summary["damage"] += result["damage"]
        for table, n in result["counts"].items():
            summary["rows"][table] += n
        log.info("%s %s:00 UTC  %s", day, hour, ", ".join(f"{n} {c:,}" for n, c in result["counts"].items() if c))

    if include_current and partial:
        # Hours not finished yet: built from the latest state, with every waiting check run at the
        # end, and no state saved (they will be rebuilt properly once finished)
        for day, hour in partial:
            result = build_hour(paths, engine, day, hour, hours[(day, hour)])
            summary["partial"].append((day, hour, result["counts"]))
        engine.finish()
        log.info("built %d unfinished hour(s) as partial", len(partial))
    elif partial:
        for day, hour in partial:
            log.info("%s %s:00 UTC  still being written, skipped (use --current to include it)", day, hour)

    summary["markets"] = build_markets(paths)
    summary["seconds"] = time.monotonic() - started
    return summary


# ---------- markets ----------

def _decimal(value) -> Decimal | None:
    try:
        return number(None if value is None else str(value), "value", required=False)
    except BadField:
        return None


def build_markets(paths: Paths) -> int:
    """One table of every market from every pick (small, rebuilt every time)."""
    rows = []
    for path in sorted(paths.universe.glob("20*.json")):
        universe = json.loads(path.read_text())
        for m in universe["markets"]:
            for i, outcome in enumerate(m["outcomes"]):
                rows.append({
                    "picked_at": universe["picked_at"], "rank": m["rank"], "token_id": outcome["token_id"],
                    "outcome": outcome["name"], "outcome_index": i, "market_id": m.get("condition_id"),
                    "gamma_market_id": None if m.get("market_id") is None else str(m["market_id"]),
                    "question": m.get("question"), "slug": m.get("slug"),
                    "event_id": None if m.get("event_id") is None else str(m["event_id"]),
                    "event_slug": m.get("event_slug"), "event_title": m.get("event_title"),
                    "tick_size": _decimal(m.get("tick_size")), "min_order_size": _decimal(m.get("min_order_size")),
                    "neg_risk": bool(m.get("neg_risk")), "end_date": m.get("end_date"),
                    "volume_24h": float(m.get("volume_24h") or 0), "liquidity": float(m.get("liquidity") or 0),
                    "venue": VENUE, "normalizer_version": NORMALIZER_VERSION})
    folder = paths.clean / "markets"
    target = folder / "markets.parquet"
    if not rows:
        return 0
    folder.mkdir(parents=True, exist_ok=True)
    temporary = folder / "markets.parquet.tmp"
    pq.write_table(pa.Table.from_pylist(rows, schema=MARKETS_SCHEMA), temporary, compression="zstd")
    os.replace(temporary, target)
    return len(rows)
