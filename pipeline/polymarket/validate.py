"""Check that the saved Polymarket data is right, streaming, in bounded memory.

Replays the raw files with the same engine the table builder uses (replay.py) and reports:

1. Rebuilt books vs Polymarket's own numbers: the best prices reported with every price change,
   and the independent REST copies (every level).
2. Sanity: crossed books, prices off the tick or at 0/1, trade prices, messages with bad fields.
3. Gaps: every time tokens had no live book, with the real blind time (from the exchange time of
   the last thing received before the drop to the first fresh book after it), the time the
   collector was paused (computer asleep), and messages that arrived late.
4. Files: damaged or truncated raw files, unreadable lines.

Memory does not grow with the amount of data: books, a few minutes of history per book and fixed
size counters only. Validating one day starts from the table builder's saved state for the hour
before it, if there is one (so books carried over from the day before are known).
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ..config import Paths
from ..raw_reader import RawFile, raw_files
from . import replay
from .replay import LAG_EDGES_MS, Engine, when
from .tables import load_state, state_path


@dataclass
class FileReport:
    damage: list = field(default_factory=list)  # Damage objects
    bad_lines: int = 0
    bad_examples: list = field(default_factory=list)
    files: int = 0


@dataclass
class Report:
    engine: Engine
    files: FileReport
    started_from: str | None
    text: str = ""
    failed: bool = False


def starting_state(paths: Paths, files: list[Path]) -> tuple[Engine, str | None]:
    """The builder's saved state for the hour just before the first file, if it exists."""
    if not files:
        return Engine(), None
    first = files[0]
    hour = datetime.strptime(f"{first.parent.name} {first.name[:2]}", "%Y-%m-%d %H").replace(tzinfo=UTC)
    before = hour - timedelta(hours=1)
    path = state_path(paths, f"{before:%Y-%m-%d}", f"{before:%H}")
    record = load_state(path) if path.exists() else None
    if record and record.get("engine"):
        try:
            return Engine.from_state(record["engine"]), record.get("hour")
        except (ValueError, KeyError):
            pass
    return Engine(), None


def validate(paths: Paths, day: str | None = None) -> Report:
    files = raw_files(paths.raw, day)
    engine, started_from = starting_state(paths, files) if day else (Engine(), None)
    report = FileReport()
    for path in files:
        name = f"{path.parent.name}/{path.name}"
        raw = RawFile(path)
        for number, envelope in raw:
            engine.feed(envelope, name, number)
        report.files += 1
        report.damage += raw.damage
        report.bad_lines += raw.bad_lines
        report.bad_examples += [f"{name}: {e}" for e in raw.bad_examples][: max(0, 5 - len(report.bad_examples))]
    engine.finish()
    result = Report(engine, report, started_from)
    result.text, result.failed = render(result)
    return result


def lag_summary(buckets: Counter) -> str:
    total = sum(buckets.values())
    if not total:
        return "  (no exchange timestamps)"
    parts = []
    labels = {-1: "< 0 (our clock behind)"}
    edges = [-1, *LAG_EDGES_MS]
    for i, edge in enumerate(edges):
        if not buckets.get(edge):
            continue
        if edge in labels:
            label = labels[edge]
        elif i + 1 < len(edges):
            label = f"{edge:,}-{edges[i + 1]:,} ms"
        else:
            label = f">= {edge:,} ms"
        parts.append(f"  {label:>22}: {buckets[edge]:>12,} ({100 * buckets[edge] / total:.3f}%)")
    return "\n".join(parts)


def render(result: Report) -> tuple[str, bool]:
    stats = result.engine.stats
    count = stats.count
    out = ["Polymarket data check", "=" * 21]
    if stats.first_ns is None:
        return "No saved data found.", False
    hours = (stats.last_ns - stats.first_ns) / 3.6e12
    out.append(f"Data from {when(stats.first_ns)} to {when(stats.last_ns)} ({hours:.1f} hours span)")
    out.append(f"Files: {result.files.files}, lines: {count['lines read']:,}")
    if result.started_from:
        out.append(f"Books carried over from the saved state after {result.started_from}")
    out.append(f"Messages: {count['book']:,} full books, {count['price_change']:,} price-change events, "
               f"{count['last_trade_price']:,} trades, {count['tick_size_change']:,} tick size changes")

    out += ["", "CHECK 1: rebuilt books vs Polymarket's own numbers"]
    checked, matched = count["price changes checked"], count["matched Polymarket's best prices"]
    if checked:
        out.append(f"  {matched:,} of {checked:,} change batches matched Polymarket's best prices ({100 * matched / checked:.4f}%)")
    compared = count["REST books compared"]
    if compared:
        exact = count["REST books matched exactly"]
        out.append(f"  {exact:,} of {compared:,} REST book copies matched our rebuilt book exactly, every level ({100 * exact / compared:.2f}%)")
        if count["REST books matched a state inside their millisecond"]:
            out.append(f"    ({count['REST books matched a state inside their millisecond']:,} of them were taken between two changes in the same millisecond)")
        near = f"REST books matched a state within {replay.REST_TOLERANCE_MS} ms of their timestamp"
        if count[near]:
            out.append(f"    ({count[near]:,} of them matched our book a few ms away from their own timestamp: REST's clock differs slightly)")
    else:
        out.append("  (no REST book copies to compare)")
    for name in ("REST copies too old to compare", "REST copies with no live book to compare",
                 "changes before first full book (skipped)", "out-of-order full books",
                 "out-of-order full books that matched", "out-of-order full books too old to check (ignored)",
                 "failovers to a standby connection", "standby changes (not recorded twice)"):
        if count[name]:
            out.append(f"  {name}: {count[name]:,}")
    for name in (replay.C_HEALED, replay.C_PERSISTENT):
        if count[name]:
            out.append(f"  {name}: {count[name]:,}")
    never = sorted(stats.subscribed - stats.booked)
    if never:
        out.append(f"  {len(never)} of {len(stats.subscribed)} subscribed tokens never got a full book "
                   f"(market closed or resolved? the list may need re-picking more often)")

    out += ["", "CHECK 2 and 3: sanity and trades"]
    sanity = [n for n in stats.problems if n not in (replay.P_BEST, replay.P_REST)]
    out.append("  all passed" if not sanity else "")
    for name in sanity:
        out.append(f"  {name}: {stats.problems[name]:,}")

    out += ["", "CHECK 4: gaps (times tokens had no live book)"]
    outages = sorted(stats.outages, key=lambda o: o.start_ns)
    if not outages:
        out.append("  none")
    blind_total = 0.0
    for o in outages:
        if o.end_ns is not None:
            blind = (o.end_ns - o.start_ns) / 1e9
            blind_total += blind
            reconnect = (o.end_ns - o.note_ns) / 1e9
            extra = f", reconnect took {reconnect:,.1f}s" if o.note_ns != o.start_ns else ""
            out.append(f"  {when(o.start_ns)}  blind {blind:,.1f}s{extra}  {o.who}: {o.reason}"
                       + (f" ({o.tokens} tokens)" if o.tokens else ""))
        else:
            out.append(f"  {when(o.start_ns)}  open  {o.who}: {o.reason}")
    if blind_total:
        out.append(f"  total blind time: {blind_total:,.1f}s")
    out += ["", "Exchange-to-receive delay (all events):", lag_summary(stats.lag_buckets)]

    damaged = [d for d in result.files.damage if d.kind == "corrupt"]
    truncated = [d for d in result.files.damage if d.kind == "truncated"]
    if result.files.damage or result.files.bad_lines:
        out += ["", "FILES"]
        for d in damaged:
            out.append(f"  DAMAGED: {d.describe()}")
        for d in truncated:
            out.append(f"  truncated: {d.describe()}")
        if result.files.bad_lines:
            out.append(f"  unreadable lines: {result.files.bad_lines:,} (half-written by a crash, or damaged)")

    if stats.warnings:
        out += ["", "WARNINGS"]
        for name, n in stats.warnings.items():
            out.append(f"  {name}: {n:,}")
            out.extend(f"    {e}" for e in stats.examples.get(name, [])[:3])

    if stats.problems:
        out += ["", "EXAMPLES OF PROBLEMS"]
        for name, n in stats.problems.items():
            out.append(f"  {name} ({n:,}):")
            out.extend(f"    {e}" for e in stats.examples.get(name, []))

    failures = sum(stats.problems.values()) + len(damaged)
    out += ["", "RESULT: " + ("PASS, no problems found" if not failures else f"{failures:,} problems found, see above")]
    return "\n".join(out), failures > 0


def save_report(paths: Paths, text: str, now: datetime | None = None) -> Path:
    now = now or datetime.now(UTC)
    paths.reports.mkdir(parents=True, exist_ok=True)
    path = paths.reports / f"{now:%Y-%m-%dT%H%M%SZ}.txt"
    path.write_text(text + "\n")
    return path
