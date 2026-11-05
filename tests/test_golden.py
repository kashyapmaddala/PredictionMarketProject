"""Golden files: a small cut of real raw data (tests/fixtures/golden) must always give exactly the
same tables and the same validation result.

The cut (283 KB) is one market (two tokens, the one whose tick size changed from 0.01 to 0.001 on
2026-11-03 23:22:17 UTC) in two windows of hour 23: 23:20 to 23:23 and 23:44:30 to 23:49 (the drop
at 23:47:49 and the reconnect), plus the final "stopped" note from 2026-11-05 15:25. It covers full
books, price-change batches, trades, tick size changes, REST copies, a drop, a reconnect with an
initial dump, and a stop. In the second window each token's changes start at its next full book,
so the two windows join without a gap in any book.

If a change to the code changes the tables on purpose, look at the difference, then refresh with:
    UPDATE_GOLDEN=1 python3 -m pytest tests/test_golden.py
"""
import hashlib
import json
import os
import shutil
from datetime import UTC, datetime
from decimal import Decimal as D
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from pipeline.config import Paths
from pipeline.polymarket import tables
from pipeline.polymarket.schemas import HOURLY_TABLES
from pipeline.polymarket.validate import validate

FIXTURE = Path(__file__).parent / "fixtures" / "golden"
EXPECTED = Path(__file__).parent / "fixtures" / "golden_expected.json"


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    root = tmp_path_factory.mktemp("golden")
    shutil.copytree(FIXTURE, root, dirs_exist_ok=True)
    paths = Paths(root)
    tables.build(paths, now=datetime(2026, 11, 6, tzinfo=UTC))
    return paths


def table_rows(paths, name):
    folder = paths.clean / name
    if not folder.exists():
        return []
    return pq.read_table(folder).to_pylist()


def summary(paths):
    out = {}
    for name in HOURLY_TABLES:
        rows = table_rows(paths, name)
        text = json.dumps(rows, default=str, sort_keys=True)
        out[name] = {"rows": len(rows), "sha256": hashlib.sha256(text.encode()).hexdigest()}
    return out


def test_tables_match_the_golden_summary(built):
    got = summary(built)
    if os.environ.get("UPDATE_GOLDEN"):
        EXPECTED.write_text(json.dumps(got, indent=2, sort_keys=True) + "\n")
    assert got == json.loads(EXPECTED.read_text())


def test_what_the_golden_tables_say(built):
    deltas = table_rows(built, "book_deltas")
    assert {r["side"] for r in deltas} == {"bid", "ask"} and all(isinstance(r["price"], D) for r in deltas)
    snapshots = table_rows(built, "book_snapshots")
    assert {r["source"] for r in snapshots} == {"live", "rest"}
    assert D("0.001") in {r["tick_size"] for r in snapshots}  # books after the tick size change
    events = [r["kind"] for r in table_rows(built, "collector_events")]
    assert events == ["disconnected", "connected", "subscribed", "stopped"]
    top = table_rows(built, "top_of_book")
    assert {r["cause"] for r in top} == {"change", "full_book"}
    assert all(r["best_bid"] < r["best_ask"] for r in top if r["best_bid"] is not None and r["best_ask"] is not None)
    assert len(table_rows(built, "trades")) == 46
    assert table_rows(built, "resyncs") == [] and table_rows(built, "unsupported_events") == []
    # every row says where it came from
    assert all(r["source_file"] == "2026-11-03/23.jsonl.gz" for r in deltas)
    assert {r["normalizer_version"] for r in top} == {2}


def test_golden_validation(built):
    report = validate(built)
    assert not report.failed, report.text
    text = report.text
    assert "6,400 of 6,400 change batches matched Polymarket's best prices (100.0000%)" in text
    assert "4 of 4 REST book copies matched our rebuilt book exactly" in text
    assert "connection: no close frame received or sent (2 tokens)" in text
    assert "stopped (not restarted yet)" in text
    assert "216" not in text.split("Messages:")[1].split("\n")[0]  # only this market's 4 tick changes
