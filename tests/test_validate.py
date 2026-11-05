"""The data check report."""
import json
from datetime import UTC, datetime

import pytest

import check_data
from pipeline.config import Paths
from pipeline.polymarket import tables
from pipeline.polymarket.validate import save_report, validate
from pipeline.raw_store import RawStore
from tests.helpers import TOKEN_A, book, change, price_change, scripted_session

T = 1_793_750_400_000_000_000  # 2026-11-04 00:00 UTC


@pytest.fixture
def paths(tmp_path):
    return Paths(tmp_path / "data")


def test_clean_session_passes_and_lists_the_drop(paths):
    scripted_session(paths.raw, T + 60 * 1_000_000_000)
    report = validate(paths)
    assert not report.failed, report.text
    text = report.text
    assert "RESULT: PASS" in text
    assert "change batches matched Polymarket's best prices (100.0000%)" in text
    assert "1 of 1 REST book copies matched" in text
    assert "connection: no close frame received or sent (2 tokens)" in text
    assert "total blind time" in text and "Exchange-to-receive delay" in text


def test_no_data(paths):
    report = validate(paths)
    assert report.text == "No saved data found." and not report.failed


def test_problems_fail_the_check(paths):
    store = RawStore(paths.raw, compress=False, session="s")
    store.write("connected", {}, T, conn=1)
    store.write("message", json.dumps([book()]), T + 1, conn=1)
    store.write("message", json.dumps(price_change(change(price="0.45", best_bid="0.46"))), T + 2, conn=1)
    store.write("message", "not json", T + 3, conn=1)
    store.close()
    report = validate(paths)
    assert report.failed
    assert "rebuilt book differs from Polymarket's best prices (1)" in report.text
    assert "message with a bad field" in report.text
    assert "RESULT: 2 problems found" in report.text


def test_damaged_files_are_reported(paths):
    scripted_session(paths.raw, T + 60 * 1_000_000_000)
    day = paths.raw / "2026-11-04"
    (day / "00.part2.jsonl.gz").write_bytes(b"\x1f\x8b\x08 this is not gzip")
    report = validate(paths)
    assert report.failed and "DAMAGED: 2026-11-04/00.part2.jsonl.gz" in report.text


def test_one_day_starts_from_the_saved_state_of_the_hour_before(paths):
    scripted_session(paths.raw, T - 30 * 60 * 1_000_000_000, minutes=2, stop=False)  # 2026-11-03 23:30
    store = RawStore(paths.raw, compress=True, session="s1")  # carries on after midnight, same connection
    store.write("message", json.dumps(price_change(change(TOKEN_A, "BUY", "0.30", "3", "0.43", "0.60"), stamp=T // 1_000_000 + 5)),
                T + 10_000_000, conn=2)  # the session reconnected once (connection 2)
    store.close()
    alone = validate(paths, "2026-11-04")
    assert alone.started_from is None
    assert alone.engine.stats.count["changes before first full book (skipped)"] == 1
    tables.build(paths, now=datetime(2026, 11, 5, tzinfo=UTC))
    carried = validate(paths, "2026-11-04")
    assert carried.started_from == "2026-11-03T23"
    assert carried.engine.stats.count["price changes checked"] == 1
    assert "Books carried over from the saved state after 2026-11-03T23" in carried.text


def test_report_is_saved(paths):
    path = save_report(paths, "hello", now=datetime(2026, 11, 5, 1, 2, 3, tzinfo=UTC))
    assert path.name == "2026-11-05T010203Z.txt" and path.read_text() == "hello\n"


def test_cli_exit_codes(paths, monkeypatch, capsys):
    monkeypatch.setenv("PM_DATA_DIR", str(paths.data))
    scripted_session(paths.raw, T + 60 * 1_000_000_000)
    assert check_data.main([]) == 0
    assert "RESULT: PASS" in capsys.readouterr().out
    assert check_data.main(["2026-11-04"]) == 0
    with pytest.raises(SystemExit):
        check_data.main(["yesterday"])
    store = RawStore(paths.raw, compress=False, session="x")
    store.write("message", "garbage", T + 3600 * 10**9 * 5)
    store.close()
    assert check_data.main([]) == 1
