"""Building the clean tables: exact decimals, idempotence, incremental builds, output checks."""
import hashlib
from datetime import UTC, datetime
from decimal import Decimal as D

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipeline.config import Paths
from pipeline.polymarket import schemas, tables
from pipeline.polymarket.schemas import OutputError, check_table
from pipeline.raw_store import RawStore
from tests.helpers import TOKEN_A, scripted_session

H = 3_600_000_000_000
T = 1_793_750_400_000_000_000  # 2026-11-04 00:00 UTC
LATER = datetime(2026, 11, 6, tzinfo=UTC)


@pytest.fixture
def paths(tmp_path):
    p = Paths(tmp_path / "data")
    scripted_session(p.raw, T + 59 * 60 * 1_000_000_000, minutes=3)  # crosses 00:00 -> 01:00
    return p


def all_files(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob("*")) if p.is_file()}


def read(paths, table):
    return pq.read_table(paths.clean / table).to_pylist() if (paths.clean / table).exists() else []


def test_builds_every_table_with_exact_decimals(paths):
    summary = tables.build(paths, now=LATER)
    assert [(d, h) for d, h, _c in summary["built"]] == [("2026-11-04", "00"), ("2026-11-04", "01")]
    deltas = read(paths, "book_deltas")
    assert deltas and all(isinstance(r["price"], D) for r in deltas)
    assert pq.read_schema(next((paths.clean / "book_deltas").rglob("*.parquet"))).field("price").type == pa.decimal128(18, 6)
    top = read(paths, "top_of_book")
    assert {r["cause"] for r in top} == {"full_book", "change"}
    row = next(r for r in top if r["best_bid"] is not None and r["best_ask"] is not None)
    assert row["mid"] == (row["best_bid"] + row["best_ask"]) / 2 and row["spread"] == row["best_ask"] - row["best_bid"]
    assert all(r["normalizer_version"] == 2 and r["venue"] == "polymarket" for r in top)
    kinds = [r["kind"] for r in read(paths, "collector_events")]
    assert kinds[0] == "started" and kinds[-1] == "stopped" and "disconnected" in kinds
    assert read(paths, "trades")[0]["taker_side"] == "buy"
    assert {r["source"] for r in read(paths, "book_snapshots")} == {"live", "rest"}
    assert summary["markets"] == 0


def test_rebuild_is_byte_identical(paths):
    tables.build(paths, now=LATER)
    first = all_files(paths.clean)
    tables.build(paths, rebuild=True, now=LATER)
    assert all_files(paths.clean) == first


def test_second_run_builds_nothing_and_changes_nothing(paths):
    tables.build(paths, now=LATER)
    first = all_files(paths.clean)
    summary = tables.build(paths, now=LATER)
    assert summary["built"] == [] and summary["skipped"] == 2
    assert all_files(paths.clean) == first


def test_incremental_build_equals_full_build(tmp_path, paths):
    tables.build(paths, now=datetime(2026, 11, 4, 1, 3, tzinfo=UTC))  # only 00:00 is finished
    assert [h for _d, h, _c in tables.build(paths, now=datetime(2026, 11, 4, 1, 3, tzinfo=UTC))["built"]] == []
    summary = tables.build(paths, now=LATER)
    assert [h for _d, h, _c in summary["built"]] == ["01"]
    incremental = all_files(paths.clean)
    tables.build(paths, rebuild=True, now=LATER)
    assert all_files(paths.clean) == incremental


def test_changed_raw_hour_is_rebuilt_with_every_hour_after_it(paths):
    tables.build(paths, now=LATER)
    store = RawStore(paths.raw, compress=True, session="s9")  # a late part for hour 00
    store.write("note", {"x": 1}, T + 10)
    store.close()
    summary = tables.build(paths, now=LATER)
    assert [h for _d, h, _c in summary["built"]] == ["00", "01"]


def test_unfinished_hour_is_skipped_unless_asked(paths):
    now = datetime(2026, 11, 4, 1, 30, tzinfo=UTC)
    summary = tables.build(paths, now=now)
    assert [h for _d, h, _c in summary["built"]] == ["00"] and summary["partial"] == []
    summary = tables.build(paths, include_current=True, now=now)
    assert [h for _d, h, _c in summary["partial"]] == ["01"]
    assert not tables.state_path(paths, "2026-11-04", "01").exists()  # rebuilt once finished
    assert len(read(paths, "collector_events")) > 0


def test_state_file_records_inputs_and_outputs(paths):
    tables.build(paths, now=LATER)
    record = tables.load_state(tables.state_path(paths, "2026-11-04", "00"))
    assert record["normalizer_version"] == 2 and record["inputs"][0][0] == "2026-11-04/00.jsonl.gz"
    path = paths.clean / "book_deltas" / "date=2026-11-04" / "hour=00" / "part.parquet"
    assert record["outputs"]["book_deltas"]["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert tables.load_state(paths.state / "missing.json.gz") is None


def test_damaged_state_file_means_rebuild(paths):
    tables.build(paths, now=LATER)
    tables.state_path(paths, "2026-11-04", "00").write_bytes(b"garbage")
    summary = tables.build(paths, now=LATER)
    assert [h for _d, h, _c in summary["built"]] == ["00", "01"]


def test_markets_table_uses_decimals(paths, tmp_path):
    paths.universe.mkdir(parents=True)
    (paths.universe / "2026-11-04T000000Z.json").write_text("""{"picked_at": "2026-11-04T00:00:00+00:00", "markets": [
      {"rank": 1, "market_id": "7", "condition_id": "0xc", "question": "Q?", "slug": "q", "event_id": 9,
       "event_slug": "e", "event_title": "E", "outcomes": [{"name": "Yes", "token_id": "1"}, {"name": "No", "token_id": "2"}],
       "tick_size": 0.001, "min_order_size": 5, "neg_risk": false, "end_date": null, "volume_24h": 1.5, "liquidity": 2}]}""")
    assert tables.build_markets(paths) == 2
    rows = pq.read_table(paths.clean / "markets" / "markets.parquet").to_pylist()
    assert rows[0]["tick_size"] == D("0.001") and rows[0]["event_id"] == "9" and rows[1]["outcome"] == "No"


def test_output_checks_fail_loud():
    good = tables.to_arrow("trades", [(1, 2, TOKEN_A, "m", D("0.5"), D("1"), "buy", D("0"), "0x", "c", "f", 1)])
    check_table("trades", good)
    with pytest.raises(OutputError, match="empty values"):
        check_table("trades", tables.to_arrow("trades", [(1, 2, None, "m", D("0.5"), D("1"), "buy", None, None, None, "f", 1)]))
    with pytest.raises(OutputError, match="unexpected values"):
        check_table("trades", tables.to_arrow("trades", [(1, 2, "t", "m", D("0.5"), D("1"), "hold", None, None, None, "f", 1)]))
    with pytest.raises(OutputError, match="outside 0 to 1"):
        check_table("trades", tables.to_arrow("trades", [(1, 2, "t", "m", D("1.5"), D("1"), "buy", None, None, None, "f", 1)]))
    with pytest.raises(OutputError, match="schema"):
        check_table("trades", good.drop_columns(["venue"]))
    with pytest.raises(OutputError, match="spread"):
        check_table("top_of_book", tables.to_arrow("top_of_book", [
            (1, 2, "t", D("0.4"), D("1"), D("0.6"), D("1"), D("0.5"), None, 1, 1, "change", "c", "f", 1)]))
    with pytest.raises((pa.ArrowInvalid, ValueError)):  # too many decimals is never rounded
        tables.to_arrow("trades", [(1, 2, "t", "m", D("0.1234567"), D("1"), "buy", None, None, None, "f", 1)])


def test_failed_build_leaves_no_half_written_hour(paths, monkeypatch):
    def broken(*_args):
        raise OutputError("boom")

    monkeypatch.setattr(tables, "check_table", broken)
    with pytest.raises(OutputError):
        tables.build(paths, now=LATER)
    assert not (paths.clean / "book_deltas").exists()
    assert not list(paths.state.glob("*")) if paths.state.exists() else True
    assert not list((paths.clean / ".staging").glob("*")) if (paths.clean / ".staging").exists() else True


def test_tables_are_queryable_with_exact_decimal_arithmetic(paths):
    tables.build(paths, now=LATER)
    db = duckdb.connect()
    files = str(paths.clean / "top_of_book" / "*" / "*" / "*.parquet")
    ((kind,),) = db.sql(f"SELECT typeof(mid) FROM read_parquet('{files}') LIMIT 1").fetchall()
    assert kind == "DECIMAL(18,7)"
    ((total,),) = db.sql(f"SELECT sum(best_bid) FROM read_parquet('{files}') WHERE best_bid IS NOT NULL").fetchall()
    assert isinstance(total, D)


def test_schemas_have_the_version_one_columns_first():
    v1 = {"book_deltas": ["exchange_ts", "recv_ts_ns", "token_id", "market_id", "side", "price", "new_size",
                          "reported_best_bid", "reported_best_ask", "hash"],
          "top_of_book": ["exchange_ts", "recv_ts_ns", "token_id", "best_bid", "best_bid_size", "best_ask",
                          "best_ask_size", "mid", "spread", "bid_levels", "ask_levels", "cause"]}
    for name, columns in v1.items():
        assert [c for c, _t in schemas.COLUMNS[name]][:len(columns)] == columns
