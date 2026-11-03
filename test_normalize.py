import gzip
import json

import pyarrow.parquet as pq
import pytest

from normalize import build, normalize


def envelope(events):
    return dict(recv_ts_ns=1793664000123456789, kind="message", raw=json.dumps(events))


def book():
    return dict(event_type="book", market="condition-1", asset_id="token-yes",
                timestamp="1793664000123", bids=[], asks=[])


def test_empty_snapshot_and_exact_timestamps():
    table, row = normalize(envelope([book()]), "day/00.jsonl", 1)[0]
    assert table == "books_snapshots"
    assert row["bids"] == row["asks"] == []
    assert row["exchange_ts_ns"] == 1793664000123000000
    assert row["recv_ts_ns"] == 1793664000123456789


def test_absolute_update_and_token_identity():
    event = dict(event_type="price_change", market="condition-1", price_changes=[
        dict(asset_id="yes", side="BUY", price="0.1234567890123456789", size="0"),
        dict(asset_id="no", side="SELL", price="0.8", size="99.123456789")])
    rows = normalize(envelope(event), "x", 2)
    assert rows[0][1]["new_size"] == "0"
    assert rows[0][1]["price"] == "0.1234567890123456789"
    assert rows[1][1]["asset_id"] == "no"
    assert rows[1][1]["side"] == "ask"
    assert rows[1][1]["change_index"] == 1
    assert rows[0][1]["exchange_ts_ns"] is None


@pytest.mark.parametrize("price", ["NaN", "Infinity", "-0.1", "1.1", 0.1])
def test_invalid_decimal(price):
    event = book()
    event["bids"] = [dict(price=price, size="1")]
    with pytest.raises(ValueError):
        normalize(envelope(event), "x", 1)


def test_trade_does_not_invent_id_or_taker_direction():
    event = dict(event_type="last_trade_price", market="c", asset_id="t", price="0.5", side="BUY")
    row = normalize(envelope(event), "x", 1)[0][1]
    assert row["size"] is None
    assert row["reported_side"] == "BUY"
    assert "taker_side" not in row and "trade_id" not in row


def test_unknown_and_health_are_visible():
    assert normalize(envelope({"event_type": "new_type"}), "x", 1)[0][0] == "unsupported_events"
    assert normalize(dict(recv_ts_ns=1, kind="disconnected", raw={"connection": 1}), "x", 1)[0][0] == "collector_events"


def test_parquet_gzip_idempotency_and_raw_unchanged(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    path = raw / "00.jsonl.gz"
    with gzip.open(path, "wt") as stream:
        stream.write(json.dumps(envelope([book(), book()])) + "\n")
    before = path.read_bytes()
    output = tmp_path / "clean"
    assert build(raw, output) == {"books_snapshots": 2}
    assert build(raw, output) == {"books_snapshots": 2}
    assert path.read_bytes() == before
    file = next(output.rglob("*.parquet"))
    rows = pq.ParquetFile(file).read().to_pylist()
    assert [r["event_index"] for r in rows] == [0, 1]
    assert rows[0]["bids"] == []


def test_malformed_input_never_publishes_partial_dataset(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "00.jsonl").write_text(json.dumps(envelope(book())) + "\n{broken")
    with pytest.raises(ValueError, match="00.jsonl:2"):
        build(raw, tmp_path / "clean")
    assert not (tmp_path / "clean").exists()


def test_changed_input_requires_new_destination(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    path = raw / "00.jsonl"
    path.write_text(json.dumps(envelope(book())) + "\n")
    output = tmp_path / "clean"
    build(raw, output)
    before = (output / "manifest.json").read_bytes()
    path.write_text(json.dumps(envelope([book(), book()])) + "\n")
    with pytest.raises(ValueError, match="output differs"):
        build(raw, output)
    assert (output / "manifest.json").read_bytes() == before


def test_all_tables_roundtrip(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    events = [book(), dict(event_type="price_change", market="c", price_changes=[
        dict(asset_id="t", side="BUY", price="0.5", size="0")]),
        dict(event_type="last_trade_price", market="c", asset_id="t", price="0.5"),
        dict(event_type="tick_size_change", new_tick_size="0.01")]
    records = [envelope(events), dict(recv_ts_ns=1793664000123456789,
                                      kind="disconnected", raw={"connection": 1})]
    (raw / "00.jsonl").write_text("".join(json.dumps(x) + "\n" for x in records))
    output = tmp_path / "clean"
    counts = build(raw, output)
    assert len(counts) == 5
    for path in output.rglob("*.parquet"):
        assert pq.ParquetFile(path).read().num_rows == 1
    assert build(raw, output) == counts
