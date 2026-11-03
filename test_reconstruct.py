import json
from decimal import Decimal

import pytest

from normalize import VERSION, build
from reconstruct import Reconstructor, load_records, replay


def row(line=1, **kwargs):
    return dict(normalizer_version=VERSION, venue="polymarket", market_id="m",
                asset_id="yes", source_file="18.jsonl.gz", source_line=line,
                event_index=0, recv_ts_ns=100, exchange_ts_ns=90, **kwargs)


def snapshot(line=1, bids=None, asks=None):
    return row(line, bids=bids if bids is not None else [{"price": "0.4", "size": "10"}],
               asks=asks if asks is not None else [{"price": "0.6", "size": "20"}])


def delta(line=2, price="0.4", size="5", side="bid"):
    return row(line, side=side, price=price, new_size=size, change_index=0)


def test_absolute_updates_and_deletions():
    engine = replay([("book_deltas", delta()), ("books_snapshots", snapshot())])
    book = engine.books[("polymarket", "m", "yes")]
    assert book.bids == {Decimal("0.4"): Decimal("5")}
    assert book.summary()["midpoint"] == "0.5"
    engine.apply("book_deltas", delta(3, size="0"))
    assert book.summary()["best_bid"] is None
    assert book.summary()["spread"] is None
    engine.apply("book_deltas", delta(4, price="0.1", size="0"))
    assert book.bids == {}


def test_empty_snapshot_replaces_both_sides():
    engine = replay([("books_snapshots", snapshot()),
                     ("books_snapshots", snapshot(2, bids=[], asks=[]))])
    result = engine.result()["books"][0]
    assert result["synchronized"] and result["bids"] == result["asks"] == []
    assert result["best_bid"] is result["best_ask"] is result["midpoint"] is None


def test_delta_before_snapshot_flagged_and_ignored():
    engine = replay([("book_deltas", delta())])
    assert engine.issues[0]["reason"] == "delta_without_snapshot"
    assert not engine.result()["books"][0]["synchronized"]
    engine.apply("books_snapshots", snapshot(3))
    assert engine.result()["books"][0]["synchronized"]


@pytest.mark.parametrize("kind", ["disconnected", "connected", "subscribed", "universe_changed"])
def test_lifecycle_invalidates_until_new_snapshot(kind):
    engine = replay([("books_snapshots", snapshot()),
                     ("collector_events", row(2, kind=kind)),
                     ("book_deltas", delta(3))])
    result = engine.result()["books"][0]
    assert not result["synchronized"] and result["best_bid"] is None
    engine.apply("books_snapshots", snapshot(4))
    assert engine.result()["books"][0]["synchronized"]


def test_clean_stop_preserves_historical_book_and_timestamps():
    engine = replay([("books_snapshots", snapshot()), ("book_deltas", delta())])
    before = engine.result()["books"][0]
    stop = row(3, kind="stopped")
    stop["recv_ts_ns"] = 200
    engine.apply("collector_events", stop)
    after = engine.result()["books"][0]
    assert after["collection_status"] == "stopped"
    assert after["collection_status_recv_ts_ns"] == 200
    assert after["synchronized"] and after["status"] == "ok"
    for field in ("bids", "asks", "best_bid", "best_ask", "spread", "midpoint",
                  "last_position", "recv_ts_ns", "exchange_ts_ns"):
        assert after[field] == before[field]


def test_stop_does_not_restore_invalid_book():
    engine = replay([("books_snapshots", snapshot()),
                     ("collector_events", row(2, kind="disconnected")),
                     ("collector_events", row(3, kind="stopped"))])
    result = engine.result()["books"][0]
    assert result["collection_status"] == "stopped"
    assert result["status"] == "disconnected"
    assert not result["synchronized"] and result["best_bid"] is None


def test_restart_requires_snapshot():
    engine = replay([("books_snapshots", snapshot()),
                     ("collector_events", row(2, kind="stopped")),
                     ("book_deltas", delta(3))])
    assert not engine.result()["books"][0]["synchronized"]
    engine.apply("collector_events", row(4, kind="connected"))
    engine.apply("books_snapshots", snapshot(5, asks=[]))
    engine.apply("book_deltas", delta(6, size="7"))
    result = engine.result()["books"][0]
    assert result["synchronized"]
    assert result["bids"][0]["size"] == "7"
    assert result["best_ask"] is result["spread"] is result["midpoint"] is None


def test_separate_assets_and_capture_order_over_timestamps():
    no = snapshot(2)
    no["asset_id"] = "no"
    update = delta(3)
    update["exchange_ts_ns"] = 1
    update["recv_ts_ns"] = 1
    engine = replay([("book_deltas", update), ("books_snapshots", no),
                     ("books_snapshots", snapshot())])
    assert engine.books[("polymarket", "m", "yes")].bids[Decimal(".4")] == 5
    assert engine.books[("polymarket", "m", "no")].bids[Decimal(".4")] == 10


def test_change_index_orders_same_frame_updates():
    a, b = delta(), delta(size="7")
    b["change_index"] = 1
    engine = replay([("book_deltas", b), ("book_deltas", a), ("books_snapshots", snapshot())])
    assert engine.result()["books"][0]["bids"][0]["size"] == "7"


def test_duplicate_positions_rejected():
    with pytest.raises(ValueError, match="duplicate"):
        replay([("books_snapshots", snapshot()), ("books_snapshots", snapshot())])


def test_bad_snapshot_is_atomic():
    engine = replay([("books_snapshots", snapshot())])
    before = engine.result()
    with pytest.raises(ValueError, match="duplicate snapshot"):
        engine.apply("books_snapshots", snapshot(2, bids=[], asks=[
            {"price": "0.6", "size": "1"}, {"price": "0.60", "size": "2"}]))
    assert engine.result() == before


def test_crossed_book_is_flagged():
    engine = replay([("books_snapshots", snapshot()), ("book_deltas", delta(price="0.7"))])
    result = engine.result()["books"][0]
    assert result["status"] == "crossed_or_locked"
    assert result["midpoint"] is result["spread"] is None


def test_unknown_events_invalidate_and_trades_do_not_change_depth():
    engine = replay([("books_snapshots", snapshot()), ("trades", row(2))])
    assert engine.result()["books"][0]["bids"][0]["size"] == "10"
    engine.apply("unsupported_events", row(3, kind="new_format"))
    assert engine.result()["books"][0]["status"] == "unsupported_event"


def test_raw_to_parquet_to_book_and_integrity(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    events = [dict(event_type="book", market="m", asset_id="yes", timestamp="1",
                   bids=[dict(price="0.4", size="10")], asks=[]),
              dict(event_type="price_change", market="m", timestamp="1", price_changes=[
                  dict(asset_id="yes", side="BUY", price="0.4", size="3")])]
    (raw / "18.jsonl").write_text(json.dumps(dict(kind="message", recv_ts_ns=2_000_000,
                                                  raw=json.dumps(events))) + "\n")
    clean = tmp_path / "clean"
    build(raw, clean)
    first = replay(load_records(clean)).result()
    assert first == replay(load_records(clean)).result()
    assert first["books"][0]["bids"] == [dict(price="0.4", size="3")]
    next(clean.rglob("*.parquet")).write_bytes(b"modified")
    with pytest.raises(ValueError, match="modified"):
        load_records(clean)
