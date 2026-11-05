"""Parsing Polymarket messages into typed events, with every field checked."""
import json
from decimal import Decimal as D

import pytest

from pipeline.polymarket import events as ev
from tests.helpers import TOKEN_A, TOKEN_B, book, change, price_change, trade


def one(message):
    parsed = ev.parse_message(json.dumps(message))
    assert len(parsed) == 1
    return parsed[0]


def test_book_parses_exact_decimals_and_drops_zero_sizes_from_the_book():
    e = one(book(bids=[("0.4", "10.5"), ("0.41", "0")], asks=[("0.6", "1743.67")], tick="0.001"))
    assert isinstance(e, ev.BookEvent)
    assert e.bids == {D("0.4"): D("10.5")} and e.asks == {D("0.6"): D("1743.67")}
    assert e.bid_levels == [(D("0.4"), D("10.5")), (D("0.41"), D("0"))]  # every level kept for the snapshot table
    assert e.tick == D("0.001") and e.stamp == int(e.stamp)


def test_initial_dump_is_a_list_of_books():
    parsed = ev.parse_message(json.dumps([book(TOKEN_A), book(TOKEN_B)]))
    assert [p.token for p in parsed] == [TOKEN_A, TOKEN_B]


def test_price_change_with_several_changes():
    e = one(price_change(change(TOKEN_A), change(TOKEN_B, side="SELL", price="0.6", size="0")))
    assert isinstance(e, ev.PriceChange)
    assert [(c.token, c.side, c.size, c.index) for c in e.changes] == [(TOKEN_A, "bid", D("5"), 0), (TOKEN_B, "ask", D("0"), 1)]


def test_price_change_without_event_type_is_still_a_price_change():
    message = price_change(change())
    del message["event_type"]
    assert isinstance(one(message), ev.PriceChange)


def test_trade_and_tick_size_change():
    t = one(trade(size="12.345678", side="SELL"))
    assert t.price == D("0.5") and t.size == D("12.345678") and t.side == "sell"
    tick = one({"event_type": "tick_size_change", "asset_id": TOKEN_A, "market": "m", "old_tick_size": "0.01",
                "new_tick_size": "0.001", "timestamp": "1793748137004"})
    assert tick.new == D("0.001") and tick.old == D("0.01")


@pytest.mark.parametrize("price", ["NaN", "Infinity", "-0.1", "1.1", "abc", "", None, 0.5, True, "0.1234567"])
def test_bad_prices_make_the_event_unsupported(price):
    message = price_change(change(price=price))
    e = one(message)
    assert isinstance(e, ev.Unsupported) and e.kind == "price_change"
    assert json.loads(e.payload) == message  # nothing lost


def test_values_that_do_not_fit_the_table_columns_are_rejected_not_rounded():
    assert ev.number("0.123456", "x") == D("0.123456")
    assert ev.number("0.1234560", "x") == D("0.1234560")  # trailing zero: fits exactly
    with pytest.raises(ev.BadField):
        ev.number("0.1234567", "x")
    assert ev.number("999999999999.999999", "x")
    with pytest.raises(ev.BadField):
        ev.number("1000000000000", "x")


@pytest.mark.parametrize("message,kind,reason", [
    ({"event_type": "best_bid_ask", "asset_id": TOKEN_A}, "best_bid_ask", "unknown event type"),
    ({"no": "type"}, "None", "unknown event type"),
    ({"event_type": "book", "asset_id": TOKEN_A, "timestamp": "1", "bids": "x", "asks": []}, "book", "bids is not a list"),
    ({"event_type": "book", "asset_id": TOKEN_A, "timestamp": "1", "bids": [{"price": "0.4", "size": "1"}] * 2, "asks": []},
     "book", "twice"),
    ({"event_type": "price_change", "timestamp": "1", "price_changes": []}, "price_change", "non-empty"),
    ({"event_type": "price_change", "timestamp": "1", "price_changes": [{**change(), "side": "HOLD"}]}, "price_change", "BUY or SELL"),
    ({"event_type": "price_change", "timestamp": "x1", "price_changes": [change()]}, "price_change", "timestamp"),
    ({"event_type": "last_trade_price", "asset_id": TOKEN_A, "timestamp": "1", "price": "0.5", "side": "UP"}, "last_trade_price", "BUY or SELL"),
    ({"event_type": "last_trade_price", "timestamp": "1", "price": "0.5"}, "last_trade_price", "asset_id missing"),
])
def test_unsupported_events_say_why(message, kind, reason):
    e = one(message)
    assert isinstance(e, ev.Unsupported)
    assert e.kind == kind and reason in e.reason


def test_non_json_and_non_objects():
    assert ev.parse_message("INVALID OPERATION")[0].kind == "not_json"
    assert ev.parse_message({"a": 1})[0].kind == "not_json"
    assert ev.parse_message("[1, 2]")[1].kind == "not_an_object"


def test_one_bad_event_does_not_spoil_its_neighbours():
    parsed = ev.parse_message(json.dumps([book(TOKEN_A), {"event_type": "book"}, book(TOKEN_B)]))
    assert [type(p).__name__ for p in parsed] == ["BookEvent", "Unsupported", "BookEvent"]


def test_rest_books():
    good = {**book(TOKEN_A), "min_order_size": "5", "neg_risk": True}
    parsed = ev.parse_rest_books(json.dumps([good, {"asset_id": TOKEN_B}, 7]))
    assert isinstance(parsed[0], ev.BookEvent)
    assert [p.kind for p in parsed[1:]] == ["rest_books", "rest_books"]
    assert ev.parse_rest_books("oops")[0].reason == "reply is not JSON"
    assert ev.parse_rest_books('{"a": 1}')[0].reason == "reply is not a list"
    assert ev.parse_rest_books({"a": 1})[0].reason == "reply is not text"


def test_timestamps():
    assert ev.stamp("1793748137004") == 1793748137004
    assert ev.stamp(1793748137004) == 1793748137004
    assert ev.stamp(None, required=False) is None
    for bad in ("-1", "1.5", "1e12", "9" * 16, True):
        with pytest.raises(ev.BadField):
            ev.stamp(bad)
