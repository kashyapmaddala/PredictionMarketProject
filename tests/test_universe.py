"""Picking the markets: filtering, ranking, capping per event, saving, and the download."""
import json
from datetime import UTC, datetime

import httpx
import pytest

import pick_markets
from pipeline.config import Paths
from pipeline.polymarket import universe as u
from pipeline.raw_reader import RawFile, raw_files
from pipeline.raw_store import RawStore


def market(i, volume, event="e1", **extra):
    base = {"id": str(i), "conditionId": f"0x{i}", "question": f"Q{i}?", "slug": f"q{i}", "version": "v1",
            "events": [{"id": event, "slug": event, "title": event.upper()}], "clobTokenIds": json.dumps([f"{i}1", f"{i}2"]),
            "outcomes": '["Yes", "No"]', "enableOrderBook": True, "acceptingOrders": True, "closed": False,
            "volume24hr": volume, "liquidityNum": 5, "orderPriceMinTickSize": 0.01, "orderMinSize": 5,
            "negRisk": False, "endDate": "2026-12-01T00:00:00Z"}
    base.update(extra)
    return base


def test_tokens_and_names():
    assert u.token_ids({"clobTokenIds": '["1", "2"]'}) == ["1", "2"]
    assert u.token_ids({"positionIds": [3, 4]}) == ["3", "4"]
    assert u.token_ids({"clobTokenIds": "not json"}) == []
    assert u.token_ids({}) == []
    assert u.outcome_names({"outcomes": '["A", "B"]'}) == ["A", "B"]
    assert u.outcome_names({"outcomes": "x"}) == []
    assert u.number("1.5") == 1.5 and u.number(None) == 0.0 and u.number("nan") == 0.0 and u.number("inf") == 0.0


def test_tradeable():
    assert u.tradeable(market(1, 5))
    assert not u.tradeable(market(1, 5, closed=True))
    assert not u.tradeable(market(1, 5, acceptingOrders=False))
    assert not u.tradeable(market(1, 5, enableOrderBook=False))
    assert not u.tradeable(market(1, 5, clobTokenIds='["1"]'))


def test_rank_cap_and_pick():
    markets = [market(i, 100 - i, event="big" if i < 8 else f"e{i}") for i in range(12)]
    markets.append(market(3, 97, event="big"))  # the same market twice (two pages)
    markets.append(market(50, 1000, closed=True))
    ranked = u.rank(markets)
    assert [m["id"] for m in ranked][:3] == ["0", "1", "2"]
    picked = u.pick_top(ranked, 7, max_per_event=5)
    assert [m["id"] for m in picked] == ["0", "1", "2", "3", "4", "8", "9"]
    assert u.event_id({"id": 7}) == "no-event-7"


def test_ties_are_broken_by_id():
    assert [m["id"] for m in u.rank([market(2, 5), market(1, 5)])] == ["1", "2"]


def test_build_save_and_load(tmp_path):
    universe = u.build_universe([market(1, 10), market(2, 20, event="e2")], 5, now=datetime(2026, 11, 5, 6, 7, 8, tzinfo=UTC))
    assert [m["rank"] for m in universe["markets"]] == [1, 2] and universe["markets"][0]["market_id"] == "2"
    assert universe["markets"][0]["outcomes"] == [{"name": "Yes", "token_id": "21"}, {"name": "No", "token_id": "22"}]
    path = u.save_universe(universe, tmp_path)
    assert path.name == "2026-11-05T060708Z.json" and (tmp_path / "current.json").read_text() == path.read_text()
    names, picked_at = u.load_universe(tmp_path / "current.json")
    assert list(names) == ["21", "22", "11", "12"] and picked_at.tzinfo is not None


@pytest.mark.parametrize("text", ["{", '{"markets": [], "picked_at": "2026-11-05T00:00:00"}', '{"markets": [{}]}'])
def test_bad_lists_are_refused(tmp_path, text):
    (tmp_path / "current.json").write_text(text)
    with pytest.raises(u.UniverseError):
        u.load_universe(tmp_path / "current.json")
    with pytest.raises(u.UniverseError):
        u.load_universe(tmp_path / "missing.json")


def test_download_saves_every_page_and_retries(tmp_path):
    pages = [{"markets": [market(1, 10)], "next_cursor": "abc"}, {"markets": [market(2, 20)], "next_cursor": None}]
    calls = []

    def handler(request):
        calls.append(request)
        if len(calls) == 2:
            return httpx.Response(429, headers={"Retry-After": "0"})
        return httpx.Response(200, json=pages[0] if "after_cursor" not in str(request.url) else pages[1])

    store = RawStore(tmp_path, compress=True)
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        got = u.pull_all_open_markets(client, store, sleep=lambda _s: None)
    store.close()
    assert [m["id"] for m in got] == ["1", "2"]
    params = str(calls[0].url)
    assert "version=v1" in params and "version=v2" in params and "closed=false" in params
    saved = [e for path in raw_files(tmp_path) for _n, e in RawFile(path)]
    assert [e["kind"] for e in saved] == ["gamma_markets_page", "gamma_markets_page"]


def test_pick_markets_cli(tmp_path, monkeypatch):
    monkeypatch.setenv("PM_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setattr(pick_markets, "pull_all_open_markets", lambda client, store: [market(1, 10), market(2, 30, event="e2")])
    assert pick_markets.main(["1"]) == 0
    paths = Paths(tmp_path / "data")
    assert json.loads(paths.universe_file.read_text())["markets"][0]["market_id"] == "2"
    monkeypatch.setattr(pick_markets, "pull_all_open_markets", lambda client, store: [])
    assert pick_markets.main([]) == 3
    with pytest.raises(SystemExit):
        pick_markets.main(["zero"])

    def fail(client, store):
        raise pick_markets.GiveUp("down")

    monkeypatch.setattr(pick_markets, "pull_all_open_markets", fail)
    assert pick_markets.main([]) == 2


def test_download_stops_if_the_cursor_repeats(tmp_path):
    def handler(request):
        return httpx.Response(200, json={"markets": [market(1, 10)], "next_cursor": "same"})

    store = RawStore(tmp_path, compress=True)
    with httpx.Client(transport=httpx.MockTransport(handler)) as client, pytest.raises(ValueError, match="kept paging"):
        u.pull_all_open_markets(client, store)
    store.close()
