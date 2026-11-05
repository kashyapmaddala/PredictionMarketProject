"""Which Polymarket markets to watch (the "universe"): download, filter, rank, cap, save.

1. pull_all_open_markets: every open market from Gamma, page by page; each page is saved exactly
   as received (raw store, kind "gamma_markets_page") before it is used.
2. tradeable: can be traded on the order book right now, with exactly two outcome tokens.
3. pick_top: ranked by 24h volume, at most MAX_PER_EVENT per event, top N.
4. save_universe: data/universe/polymarket/<UTC time>.json, copied to current.json (atomically,
   so the collector never reads a half-written file). Old picks are never deleted.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path

import httpx

from ..config import GAMMA_MARKETS_URL, GAMMA_PAGE_SIZE, MAX_PER_EVENT
from ..http import request

log = logging.getLogger(__name__)

MAX_PAGES = 1000  # 1,000 markets a page: far more than exist; a guard against an endless loop


def token_ids(market: dict) -> list[str]:
    """The outcome tokens. v1 markets list them in clobTokenIds (a JSON string), v2 in positionIds."""
    ids = market.get("clobTokenIds") or market.get("positionIds")
    if isinstance(ids, str):
        try:
            ids = json.loads(ids)
        except ValueError:
            return []
    return [str(i) for i in ids] if isinstance(ids, list) else []


def outcome_names(market: dict) -> list[str]:
    names = market.get("outcomes")
    if isinstance(names, str):
        try:
            names = json.loads(names)
        except ValueError:
            return []
    return [str(n) for n in names] if isinstance(names, list) else []


def number(value) -> float:
    """Gamma's statistics (volume, liquidity) as a float; anything unreadable counts as 0."""
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    return result if result == result and result not in (float("inf"), float("-inf")) else 0.0


def tradeable(market: dict) -> bool:
    return bool(
        market.get("enableOrderBook")
        and market.get("acceptingOrders")
        and not market.get("closed")
        and len(token_ids(market)) == 2
        and len(outcome_names(market)) == 2
    )


def event_id(market: dict) -> str:
    events = market.get("events") or []
    if events and isinstance(events[0], dict) and events[0].get("id") is not None:
        return str(events[0]["id"])
    return f"no-event-{market.get('id')}"


def rank(markets: list[dict]) -> list[dict]:
    """Tradeable markets, busiest first. Ties are broken by market id so the order is stable."""
    unique = {str(m.get("id")): m for m in markets}  # a market can show up on two pages
    can_trade = [m for m in unique.values() if tradeable(m)]
    return sorted(can_trade, key=lambda m: (-number(m.get("volume24hr")), str(m.get("id"))))


def pick_top(ranked: list[dict], how_many: int, max_per_event: int = MAX_PER_EVENT) -> list[dict]:
    """Walk down the ranking, skipping markets whose event already has max_per_event picks."""
    picked, per_event = [], {}
    for market in ranked:
        if len(picked) >= how_many:
            break
        key = event_id(market)
        if per_event.get(key, 0) >= max_per_event:
            continue
        per_event[key] = per_event.get(key, 0) + 1
        picked.append(market)
    return picked


def summarize(market: dict, rank_number: int) -> dict:
    event = (market.get("events") or [{}])[0]
    tokens, names = token_ids(market), outcome_names(market)
    return {
        "rank": rank_number,
        "market_id": market.get("id"),
        "condition_id": market.get("conditionId"),
        "question": market.get("question"),
        "slug": market.get("slug"),
        "version": market.get("version"),
        "event_id": event.get("id"),
        "event_slug": event.get("slug"),
        "event_title": event.get("title"),
        # outcome order matters: index 0 is usually "Yes", but sports markets use team names
        "outcomes": [{"name": n, "token_id": t} for n, t in zip(names, tokens, strict=False)],
        "tick_size": market.get("orderPriceMinTickSize"),
        "min_order_size": market.get("orderMinSize"),
        "neg_risk": market.get("negRisk"),
        "end_date": market.get("endDate"),
        "volume_24h": number(market.get("volume24hr")),
        "liquidity": number(market.get("liquidityNum")),
    }


def pull_all_open_markets(client: httpx.Client, store, sleep=None) -> list[dict]:
    """Download every open market. Saves each raw page before using it."""
    markets, cursor, page, seen = [], None, 0, set()
    while True:
        # version=v1 AND version=v2: the default is v1 only, which would miss newer markets
        params = [("closed", "false"), ("version", "v1"), ("version", "v2"), ("limit", GAMMA_PAGE_SIZE)]
        if cursor:
            params.append(("after_cursor", cursor))
        kwargs = {"params": params}
        if sleep is not None:
            kwargs["sleep"] = sleep
        reply = request(client, "GET", GAMMA_MARKETS_URL, **kwargs)
        store.write("gamma_markets_page", reply.text)
        body = reply.json()
        page_markets = body.get("markets") or []
        markets += [m for m in page_markets if isinstance(m, dict)]
        page += 1
        log.info("page %d: %d markets so far", page, len(markets))
        cursor = body.get("next_cursor")
        if not cursor or not page_markets:
            return markets
        if cursor in seen or page >= MAX_PAGES:
            raise ValueError(f"Gamma kept paging (page {page}, cursor {cursor!r} seen before: {cursor in seen}); giving up")
        seen.add(cursor)


def build_universe(markets: list[dict], how_many: int, now: datetime | None = None) -> dict:
    now = now or datetime.now(UTC)
    ranked = rank(markets)
    picked = [summarize(m, i + 1) for i, m in enumerate(pick_top(ranked, how_many))]
    return {
        "venue": "polymarket",
        "picked_at": now.isoformat(),
        "rule": f"top {how_many} open order-book markets by 24h volume, at most {MAX_PER_EVENT} per event",
        "open_markets_seen": len({str(m.get('id')) for m in markets}),
        "tradeable_markets_seen": len(ranked),
        "markets": picked,
    }


def save_universe(universe: dict, folder: Path) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    picked_at = datetime.fromisoformat(universe["picked_at"])
    path = folder / f"{picked_at:%Y-%m-%dT%H%M%SZ}.json"
    text = json.dumps(universe, indent=2, ensure_ascii=False)
    path.write_text(text)
    temporary = folder / "current.json.tmp"
    temporary.write_text(text)
    os.replace(temporary, folder / "current.json")  # atomic: readers see the old or the new list
    return path


class UniverseError(ValueError):
    """current.json is missing or not a valid market list."""


def load_universe(path: Path) -> tuple[dict[str, str], datetime]:
    """Read a saved pick. Returns ({token_id: printable label}, picked_at)."""
    try:
        universe = json.loads(path.read_text())
        names = {}
        for market in universe["markets"]:
            for outcome in market["outcomes"]:
                names[str(outcome["token_id"])] = f"{str(outcome['name'])[:10]:<10} | {str(market['question'])[:45]}"
        picked_at = datetime.fromisoformat(universe["picked_at"])
    except (OSError, ValueError, KeyError, TypeError) as problem:
        raise UniverseError(f"cannot read the market list {path}: {problem}") from problem
    if picked_at.tzinfo is None:
        picked_at = picked_at.replace(tzinfo=UTC)
    if not names:
        raise UniverseError(f"the market list {path} has no markets")
    return names, picked_at
