"""Pick which Polymarket markets to watch (the "universe").

1. Pulls EVERY open Polymarket market from Gamma (Polymarket's market list),
   page by page, and saves each page exactly as received under
   data/raw/polymarket_markets/<day>/<hour>.jsonl.gz (compressed; see raw_store.py).
   That is our record of each market's info: question, rules, close time,
   tick size, fees and so on.
2. Keeps markets that can actually be traded on the order book right now.
3. Ranks them by money traded in the last 24 hours and keeps the top N (default 100),
   taking at most MAX_PER_EVENT from any one event so a single big event
   (like a 50-state election map) can't fill the whole list.
4. Saves the pick to data/universe/polymarket/<UTC time>.json and copies it to
   data/universe/polymarket/current.json. Old picks are never deleted, so we
   always know which markets were being watched when.

    python3 pick_markets.py          # top 100
    python3 pick_markets.py 250      # top 250
"""
import json
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

from raw_store import RawStore

HOW_MANY = 100
MAX_PER_EVENT = 5
LIST_URL = "https://gamma-api.polymarket.com/markets/keyset"
PAGE_SIZE = 1000
UNIVERSE_DIR = Path(__file__).resolve().parent / "data" / "universe" / "polymarket"


def pull_all_open_markets(store):
    """Download every open market. Saves each raw page before using it."""
    markets, cursor, page = [], None, 0
    with httpx.Client(timeout=60) as client:
        while True:
            # version=v1 AND version=v2: the default is v1 only, which would miss newer markets
            params = [("closed", "false"), ("version", "v1"), ("version", "v2"), ("limit", PAGE_SIZE)]
            if cursor:
                params.append(("after_cursor", cursor))
            for attempt in range(5):
                try:
                    reply = client.get(LIST_URL, params=params)
                    if reply.status_code == 429:  # too many requests: wait and try again
                        raise httpx.HTTPStatusError("rate limited", request=reply.request, response=reply)
                    reply.raise_for_status()
                    break
                except (httpx.HTTPError, httpx.HTTPStatusError) as problem:
                    if attempt == 4:
                        raise
                    print(f"  page {page + 1} failed ({problem}), retrying...")
                    time.sleep(2 ** attempt)
            received = time.time_ns()
            store.write("gamma_markets_page", reply.text, received)
            body = reply.json()
            markets += body.get("markets", [])
            page += 1
            print(f"  page {page}: {len(markets)} markets so far")
            cursor = body.get("next_cursor")
            if not cursor or not body.get("markets"):
                return markets


def token_ids(market):
    """The two outcome tokens. v1 markets list them in clobTokenIds (a JSON string), v2 in positionIds."""
    ids = market.get("clobTokenIds") or market.get("positionIds")
    if isinstance(ids, str):
        ids = json.loads(ids)
    return [str(i) for i in ids] if ids else []


def outcome_names(market):
    names = market.get("outcomes")
    if isinstance(names, str):
        names = json.loads(names)
    return names or []


def number(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def tradeable(market):
    return (
        market.get("enableOrderBook")
        and market.get("acceptingOrders")
        and not market.get("closed")
        and len(token_ids(market)) == 2
    )


def event_id(market):
    events = market.get("events") or []
    return events[0].get("id") if events else f"no-event-{market.get('id')}"


def pick_top(ranked, how_many):
    """Walk down the ranking, skipping markets whose event already has MAX_PER_EVENT picks."""
    picked, per_event = [], {}
    for market in ranked:
        key = event_id(market)
        if per_event.get(key, 0) >= MAX_PER_EVENT:
            continue
        per_event[key] = per_event.get(key, 0) + 1
        picked.append(market)
        if len(picked) == how_many:
            break
    return picked


def summarize(market, rank):
    event = (market.get("events") or [{}])[0]
    tokens, names = token_ids(market), outcome_names(market)
    return {
        "rank": rank,
        "market_id": market.get("id"),
        "condition_id": market.get("conditionId"),
        "question": market.get("question"),
        "slug": market.get("slug"),
        "version": market.get("version"),
        "event_id": event.get("id"),
        "event_slug": event.get("slug"),
        "event_title": event.get("title"),
        # outcome order matters: index 0 is usually "Yes", but sports markets use team names
        "outcomes": [{"name": n, "token_id": t} for n, t in zip(names, tokens)],
        "tick_size": market.get("orderPriceMinTickSize"),
        "min_order_size": market.get("orderMinSize"),
        "neg_risk": market.get("negRisk"),
        "end_date": market.get("endDate"),
        "volume_24h": number(market.get("volume24hr")),
        "liquidity": number(market.get("liquidityNum")),
    }


def main():
    how_many = int(sys.argv[1]) if len(sys.argv) > 1 else HOW_MANY
    store = RawStore("polymarket_markets", compress=True)  # a full pull is ~1.6 GB uncompressed
    print("Downloading every open Polymarket market (saving each page as received)...")
    try:
        everything = pull_all_open_markets(store)
    finally:
        store.close()

    unique = {m["id"]: m for m in everything}  # in case a market shows up on two pages
    can_trade = [m for m in unique.values() if tradeable(m)]
    ranked = sorted(can_trade, key=lambda m: number(m.get("volume24hr")), reverse=True)
    picked = [summarize(m, i + 1) for i, m in enumerate(pick_top(ranked, how_many))]

    now = datetime.now(timezone.utc)
    universe = {
        "venue": "polymarket",
        "picked_at": now.isoformat(),
        "rule": f"top {how_many} open order-book markets by 24h volume, at most {MAX_PER_EVENT} per event",
        "open_markets_seen": len(unique),
        "tradeable_markets_seen": len(can_trade),
        "markets": picked,
    }
    UNIVERSE_DIR.mkdir(parents=True, exist_ok=True)
    path = UNIVERSE_DIR / f"{now:%Y-%m-%dT%H%M%SZ}.json"
    path.write_text(json.dumps(universe, indent=2, ensure_ascii=False))
    shutil.copyfile(path, UNIVERSE_DIR / "current.json")

    events = {m["event_id"] for m in picked}
    print(f"\nOpen markets: {len(unique)}. Tradeable on the order book: {len(can_trade)}.")
    print(f"Picked the top {len(picked)} by 24h volume (max {MAX_PER_EVENT} per event), from {len(events)} different events.")
    if picked:
        print(f"24h volume ranges from ${picked[0]['volume_24h']:,.0f} (#1) to ${picked[-1]['volume_24h']:,.0f} (#{len(picked)}).")
    print(f"\nSaved: {path}\nAnd:   {UNIVERSE_DIR / 'current.json'}\n\nTop 10:")
    for m in picked[:10]:
        print(f"  {m['rank']:>3}. ${m['volume_24h']:>12,.0f}  {m['question'][:70]}")


if __name__ == "__main__":
    main()
