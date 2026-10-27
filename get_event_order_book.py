import json
import requests

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"


def _get(url: str, params: dict | None = None):
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def resolve_slug(slug: str) -> list[dict]:
    """Return the markets behind a slug, which may be an event or a market slug."""

    # Try as a single-market slug first
    markets = _get(f"{GAMMA}/markets", {"slug": slug})
    if markets:
        return markets
    
    # Try as an event slug (multi-market events, e.g. "La Liga champion")
    events = _get(f"{GAMMA}/events", {"slug": slug})
    if events:
        return events[0].get("markets", [])

    raise ValueError(f"No event or market found for slug {slug!r}")


def select_market(markets: list[dict], team: str | None = None) -> dict:
    """Pick one market. If the slug resolved to a single market, return it."""
    if team is None:
        if len(markets) == 1:
            return markets[0]
        raise ValueError(
            "Slug resolved to multiple markets; pass team=... Options:\n"
            + "\n".join(f"  - {m.get('question')} ({m.get('slug')})" for m in markets)
        )

    needle = team.lower()
    matches = [
        m for m in markets
        if needle in (m.get("question", "") + " " + m.get("groupItemTitle", "")).lower()
    ]
    if len(matches) != 1:
        raise ValueError(
            f"{len(matches)} markets match {team!r}: "
            + ", ".join(m.get("question", "?") for m in matches)
        )
    return matches[0]


def get_token_ids(market: dict) -> dict[str, str]:
    outcomes = json.loads(market["outcomes"])
    token_ids = json.loads(market["clobTokenIds"])
    return dict(zip(outcomes, token_ids))


def get_order_book(token_id: str) -> dict:
    book = _get(f"{CLOB}/book", {"token_id": token_id})
    book["bids"] = sorted(book["bids"], key=lambda x: float(x["price"]), reverse=True)
    book["asks"] = sorted(book["asks"], key=lambda x: float(x["price"]))
    return book


def fetch_books(slug: str, team: str | None = None) -> dict:
    market = select_market(resolve_slug(slug), team)
    tokens = get_token_ids(market)
    return {
        "question": market.get("question"),
        "condition_id": market.get("conditionId"),
        "tokens": tokens,
        "books": {outcome: get_order_book(tid) for outcome, tid in tokens.items()},
    }


if __name__ == "__main__":
    # nba-mem-uta-2026-10-26
    result = fetch_books("laliga-2027-champion-20260701200737375", team="Barcelona")
    # result = fetch_books("nba-mem-uta-2026-10-26", "Grizzlies")
    print(result["question"])
    for outcome, book in result["books"].items():
        print(outcome, "bid:", book["bids"][:1], "ask:", book["asks"][:1])