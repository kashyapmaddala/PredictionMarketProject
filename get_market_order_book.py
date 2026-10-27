import json
import requests

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"


def search_markets(query: str) -> list[dict]:
    """Search and flatten markets out of the matching events."""
    r = requests.get(f"{GAMMA}/public-search", params={"q": query}, timeout=15)
    r.raise_for_status()
    markets = []
    for event in r.json().get("events", []):
        for m in event.get("markets", []):
            m["_event_title"] = event.get("title")
            markets.append(m)
    return markets


def get_market_by_slug(slug: str) -> dict:
    r = requests.get(f"{GAMMA}/markets", params={"slug": slug}, timeout=15)
    r.raise_for_status()
    data = r.json()
    if not data:
        raise ValueError(f"No market with slug {slug!r}")
    return data[0]


def get_token_ids(market: dict) -> dict[str, str]:
    """Return {'Yes': token_id, 'No': token_id}."""
    outcomes = json.loads(market["outcomes"])
    token_ids = json.loads(market["clobTokenIds"])
    return dict(zip(outcomes, token_ids))


def get_order_book(token_id: str) -> dict:
    r = requests.get(f"{CLOB}/book", params={"token_id": token_id}, timeout=15)
    r.raise_for_status()
    book = r.json()
    # Sort defensively rather than trusting server ordering
    book["bids"] = sorted(book["bids"], key=lambda x: float(x["price"]), reverse=True)
    book["asks"] = sorted(book["asks"], key=lambda x: float(x["price"]))
    return book


if __name__ == "__main__":
    # Step 1: discover. Print candidates so you can pick the right one
    # (and grab its slug for hard-coding later).
    candidates = search_markets("Will FC Barcelona win")
    for m in candidates:
        print(m["slug"], "|", m.get("question"), "| closed:", m.get("closed"))

    # Step 2: pick the market (replace with the slug printed above)
    market = get_market_by_slug("lal-bar-ala-2026-10-31-bar")
    tokens = get_token_ids(market)
    print(tokens)

    # Step 3: pull both books
    books = {outcome: get_order_book(tid) for outcome, tid in tokens.items()}
    for outcome, book in books.items():
        print(outcome, "best bid:", book["bids"][:1], "best ask:", book["asks"][:1])