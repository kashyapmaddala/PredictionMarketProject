import requests

GAMMA = "https://gamma-api.polymarket.com"


def find_test_market() -> dict:
    """Return a currently active binary market with an order book."""
    r = requests.get(
        f"{GAMMA}/markets",
        params={
            "active": "true",
            "closed": "false",
            "order": "volume24hr",   # most liquid first, so the book isn't empty
            "ascending": "false",
            "limit": 20,
        },
        timeout=15,
    )
    r.raise_for_status()
    for m in r.json():
        if m.get("enableOrderBook") and m.get("acceptingOrders", True):
            return m
    raise RuntimeError("No active market found")


if __name__ == "__main__":
    m = find_test_market()
    print("slug:    ", m["slug"])
    print("question:", m["question"])
    print("outcomes:", m["outcomes"])
    print("tokens:  ", m["clobTokenIds"])