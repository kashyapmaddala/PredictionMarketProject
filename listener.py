"""Starter Listener: watch live Polymarket order book changes.

Finds the busiest Polymarket questions, connects to Polymarket's live feed,
and prints every change in plain English. Press Ctrl+C to stop.
"""
import asyncio
import json
import ssl

import certifi
import httpx
import websockets

HOW_MANY_QUESTIONS = 3
LIST_URL = "https://gamma-api.polymarket.com/markets"
LIVE_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


def pick_busiest_questions():
    """Ask Polymarket for its busiest open questions and grab their YES/NO name tags."""
    reply = httpx.get(
        LIST_URL,
        params={"closed": "false", "order": "volume24hr", "ascending": "false", "limit": 20},
        timeout=20,
    )
    reply.raise_for_status()
    questions = []
    for market in reply.json():
        if not market.get("enableOrderBook") or not market.get("clobTokenIds"):
            continue
        yes_tag, no_tag = json.loads(market["clobTokenIds"])
        questions.append({"question": market["question"], "yes": yes_tag, "no": no_tag})
        if len(questions) == HOW_MANY_QUESTIONS:
            break
    return questions


def describe(event, names):
    """Turn one Polymarket message into a plain-English line."""
    kind = event.get("event_type")
    if kind == "book":
        name = names.get(event["asset_id"], "?")
        bids, asks = event.get("bids", []), event.get("asks", [])
        best_bid = bids[-1]["price"] if bids else "none"
        best_ask = asks[-1]["price"] if asks else "none"
        return f"FULL BOARD  {name}: best buy {best_bid}, best sell {best_ask}"
    if kind == "price_change":
        lines = []
        for change in event.get("price_changes", []):
            name = names.get(change["asset_id"], "?")
            who = "buyers" if change["side"] == "BUY" else "sellers"
            lines.append(f"UPDATE      {name}: {who} at {change['price']} now total {change['size']}")
        return "\n".join(lines)
    if kind == "last_trade_price":
        name = names.get(event["asset_id"], "?")
        return f"TRADE       {name}: {event.get('size')} traded at {event['price']}"
    return None


async def listen(questions):
    names = {}
    for q in questions:
        short = q["question"][:45]
        names[q["yes"]] = f"YES | {short}"
        names[q["no"]] = f"NO  | {short}"

    secure = ssl.create_default_context(cafile=certifi.where())
    while True:  # if the line drops, call back
        try:
            async with websockets.connect(LIVE_URL, ssl=secure, max_size=None) as line:
                await line.send(json.dumps({"type": "market", "assets_ids": list(names), "initial_dump": True}))
                print("Connected. Listening... (press Ctrl+C to stop)\n")

                async def say_still_here():  # Polymarket hangs up without this every 10 seconds
                    while True:
                        await asyncio.sleep(10)
                        await line.send("PING")

                pinger = asyncio.create_task(say_still_here())
                try:
                    async for message in line:
                        if message == "PONG":
                            continue
                        data = json.loads(message)
                        for event in data if isinstance(data, list) else [data]:
                            text = describe(event, names)
                            if text:
                                print(text)
                finally:
                    pinger.cancel()
        except (websockets.ConnectionClosed, OSError) as problem:
            print(f"\nLine dropped ({problem}). Calling back in 3 seconds...\n")
            await asyncio.sleep(3)


def main():
    print("Asking Polymarket for its busiest questions...")
    questions = pick_busiest_questions()
    for q in questions:
        print(f"  - {q['question']}")
    print()
    try:
        asyncio.run(listen(questions))
    except KeyboardInterrupt:
        print("\nStopped. Bye!")


if __name__ == "__main__":
    main()
