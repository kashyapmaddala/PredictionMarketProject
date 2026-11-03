"""Polymarket Listener: watch every picked market live, save every message, never quit.

Reads the market list made by pick_markets.py (data/universe/polymarket/current.json),
connects to Polymarket's live feed, and saves every message exactly as received to
data/raw/polymarket/<day>/<hour>.jsonl.gz (compressed; see raw_store.py). Press Ctrl+C to stop.

    python3 listener.py                     # every market in current.json (the normal way)
    python3 listener.py --verbose           # same, and print every change in plain English
    python3 listener.py <event-slug>        # just the questions in one event, e.g.
    python3 listener.py laliga-2027-champion-20260701200737375

Staying alive:
- Polymarket allows at most 500 tokens per connection, so the tokens are split
  across as many connections as needed (MAX_TOKENS_PER_CONNECTION each).
- If a connection drops, or goes silent for SILENCE_LIMIT seconds, it reconnects
  (waiting 1, 2, 4 ... up to 60 seconds between tries) and asks for a fresh full
  copy of every book, so nothing is built on a stale book.
- Every REPICK_EVERY_HOURS it runs pick_markets.py in the background (the old list
  keeps being saved meanwhile), then switches to the new list. Markets that closed
  drop out and newly busy markets come in.
- Every connect, subscribe, drop and list switch is written to the raw files as a
  note, so later we can tell a quiet market apart from a time we were offline.
- Every SNAPSHOT_EVERY seconds it also fetches every book straight from Polymarket's
  REST API and saves the reply (kind "rest_books"). check_data.py compares our
  rebuilt books against these independent copies.
"""
import asyncio
import fcntl
import json
import signal
import ssl
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import certifi
import httpx
import websockets

from raw_store import RawStore

HERE = Path(__file__).resolve().parent
UNIVERSE_FILE = HERE / "data" / "universe" / "polymarket" / "current.json"
PICKER = HERE / "pick_markets.py"
PICKER_LOG = HERE / "data" / "logs" / "pick_markets.log"
LOCK_FILE = HERE / "data" / "logs" / "listener.lock"
EVENT_URL = "https://gamma-api.polymarket.com/events"
LIVE_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BOOKS_URL = "https://clob.polymarket.com/books"

MAX_TOKENS_PER_CONNECTION = 400  # Polymarket's limit is 500; stay under it
SILENCE_LIMIT = 30  # seconds. We PING every 10s and Polymarket answers, so 30s of nothing = dead line
REPICK_EVERY_HOURS = 24
STATUS_EVERY = 60  # seconds between status lines
SNAPSHOT_EVERY = 300  # seconds between REST book snapshots (for checking our rebuilt books)
SNAPSHOT_BATCH = 200  # tokens per REST request

SECURE = ssl.create_default_context(cafile=certifi.where())


# ---------- which tokens to watch ----------

def load_universe():
    """Read current.json. Returns ({token_id: label}, picked_at)."""
    universe = json.loads(UNIVERSE_FILE.read_text())
    names = {}
    for market in universe["markets"]:
        for outcome in market["outcomes"]:
            names[outcome["token_id"]] = f"{outcome['name'][:10]:<10} | {market['question'][:45]}"
    picked_at = datetime.fromisoformat(universe["picked_at"])
    return names, picked_at


def questions_in_event(slug):
    """Ask Polymarket for one event (the slug is the end of its polymarket.com/event/... link)."""
    reply = httpx.get(EVENT_URL, params={"slug": slug}, timeout=20)
    reply.raise_for_status()
    events = reply.json()
    if not events:
        sys.exit(f"Polymarket has no event called '{slug}'. Copy the part after polymarket.com/event/ in its link.")
    event = events[0]
    print(f"Event: {event['title']}")
    names = {}
    for market in event.get("markets", []):
        if market.get("closed") or not market.get("enableOrderBook") or not market.get("clobTokenIds"):
            continue
        yes_tag, no_tag = json.loads(market["clobTokenIds"])
        label = (market.get("groupItemTitle") or market["question"])[:45]
        names[yes_tag] = f"YES        | {label}"
        names[no_tag] = f"NO         | {label}"
    return names


def split_into_connections(tokens):
    return [tokens[i:i + MAX_TOKENS_PER_CONNECTION] for i in range(0, len(tokens), MAX_TOKENS_PER_CONNECTION)]


# ---------- plain-English printing (only with --verbose) ----------

def describe(event, names):
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


def clock():
    return datetime.now().strftime("%H:%M:%S")


# ---------- the collector ----------

class Collector:
    def __init__(self, store, verbose):
        self.store = store
        self.verbose = verbose
        self.names = {}
        self.live = set()  # connection numbers currently connected
        self.saved = 0
        self.drops = 0
        self.snapshots_taken = 0

    async def connection(self, number, tokens):
        """Keep one connection alive forever for this group of tokens."""
        wait = 1
        while True:
            try:
                async with websockets.connect(LIVE_URL, ssl=SECURE, max_size=None) as line:
                    self.store.write("connected", {"connection": number, "url": LIVE_URL})
                    # initial_dump: Polymarket sends a full fresh book for every token first
                    request = {"type": "market", "assets_ids": tokens, "initial_dump": True}
                    await line.send(json.dumps(request))
                    self.store.write("subscribed", {"connection": number, "request": request})
                    self.live.add(number)
                    wait = 1

                    async def say_still_here():  # Polymarket hangs up without this
                        while True:
                            await asyncio.sleep(10)
                            await line.send("PING")

                    pinger = asyncio.create_task(say_still_here())
                    try:
                        while True:
                            message = await asyncio.wait_for(line.recv(), SILENCE_LIMIT)
                            received = time.time_ns()  # arrival time first, before anything else
                            if message == "PONG":
                                continue
                            self.store.write("message", message, received)
                            self.saved += 1
                            if self.verbose:
                                self.print_message(message)
                    finally:
                        pinger.cancel()
                        self.live.discard(number)
            except (websockets.WebSocketException, OSError, asyncio.TimeoutError) as problem:
                reason = f"no data for {SILENCE_LIMIT} seconds" if isinstance(problem, TimeoutError) else str(problem) or type(problem).__name__
                self.store.write("disconnected", {"connection": number, "reason": reason})
                self.drops += 1
                print(f"[{clock()}] Connection {number} dropped ({reason}). Reconnecting in {wait}s...")
                await asyncio.sleep(wait)
                wait = min(wait * 2, 60)

    def print_message(self, message):
        try:
            data = json.loads(message)
        except ValueError:
            return
        for event in data if isinstance(data, list) else [data]:
            text = describe(event, self.names) if isinstance(event, dict) else None
            if text:
                print(text)

    async def snapshots(self, tokens):
        """Every SNAPSHOT_EVERY seconds, fetch every book over REST and save the reply as received."""
        async with httpx.AsyncClient(timeout=30) as client:
            while True:
                await asyncio.sleep(SNAPSHOT_EVERY)
                for i in range(0, len(tokens), SNAPSHOT_BATCH):
                    batch = tokens[i:i + SNAPSHOT_BATCH]
                    try:
                        reply = await client.post(BOOKS_URL, json=[{"token_id": t} for t in batch])
                        received = time.time_ns()
                        reply.raise_for_status()
                        self.store.write("rest_books", reply.text, received)
                        self.snapshots_taken += 1
                    except httpx.HTTPError as problem:
                        self.store.write("rest_books_failed", {"reason": str(problem) or type(problem).__name__})

    async def status(self, total_connections):
        last = self.saved
        while True:
            await asyncio.sleep(STATUS_EVERY)
            print(f"[{clock()}] {len(self.live)}/{total_connections} connections up | "
                  f"{self.saved:,} messages saved ({self.saved - last:,} in the last minute) | drops so far: {self.drops} | "
                  f"book snapshots: {self.snapshots_taken}")
            last = self.saved

    async def run(self, names):
        """Watch these tokens until cancelled."""
        self.names = names
        groups = split_into_connections(list(names))
        print(f"[{clock()}] Watching {len(names)} tokens over {len(groups)} connection(s).")
        tasks = [asyncio.create_task(self.connection(i + 1, g)) for i, g in enumerate(groups)]
        tasks.append(asyncio.create_task(self.status(len(groups))))
        tasks.append(asyncio.create_task(self.snapshots(list(names))))
        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


# ---------- daily re-pick ----------

async def next_universe(picked_at):
    """Wait until the list is REPICK_EVERY_HOURS old, re-pick in the background, return the new list."""
    while True:
        age = (datetime.now(timezone.utc) - picked_at).total_seconds()
        await asyncio.sleep(max(0, REPICK_EVERY_HOURS * 3600 - age))
        print(f"[{clock()}] Market list is {REPICK_EVERY_HOURS}h old. Re-picking in the background "
              f"(about 10 minutes; still saving meanwhile). Log: {PICKER_LOG}")
        PICKER_LOG.parent.mkdir(parents=True, exist_ok=True)
        with open(PICKER_LOG, "a") as log:
            picker = await asyncio.create_subprocess_exec(
                sys.executable, str(PICKER), stdout=log, stderr=subprocess.STDOUT, cwd=HERE)
            code = await picker.wait()
        if code == 0:
            return load_universe()
        print(f"[{clock()}] Re-pick failed (see {PICKER_LOG}). Keeping the old list, trying again in 1 hour.")
        picked_at = datetime.now(timezone.utc) - timedelta(hours=REPICK_EVERY_HOURS - 1)  # due again in 1 hour


async def watch_universe(collector):
    names, picked_at = load_universe()
    while True:
        print(f"[{clock()}] Using the market list picked at {picked_at:%Y-%m-%d %H:%M} UTC.")
        watching = asyncio.create_task(collector.run(names))
        new_names, new_picked_at = await next_universe(picked_at)
        added, removed = set(new_names) - set(names), set(names) - set(new_names)
        collector.store.write("universe_changed", {
            "picked_at": new_picked_at.isoformat(), "tokens_added": sorted(added), "tokens_removed": sorted(removed)})
        print(f"[{clock()}] New list: {len(added)} tokens added, {len(removed)} removed. Switching over.")
        # Polymarket has no "unsubscribe", so switching means reconnecting with the new list
        watching.cancel()
        await asyncio.gather(watching, return_exceptions=True)
        names, picked_at = new_names, new_picked_at


# ---------- start ----------

def stop_politely(signum, frame):
    raise KeyboardInterrupt  # treat "pkill" like Ctrl+C, so the "stopped" note gets written


def only_one_copy():
    """Refuse to start if another listener is already running: two copies would save
    everything twice and could write into the same file at once, which breaks it."""
    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    lock = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)  # released automatically when this program ends
    except BlockingIOError:
        sys.exit("Another listener is already running (check with: pgrep -fl listener.py). Not starting a second copy.")
    return lock


def main():
    signal.signal(signal.SIGTERM, stop_politely)
    lock = only_one_copy()  # keep a reference so the lock is held while we run
    args = [a for a in sys.argv[1:] if a != "--verbose"]
    verbose = "--verbose" in sys.argv[1:]
    store = RawStore("polymarket", compress=True)  # ~2.3 GB/hour uncompressed for 100 markets
    collector = Collector(store, verbose)

    if args:
        print("Asking Polymarket for that event...")
        names = questions_in_event(args[0])
        job = collector.run(names)
    else:
        if not UNIVERSE_FILE.exists():
            print("No market list yet. Running pick_markets.py first (about 10 minutes)...")
            subprocess.run([sys.executable, str(PICKER)], check=True, cwd=HERE)
        job = watch_universe(collector)

    print(f"Saving every message under {store.folder}")
    print("Press Ctrl+C to stop.\n")
    try:
        asyncio.run(job)
    except KeyboardInterrupt:
        store.write("stopped", {"reason": "Ctrl+C"})
        print(f"\nStopped. Saved {collector.saved:,} messages. Bye!")
    finally:
        store.close()


if __name__ == "__main__":
    main()
