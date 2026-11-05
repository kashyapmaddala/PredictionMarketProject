"""Pick which Polymarket markets to watch (the "universe").

1. Pulls EVERY open Polymarket market from Gamma (Polymarket's market list), page by page, and
   saves each page exactly as received under data/raw/polymarket_markets/<day>/<hour>.jsonl.gz.
   That is our record of each market's info: question, rules, close time, tick size, fees...
2. Keeps markets that can actually be traded on the order book right now.
3. Ranks them by money traded in the last 24 hours and keeps the top N (default 100), taking at
   most 5 from any one event so a single big event can't fill the whole list.
4. Saves the pick to data/universe/polymarket/<UTC time>.json and copies it to current.json.
   Old picks are never deleted, so we always know which markets were being watched when.

    python3 pick_markets.py          # top 100
    python3 pick_markets.py 250      # top 250

Network errors, 429 (too many requests) and 5xx replies are retried with backoff.
Exit code 0 = saved a new list; anything else = failed (the old list is kept).
"""
import logging
import sys

import httpx

from pipeline import logs
from pipeline.config import HOW_MANY, HTTP_TIMEOUT, MAX_PER_EVENT, Paths
from pipeline.http import GiveUp
from pipeline.polymarket.universe import build_universe, pull_all_open_markets, save_universe
from pipeline.raw_store import RawStore

log = logging.getLogger("pick_markets")


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) > 1 or (argv and (not argv[0].isdigit() or int(argv[0]) < 1)):
        sys.exit(__doc__)
    how_many = int(argv[0]) if argv else HOW_MANY
    logs.setup()
    paths = Paths.from_env()
    store = RawStore(paths.raw_markets, compress=True)  # a full pull is ~1.6 GB uncompressed
    log.info("Downloading every open Polymarket market (saving each page as received)...")
    try:
        with httpx.Client(timeout=HTTP_TIMEOUT) as client:
            everything = pull_all_open_markets(client, store)
    except (GiveUp, ValueError) as problem:
        log.error("Could not download the market list: %s", problem)
        return 2
    finally:
        store.close()

    universe = build_universe(everything, how_many)
    if not universe["markets"]:
        log.error("No tradeable markets found; keeping the old list.")
        return 3
    path = save_universe(universe, paths.universe)
    picked = universe["markets"]
    events = {m["event_id"] for m in picked}
    log.info("Open markets: %d. Tradeable on the order book: %d.", universe["open_markets_seen"], universe["tradeable_markets_seen"])
    log.info("Picked the top %d by 24h volume (max %d per event), from %d different events.", len(picked), MAX_PER_EVENT, len(events))
    log.info("24h volume ranges from $%s (#1) to $%s (#%d).", f"{picked[0]['volume_24h']:,.0f}", f"{picked[-1]['volume_24h']:,.0f}", len(picked))
    log.info("Saved: %s and %s", path, paths.universe_file)
    for m in picked[:10]:
        log.info("  %3d. $%12s  %s", m["rank"], f"{m['volume_24h']:,.0f}", (m["question"] or "")[:70])
    return 0


if __name__ == "__main__":
    sys.exit(main())
