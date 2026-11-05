"""Polymarket live collector: watch every picked market live, save every message, never quit.

Reads the market list made by pick_markets.py (data/universe/polymarket/current.json), connects
to Polymarket's live feed, and saves every message exactly as received to
data/raw/polymarket/<day>/<hour>.jsonl.gz (see pipeline/raw_store.py). Stop it with Ctrl+C or
`pkill -f collect_live.py` (SIGTERM); either way it writes a "stopped" note and closes its files.

    python3 collect_live.py                     # every market in current.json (the normal way)
    python3 collect_live.py --verbose           # same, and print every change in plain English
    python3 collect_live.py --standby           # two connections per group (no gap on a drop; 2x disk)
    python3 collect_live.py <event-slug>        # just the questions in one event, e.g.
    python3 collect_live.py laliga-2027-champion-20260701200737375

How it stays alive, and everything it writes, is explained in pipeline/polymarket/collector.py.
Only one copy can run at a time (a lock file in data/logs). Exit codes: 0 stopped cleanly,
1 another copy is running or bad arguments, 2 saving data failed (disk full?), 3 no market list.
"""
import asyncio
import json
import logging
import subprocess
import sys

import httpx

from pipeline import logs
from pipeline.config import GAMMA_EVENTS_URL, HTTP_TIMEOUT, Paths
from pipeline.http import GiveUp, request
from pipeline.polymarket.collector import (
    AlreadyRunning,
    Collector,
    Settings,
    StorageFailed,
    acquire_lock,
    new_session,
    run_until_stopped,
)
from pipeline.polymarket.universe import UniverseError, load_universe
from pipeline.raw_store import RawStore

log = logging.getLogger("collector")


def questions_in_event(slug):
    """Ask Polymarket for one event (the slug is the end of its polymarket.com/event/... link)."""
    with httpx.Client(timeout=HTTP_TIMEOUT) as client:
        events = request(client, "GET", GAMMA_EVENTS_URL, params={"slug": slug}).json()
    if not events:
        sys.exit(f"Polymarket has no event called '{slug}'. Copy the part after polymarket.com/event/ in its link.")
    event = events[0]
    log.info("Event: %s", event.get("title"))
    names = {}
    for market in event.get("markets", []):
        if market.get("closed") or not market.get("enableOrderBook") or not market.get("clobTokenIds"):
            continue
        yes_tag, no_tag = json.loads(market["clobTokenIds"])
        label = (market.get("groupItemTitle") or market["question"])[:45]
        names[yes_tag] = f"YES        | {label}"
        names[no_tag] = f"NO         | {label}"
    return names


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    flags = {a for a in argv if a.startswith("--")}
    args = [a for a in argv if not a.startswith("--")]
    if flags - {"--verbose", "--standby"} or len(args) > 1:
        print(__doc__)
        return 1
    logs.setup()
    paths = Paths.from_env()
    try:
        lock = acquire_lock(paths.lock_file)  # keep a reference so the lock is held while we run
    except AlreadyRunning as problem:
        log.error("%s. Not starting a second copy.", problem)
        return 1

    settings = Settings.from_env(verbose="--verbose" in flags, standby="--standby" in flags)
    picked_at = None
    try:
        if args:
            log.info("Asking Polymarket for that event...")
            names = questions_in_event(args[0])
        else:
            if not paths.universe_file.exists():
                log.info("No market list yet. Running pick_markets.py first (about 10 minutes)...")
                subprocess.run(settings.picker, check=True)
            names, picked_at = load_universe(paths.universe_file)
    except (UniverseError, GiveUp, subprocess.CalledProcessError) as problem:
        log.error("No market list to watch: %s", problem)
        return 3

    store = RawStore(paths.raw, compress=True, session=new_session())  # ~2.3 GB/hour uncompressed for 100 markets
    collector = Collector(store, settings, paths=paths)
    log.info("Saving every message under %s (session %s). Ctrl+C stops.", store.folder, store.session)
    code = 0
    try:
        why = asyncio.run(run_until_stopped(collector, collector.run(names, picked_at)))
        log.info("Stopped (%s). Saved %s messages. Bye!", why, f"{collector.saved:,}")
    except StorageFailed as problem:
        log.critical("Stopping: %s", problem)
        code = 2
    finally:
        try:
            store.close()
        except OSError as problem:
            log.critical("Could not close the raw file cleanly: %s", problem)
            code = 2
        lock.close()
    return code


if __name__ == "__main__":
    sys.exit(main())
