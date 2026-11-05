"""Opt-in live test against the real Polymarket API (no account needed). Skipped by default and
in CI. Run it with:

    python3 -m pytest -m live

It picks the 3 busiest open markets from one page of Gamma, runs the real collector on them for
about 60 seconds (with a REST copy every 20 s) into a temporary folder, and validates the result.
"""
import asyncio
import os

import httpx
import pytest

from pipeline.config import GAMMA_MARKETS_URL, Paths
from pipeline.http import request
from pipeline.polymarket.collector import Collector, Settings, run_until_stopped
from pipeline.polymarket.universe import pick_top, rank, token_ids
from pipeline.polymarket.validate import validate
from pipeline.raw_store import RawStore

pytestmark = pytest.mark.live
SECONDS = float(os.environ.get("PM_LIVE_SECONDS", "60"))


def busiest_tokens(how_many=3):
    with httpx.Client(timeout=30) as client:
        params = [("closed", "false"), ("version", "v1"), ("version", "v2"), ("limit", 500)]
        markets = request(client, "GET", GAMMA_MARKETS_URL, params=params).json()["markets"]
    picked = pick_top(rank(markets), how_many)
    return {t: m["question"][:40] for m in picked for t in token_ids(m)}


def test_live_collector_for_a_minute(tmp_path):
    names = busiest_tokens()
    assert len(names) == 6
    paths = Paths(tmp_path / "data")
    store = RawStore(paths.raw, compress=True, session="live-test")
    collector = Collector(store, Settings(snapshot_every=20, status_every=30), paths=paths)

    async def main():
        loop = asyncio.get_running_loop()
        job = asyncio.ensure_future(run_until_stopped(collector, collector.run(names)))
        loop.call_later(SECONDS, job.cancel)
        try:
            await job
        except asyncio.CancelledError:
            collector.save("stopped", {"reason": "live test over"})

    asyncio.run(main())
    store.close()
    assert collector.saved > 0 and collector.pongs > 0
    report = validate(paths)
    stats = report.engine.stats
    assert stats.count["book"] >= 6, report.text  # the initial dump: one full book per token
    assert stats.count["REST books compared"] >= 1, report.text
    assert not report.failed, report.text
