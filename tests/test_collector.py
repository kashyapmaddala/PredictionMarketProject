"""The live collector against a local fake Polymarket (WebSocket and REST)."""
import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from pipeline.config import Paths
from pipeline.polymarket import collector as collector_module
from pipeline.polymarket.collector import (
    AlreadyRunning,
    Collector,
    Settings,
    StorageFailed,
    acquire_lock,
    describe_message,
    reconnect_wait,
    run_until_stopped,
    split_into_shards,
)
from pipeline.raw_reader import RawFile, raw_files
from pipeline.raw_store import RawStore
from tests.fake_polymarket import FakePolymarket

REPO = Path(__file__).resolve().parent.parent
TOKENS = [f"{i}" * 70 for i in range(1, 6)]


def settings(url, **kw):
    base = dict(live_url=url, tls=False, app_ping_every=0.2, silence_limit=2.0, status_every=3600,
                snapshot_every=3600, housekeeping_every=0.1, overlap_seconds=0.1, switch_timeout=5)
    base.update(kw)
    return Settings(**base)


def lines(paths):
    out = []
    for path in raw_files(paths.raw):
        out += [e for _n, e in RawFile(path)]
    return out


async def run_for(collector, names, seconds, picked_at=None):
    task = asyncio.create_task(collector.run(names, picked_at))
    await asyncio.sleep(seconds)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def make(tmp_path, url, **kw):
    paths = Paths(tmp_path / "data")
    store = RawStore(paths.raw, compress=True, session="test")
    return paths, store, Collector(store, settings(url, **kw), paths=paths)


# ---------- pure helpers ----------

def test_shards_and_backoff():
    assert split_into_shards(TOKENS, 2) == [TOKENS[:2], TOKENS[2:4], TOKENS[4:]]
    assert split_into_shards([], 3) == []
    with pytest.raises(ValueError):
        split_into_shards(TOKENS, 0)
    assert reconnect_wait(0, 60) == 0.0  # the first retry is immediate
    assert 0.75 <= reconnect_wait(1, 60) <= 1.25
    assert reconnect_wait(30, 60) <= 75


def test_describe_message():
    text = json.dumps([{"event_type": "book", "asset_id": "a", "bids": [{"price": "0.4"}], "asks": []},
                       {"event_type": "price_change", "price_changes": [{"asset_id": "a", "side": "SELL", "price": "0.6", "size": "3"}]},
                       {"event_type": "last_trade_price", "asset_id": "a", "price": "0.5", "size": "2"}, 5])
    out = describe_message(text, {"a": "Yes | Q"})
    assert out == ["FULL BOARD  Yes | Q: best buy 0.4, best sell none", "UPDATE      Yes | Q: sellers at 0.6 now total 3",
                   "TRADE       Yes | Q: 2 traded at 0.5"]
    assert describe_message("nope", {}) == []


# ---------- against the fake server ----------

def test_subscribes_saves_the_dump_and_every_message(tmp_path):
    async def main():
        async with FakePolymarket() as fake:
            paths, store, c = make(tmp_path, fake.url)
            await run_for(c, {t: t for t in TOKENS[:2]}, 0.8)
            store.close()
            return fake, paths, c

    fake, paths, c = asyncio.run(main())
    assert fake.subscriptions == [{"type": "market", "assets_ids": TOKENS[:2], "initial_dump": True}]
    assert fake.pings >= 2  # our "PING" keepalive
    got = lines(paths)
    kinds = [e["kind"] for e in got]
    assert kinds[:3] == ["started", "connected", "subscribed"]
    assert all(e["session"] == "test" for e in got)
    assert {e.get("conn") for e in got if e["kind"] == "message"} == {1}
    dump = json.loads(next(e["raw"] for e in got if e["kind"] == "message"))
    assert [b["asset_id"] for b in dump] == TOKENS[:2]
    assert c.saved == kinds.count("message") >= 5
    assert not any(e["raw"] == "PONG" for e in got)  # keepalive answers are not data


def test_silence_means_reconnect(tmp_path):
    async def main():
        async with FakePolymarket("silent") as fake:
            paths, store, c = make(tmp_path, fake.url, silence_limit=0.5)
            await run_for(c, {TOKENS[0]: "a"}, 1.6)
            store.close()
            return fake, paths

    fake, paths = asyncio.run(main())
    got = lines(paths)
    drops = [e for e in got if e["kind"] == "disconnected"]
    assert drops[0]["raw"]["reason"] == "no data for 0.5 seconds" and drops[0]["conn"] == 1
    assert fake.connections >= 2
    assert [e["conn"] for e in got if e["kind"] == "connected"][:2] == [1, 2]


def test_server_drop_reconnects_immediately(tmp_path):
    async def main():
        async with FakePolymarket("abort", abort_after=3) as fake:
            paths, store, c = make(tmp_path, fake.url)
            await run_for(c, {TOKENS[0]: "a"}, 1.2)
            store.close()
            return paths, c

    paths, c = asyncio.run(main())
    got = lines(paths)
    drop = next(e for e in got if e["kind"] == "disconnected")
    assert "no close frame" in drop["raw"]["reason"]
    again = next(e for e in got if e["kind"] == "connected" and e["conn"] == 2)
    assert again["recv_ts_ns"] - drop["recv_ts_ns"] < 300_000_000  # back within 0.3 s, no waiting
    assert c.drops == 1


def test_malformed_frames_are_saved_raw(tmp_path):
    async def main():
        async with FakePolymarket("malformed") as fake:
            paths, store, c = make(tmp_path, fake.url)
            await run_for(c, {TOKENS[0]: "a"}, 0.5)
            store.close()
            return paths

    got = lines(asyncio.run(main()))
    assert any(e["kind"] == "message" and e["raw"] == "INVALID OPERATION" for e in got)
    binary = next(e for e in got if e["kind"] == "message_binary")
    assert binary["raw"] == "AAFiaW5hcnk="  # base64 of the exact bytes


def test_tokens_are_split_across_connections(tmp_path):
    async def main():
        async with FakePolymarket() as fake:
            paths, store, c = make(tmp_path, fake.url, max_tokens=2)
            await run_for(c, {t: t for t in TOKENS}, 0.6)
            store.close()
            return fake, paths

    fake, paths = asyncio.run(main())
    assert sorted(len(s["assets_ids"]) for s in fake.subscriptions) == [1, 2, 2]
    assert sorted(t for s in fake.subscriptions for t in s["assets_ids"]) == sorted(TOKENS)
    subscribed = [e for e in lines(paths) if e["kind"] == "subscribed"]
    assert sorted(e["raw"]["shard"] for e in subscribed) == [1, 2, 3]
    assert len({e["conn"] for e in subscribed}) == 3


def test_standby_runs_two_connections_per_shard(tmp_path):
    async def main():
        async with FakePolymarket() as fake:
            _paths, store, c = make(tmp_path, fake.url, standby=True)
            await run_for(c, {TOKENS[0]: "a"}, 0.5)
            store.close()
            return fake, c

    fake, c = asyncio.run(main())
    assert fake.connections == 2 and c.shards_total == 2


def test_universe_switch_is_make_before_break(tmp_path):
    """The new list's connections are open and have data before the old ones are retired."""
    paths = Paths(tmp_path / "data")
    paths.universe.mkdir(parents=True)
    new_list = {"picked_at": datetime.now(UTC).isoformat(),
                "markets": [{"question": "Q2", "outcomes": [{"name": "Yes", "token_id": TOKENS[1]}, {"name": "No", "token_id": TOKENS[2]}]}]}
    picker = [sys.executable, "-c", f"import json,os,pathlib; p=pathlib.Path(os.environ['PM_DATA_DIR'])/'universe'/'polymarket'/'current.json'; "
              f"p.write_text(json.dumps({json.dumps(new_list)!s}))"]

    async def main():
        async with FakePolymarket() as fake:
            store = RawStore(paths.raw, compress=True, session="test")
            c = Collector(store, settings(fake.url, picker=picker, repick_every_hours=1), paths=paths)
            old_pick = datetime.now(UTC) - timedelta(hours=2)  # due now
            await run_for(c, {TOKENS[0]: "a", TOKENS[1]: "b"}, 2.5, old_pick)
            store.close()
            return fake

    fake = asyncio.run(main())
    assert [s["assets_ids"] for s in fake.subscriptions] == [[TOKENS[0], TOKENS[1]], [TOKENS[1], TOKENS[2]]]
    got = lines(paths)
    kinds = [(e["kind"], e.get("conn")) for e in got if e["kind"] != "message"]
    assert ("universe_changed", None) in kinds
    changed = next(e for e in got if e["kind"] == "universe_changed")
    assert changed["raw"]["tokens_added"] == [TOKENS[2]] and changed["raw"]["tokens_removed"] == [TOKENS[0]]
    retired = next(i for i, e in enumerate(got) if e["kind"] == "retired")
    first_new = next(i for i, e in enumerate(got) if e["kind"] == "message" and e["conn"] == 2)
    assert first_new < retired and got[retired]["conn"] == 1
    assert not any(e["kind"] == "disconnected" for e in got)


def test_failed_repick_keeps_the_old_list(tmp_path):
    async def main():
        async with FakePolymarket() as fake:
            paths, store, c = make(tmp_path, fake.url, picker=[sys.executable, "-c", "raise SystemExit(4)"],
                                   repick_every_hours=1, repick_retry_hours=1)
            await run_for(c, {TOKENS[0]: "a"}, 1.0, datetime.now(UTC) - timedelta(hours=2))
            store.close()
            return fake, paths

    fake, paths = asyncio.run(main())
    assert len(fake.subscriptions) == 1
    assert "exit code 4" not in "".join(e["kind"] for e in lines(paths))
    assert (paths.picker_log).exists()


def test_repick_timeout_kills_the_picker(tmp_path):
    async def main():
        _paths, store, c = make(tmp_path, "ws://127.0.0.1:9", picker=[sys.executable, "-c", "import time; time.sleep(30)"],
                               repick_timeout=0.3)
        started = time.monotonic()
        ok = await c.repick()
        store.close()
        return ok, time.monotonic() - started

    ok, took = asyncio.run(main())
    assert not ok and took < 5


def test_pause_is_noticed_and_every_connection_restarted(tmp_path):
    wall = [1000.0]

    async def main():
        async with FakePolymarket() as fake:
            paths, store, c = make(tmp_path, fake.url)
            c.wall = lambda: wall[0] + time.monotonic()
            task = asyncio.create_task(c.run({TOKENS[0]: "a"}))
            await asyncio.sleep(0.5)
            wall[0] += 300  # the computer "slept" for 5 minutes
            await asyncio.sleep(0.8)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            store.close()
            return paths, c

    paths, c = asyncio.run(main())
    got = lines(paths)
    paused = next(e for e in got if e["kind"] == "paused")
    assert 299 < paused["raw"]["seconds"] < 302 and c.pauses == 1
    drop = next(e for e in got if e["kind"] == "disconnected")
    assert drop["raw"]["reason"].startswith("reconnect after pause")
    assert any(e["kind"] == "connected" and e["conn"] == 2 for e in got)


def test_rest_copies_are_saved_and_failures_noted(tmp_path):
    calls = []

    def handler(request):
        calls.append(json.loads(request.content))
        if len(calls) <= 3:
            return httpx.Response(503)
        return httpx.Response(200, text='[{"asset_id": "x"}]')

    async def main():
        async with FakePolymarket() as fake:
            client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            paths, store, c = make(tmp_path, fake.url, snapshot_every=0.2, snapshot_batch=1)
            c.http_client = client
            orig = collector_module.arequest

            async def fast(*a, **k):
                return await orig(*a, sleep=lambda _s: asyncio.sleep(0), **k)

            collector_module.arequest = fast
            try:
                await run_for(c, {TOKENS[0]: "a"}, 0.9)
            finally:
                collector_module.arequest = orig
                await client.aclose()
            store.close()
            return paths

    got = lines(asyncio.run(main()))
    failed = [e for e in got if e["kind"] == "rest_books_failed"]
    saved = [e for e in got if e["kind"] == "rest_books"]
    assert failed and "HTTP 503" in failed[0]["raw"]["reason"]
    assert saved and saved[0]["raw"] == '[{"asset_id": "x"}]'
    assert calls[0] == [{"token_id": TOKENS[0]}]


def test_disk_full_stops_the_collector(tmp_path):
    class FullStore(RawStore):
        def write(self, *a, **k):
            if self.lines_written >= 3:
                raise OSError(28, "No space left on device")
            return super().write(*a, **k)

    async def main():
        async with FakePolymarket() as fake:
            paths = Paths(tmp_path / "data")
            store = FullStore(paths.raw, compress=True, session="t")
            c = Collector(store, settings(fake.url), paths=paths)
            with pytest.raises(StorageFailed, match="No space left"):
                await asyncio.wait_for(run_until_stopped(c, c.run({TOKENS[0]: "a"})), 5)
            store.close()

    asyncio.run(main())


def test_run_until_stopped_writes_the_stopped_note_on_sigterm(tmp_path):
    async def main():
        async with FakePolymarket() as fake:
            paths, store, c = make(tmp_path, fake.url)
            asyncio.get_running_loop().call_later(0.4, os.kill, os.getpid(), signal.SIGTERM)
            why = await run_until_stopped(c, c.run({TOKENS[0]: "a"}))
            store.close()
            return why, paths

    why, paths = asyncio.run(main())
    assert why == "SIGTERM"
    assert lines(paths)[-1]["kind"] == "stopped" and lines(paths)[-1]["raw"] == {"reason": "SIGTERM"}


def test_lock_refuses_a_second_copy(tmp_path):
    path = tmp_path / "logs" / "collect.lock"
    first = acquire_lock(path)
    assert path.read_text().startswith(f"pid {os.getpid()}")
    with pytest.raises(AlreadyRunning, match=f"pid {os.getpid()}"):
        acquire_lock(path)
    first.close()
    acquire_lock(path).close()  # free again once the holder is gone


# ---------- the real script, as a subprocess ----------

def start_cli(tmp_path, url, *args):
    data = tmp_path / "data"
    universe = data / "universe" / "polymarket"
    universe.mkdir(parents=True, exist_ok=True)
    (universe / "current.json").write_text(json.dumps({
        "picked_at": datetime.now(UTC).isoformat(),
        "markets": [{"question": "Q", "outcomes": [{"name": "Yes", "token_id": TOKENS[0]}, {"name": "No", "token_id": TOKENS[1]}]}]}))
    env = {**os.environ, "PM_DATA_DIR": str(data), "PM_LIVE_URL": url, "PM_BOOKS_URL": "http://127.0.0.1:9/books"}
    return subprocess.Popen([sys.executable, str(REPO / "listener.py"), *args], cwd=REPO, env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True), Paths(data)


def wait_for_messages(paths, count, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if paths.raw.exists() and sum(1 for e in lines(paths) if e["kind"] == "message") >= count:
            return True
        time.sleep(0.2)
    return False


@pytest.mark.slow
def test_cli_sigterm_writes_stopped_and_second_copy_is_refused(tmp_path):
    async def serve_forever(ready, stop):
        async with FakePolymarket(tick=0.1) as fake:
            ready.append(fake.url)
            while not stop:
                await asyncio.sleep(0.05)

    import threading
    ready, stop = [], []
    thread = threading.Thread(target=lambda: asyncio.run(serve_forever(ready, stop)), daemon=True)
    thread.start()
    while not ready:
        time.sleep(0.05)
    first, paths = start_cli(tmp_path, ready[0])
    try:
        assert wait_for_messages(paths, 3), first.stdout.read() if first.poll() is not None else "no messages"
        second, _ = start_cli(tmp_path, ready[0])
        out, _ = second.communicate(timeout=20)
        assert second.returncode == 1 and "already running" in out
        first.send_signal(signal.SIGTERM)
        out, _ = first.communicate(timeout=20)
        assert first.returncode == 0, out
        assert "Stopped (SIGTERM)" in out
    finally:
        if first.poll() is None:
            first.kill()
        stop.append(True)
    got = lines(paths)
    assert got[-1]["kind"] == "stopped" and got[-1]["raw"]["reason"] == "SIGTERM"
    assert RawFile(raw_files(paths.raw)[-1]).damage == [] or all(d.kind != "corrupt" for d in RawFile(raw_files(paths.raw)[-1]).damage)


def test_cli_without_a_market_list_or_with_bad_arguments(tmp_path, monkeypatch):
    import listener
    monkeypatch.setenv("PM_DATA_DIR", str(tmp_path / "data"))
    assert listener.main(["--nope"]) == 1
    paths = Paths(tmp_path / "data")
    paths.universe.mkdir(parents=True)
    paths.universe_file.write_text("{not json")
    assert listener.main([]) == 3
