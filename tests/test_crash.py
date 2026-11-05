"""Crash tests: kill -9 a writer mid-stream, then read back and validate."""
import asyncio
import json
import os
import signal
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path

import pytest

from pipeline.polymarket.validate import validate
from pipeline.raw_reader import RawFile, raw_files
from tests.fake_polymarket import FakePolymarket
from tests.test_collector import lines, start_cli, wait_for_messages

REPO = Path(__file__).resolve().parent.parent

WRITER = textwrap.dedent("""
    import sys, json, time
    sys.path.insert(0, {repo!r})
    from pipeline.raw_store import RawStore
    store = RawStore({folder!r}, compress=True, session="crash")
    t = 1_793_750_400_000_000_000
    store.write("started", {{"session": "crash"}}, t)
    i = 0
    while True:
        i += 1
        store.write("message", json.dumps({{"n": i, "pad": "x" * 200}}), t + i * 1000, conn=1)
        if i % 100 == 0:
            store.flush()
            print("flushed", i, flush=True)
""")


def test_kill_9_loses_nothing_that_was_flushed(tmp_path):
    folder = tmp_path / "raw"
    process = subprocess.Popen([sys.executable, "-c", WRITER.format(repo=str(REPO), folder=str(folder))],
                               stdout=subprocess.PIPE, text=True)
    flushed = 0
    for row in process.stdout:
        flushed = int(row.split()[1])
        if flushed >= 3000:
            break
    os.kill(process.pid, signal.SIGKILL)
    process.wait()
    (path,) = raw_files(folder)
    reader = RawFile(path)
    numbers = [json.loads(e["raw"])["n"] for _n, e in reader if e["kind"] == "message"]
    assert numbers[:flushed] == list(range(1, flushed + 1))  # everything flushed, in order, no gaps
    assert numbers == list(range(1, len(numbers) + 1))
    assert [d.kind for d in reader.damage] == ["truncated"]


@pytest.mark.slow
def test_kill_9_collector_then_restart_is_reported_as_a_gap(tmp_path):
    ready, stop = [], []

    async def serve():
        async with FakePolymarket(tick=0.05) as fake:
            ready.append(fake.url)
            while not stop:
                await asyncio.sleep(0.05)

    threading.Thread(target=lambda: asyncio.run(serve()), daemon=True).start()
    while not ready:
        time.sleep(0.05)
    try:
        first, paths = start_cli(tmp_path, ready[0])
        assert wait_for_messages(paths, 20)
        time.sleep(1.5)  # let at least one timed flush happen
        first.send_signal(signal.SIGKILL)
        first.wait()
        saved_before = sum(1 for e in lines(paths) if e["kind"] == "message")
        second, _ = start_cli(tmp_path, ready[0])  # the lock was released by the kill
        assert wait_for_messages(paths, saved_before + 20)
        second.send_signal(signal.SIGTERM)
        out, _ = second.communicate(timeout=20)
        assert second.returncode == 0, out
    finally:
        stop.append(True)
        for p in ("first", "second"):
            proc = locals().get(p)
            if proc is not None and proc.poll() is None:
                proc.kill()
    report = validate(paths)
    assert "collector restarted without a stop note (killed or crashed)" in report.text
    assert "truncated:" in report.text and "DAMAGED" not in report.text
    assert not report.failed, report.text
    sessions = {e.get("session") for e in lines(paths)}
    assert len(sessions) == 2
