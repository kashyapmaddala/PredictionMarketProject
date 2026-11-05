"""The live collector: watch every picked market, save every message, never quit on its own.

What it does (see collect_live.py for how to run it):
- Splits the tokens into shards of at most MAX_TOKENS_PER_CONNECTION (Polymarket allows 500 per
  connection) and keeps one WebSocket connection per shard (two with standby=True).
- Saves every frame exactly as received (raw_store), tagged with this run's session id and the
  connection number, plus notes for every connect, subscribe, drop, pause and stop.
- Keepalive: a "PING" text frame every APP_PING_EVERY seconds (Polymarket's documented heartbeat;
  it answers "PONG") plus the WebSocket library's own ping. SILENCE_LIMIT seconds with no frame at
  all (not even PONG) means the line is dead.
- Reconnects at once after a drop (the first retry has no wait), then backs off 1, 2, 4 ... 60 s
  with some randomness if the network stays down. Every new connection asks for a fresh full
  book of every token (initial_dump).
- Detects when the computer slept (the wall clock jumped ahead of the monotonic clock) or the
  program was frozen, writes a "paused" note with how long, and reconnects every connection
  right away: after a sleep the server has already hung up, and waiting to find that out only
  extends the blind time. (Root cause of the 2026-11-03/05 drops: the laptop slept with the lid
  closed; see tests/test_collector.py and the report.)
- Optional hot standby (standby=True): two connections per shard, both saved. The table builder
  records one and keeps the other's books ready, so a drop on one loses nothing. Costs double the
  disk and bandwidth.
- Every SNAPSHOT_EVERY seconds it fetches every book over REST and saves the reply (kind
  "rest_books"), so the validator can compare our rebuilt books with Polymarket's.
- Every REPICK_EVERY_HOURS it re-picks the markets in a subprocess and switches over
  make-before-break: the new connections are opened and have their books before the old ones are
  retired, so tokens in both lists are never blind.
- A full disk (or any other error saving data) stops the collector with an error instead of
  pretending to collect. SIGTERM and Ctrl+C stop it cleanly with a "stopped" note.
"""
from __future__ import annotations

import asyncio
import base64
import contextlib
import fcntl
import json
import logging
import os
import random
import signal
import socket
import ssl
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import certifi
import httpx
import websockets

from .. import config
from ..http import GiveUp, arequest
from .universe import load_universe

log = logging.getLogger("collector")


@dataclass
class Settings:
    live_url: str = config.LIVE_URL
    books_url: str = config.BOOKS_URL
    max_tokens: int = config.MAX_TOKENS_PER_CONNECTION
    app_ping_every: float = config.APP_PING_EVERY
    silence_limit: float = config.SILENCE_LIMIT
    reconnect_max_wait: float = config.RECONNECT_MAX_WAIT
    healthy_after: float = 60.0  # a connection up this long resets the backoff
    status_every: float = config.STATUS_EVERY
    snapshot_every: float = config.SNAPSHOT_EVERY
    snapshot_batch: int = config.SNAPSHOT_BATCH
    repick_every_hours: float = config.REPICK_EVERY_HOURS
    repick_retry_hours: float = config.REPICK_RETRY_HOURS
    repick_timeout: float = 30 * 60.0
    overlap_seconds: float = config.OVERLAP_SECONDS
    switch_timeout: float = 60.0  # longest wait for new connections during a list switch
    pause_limit: float = 5.0  # seconds of clock jump or freeze that count as a pause
    housekeeping_every: float = 1.0
    standby: bool = False
    verbose: bool = False
    picker: list[str] = field(default_factory=lambda: [sys.executable, str(config.REPO_DIR / "pick_markets.py")])
    tls: bool = True

    @classmethod
    def from_env(cls, **overrides) -> Settings:
        """Defaults, with PM_LIVE_URL / PM_BOOKS_URL from the environment if set."""
        settings = cls(**overrides)
        settings.live_url = os.environ.get("PM_LIVE_URL", settings.live_url)
        settings.books_url = os.environ.get("PM_BOOKS_URL", settings.books_url)
        settings.tls = settings.live_url.startswith("wss://")
        return settings


class StorageFailed(RuntimeError):
    """Saving data failed (disk full, permissions...). Collecting without saving is pointless."""


class AlreadyRunning(RuntimeError):
    pass


def new_session() -> str:
    return uuid.uuid4().hex[:12]


def split_into_shards(tokens: list[str], size: int) -> list[list[str]]:
    if size < 1:
        raise ValueError("shard size must be at least 1")
    return [tokens[i:i + size] for i in range(0, len(tokens), size)]


def reconnect_wait(attempt: int, max_wait: float, rng=random.random) -> float:
    """Seconds to wait before reconnect attempt number `attempt` (0 = first retry: no wait)."""
    if attempt <= 0:
        return 0.0
    return min(2.0 ** (attempt - 1), max_wait) * (0.75 + 0.5 * rng())


def describe(event: dict, names: dict) -> str | None:
    """One event in plain English (for --verbose)."""
    kind = event.get("event_type")
    if kind == "book":
        name = names.get(event.get("asset_id"), "?")
        bids, asks = event.get("bids") or [], event.get("asks") or []
        best_bid = bids[-1].get("price") if bids else "none"
        best_ask = asks[-1].get("price") if asks else "none"
        return f"FULL BOARD  {name}: best buy {best_bid}, best sell {best_ask}"
    if kind == "price_change":
        lines = []
        for change in event.get("price_changes") or []:
            name = names.get(change.get("asset_id"), "?")
            who = "buyers" if change.get("side") == "BUY" else "sellers"
            lines.append(f"UPDATE      {name}: {who} at {change.get('price')} now total {change.get('size')}")
        return "\n".join(lines) or None
    if kind == "last_trade_price":
        name = names.get(event.get("asset_id"), "?")
        return f"TRADE       {name}: {event.get('size')} traded at {event.get('price')}"
    return None


def describe_message(message: str, names: dict) -> list[str]:
    try:
        data = json.loads(message)
    except ValueError:
        return []
    out = []
    for event in data if isinstance(data, list) else [data]:
        text = describe(event, names) if isinstance(event, dict) else None
        if text:
            out.append(text)
    return out


class Generation:
    """The connections for one market list."""

    def __init__(self, names: dict[str, str]):
        self.names = names
        self.tasks: list[asyncio.Task] = []
        self.ready: list[asyncio.Event] = []  # one per shard copy: set when its books arrived
        self.conns: set[int] = set()


class Collector:
    def __init__(self, store, settings: Settings | None = None, connect=None, http_client=None,
                 wall=time.time, monotonic=time.monotonic, paths: config.Paths | None = None):
        self.store = store
        self.settings = settings or Settings()
        self.connect = connect or websockets.connect
        self.http_client = http_client  # an httpx.AsyncClient (made in run() if None)
        self.wall = wall
        self.monotonic = monotonic
        self.paths = paths or config.Paths.from_env()
        self.session = store.session or new_session()
        self.ssl = ssl.create_default_context(cafile=certifi.where()) if self.settings.tls else None
        self.next_conn = 0
        self.sockets: dict[int, object] = {}  # conn -> live websocket
        self.kick_reason: dict[int, str] = {}  # conn -> why we closed it on purpose
        self.retire_reason: dict[int, str] = {}
        self.current: Generation | None = None
        self.saved = 0
        self.pongs = 0
        self.drops = 0
        self.pauses = 0
        self.snapshots_taken = 0
        self.shards_total = 0
        self.failed: asyncio.Future | None = None  # set if a connection task fails for real

    # ---------- saving ----------

    def save(self, kind: str, raw, recv: int | None = None, conn: int | None = None) -> None:
        try:
            self.store.write(kind, raw, recv, conn=conn)
        except OSError as problem:
            raise StorageFailed(f"could not save data: {problem}") from problem

    @property
    def tokens(self) -> list[str]:
        return list(self.current.names) if self.current else []

    # ---------- one connection ----------

    async def connection(self, conn: int, shard: int, tokens: list[str], ready: asyncio.Event) -> float:
        """Run one connection until it ends. Returns how many seconds it was up (0 if it never
        opened). Writes its own connected/subscribed/disconnected/retired notes."""
        s = self.settings
        opened = None
        reason = None
        retired = False
        try:
            async with self.connect(s.live_url, ssl=self.ssl, max_size=None, open_timeout=15,
                                    ping_interval=20, ping_timeout=20, close_timeout=2) as ws:
                opened = self.monotonic()
                self.sockets[conn] = ws
                self.save("connected", {"shard": shard, "url": s.live_url}, conn=conn)
                request = {"type": "market", "assets_ids": tokens, "initial_dump": True}
                await ws.send(json.dumps(request))
                self.save("subscribed", {"shard": shard, "request": request}, conn=conn)
                pinger = asyncio.create_task(self.ping(ws))
                try:
                    while True:
                        message = await asyncio.wait_for(ws.recv(), s.silence_limit)
                        received = time.time_ns()  # arrival time first, before anything else
                        if message == "PONG":
                            self.pongs += 1
                            continue
                        if isinstance(message, bytes):
                            self.save("message_binary", base64.b64encode(message).decode(), received, conn)
                            continue
                        self.save("message", message, received, conn)
                        self.saved += 1
                        if not ready.is_set():
                            ready.set()  # the initial dump arrived
                        if s.verbose:
                            for line in describe_message(message, self.current.names if self.current else {}):
                                print(line, flush=True)
                finally:
                    pinger.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await pinger
        except asyncio.CancelledError:
            retired = True
            raise
        except StorageFailed:
            raise
        except TimeoutError:
            reason = f"no data for {s.silence_limit:g} seconds" if opened is not None else "timed out while connecting"
        except websockets.ConnectionClosed as problem:
            reason = self.kick_reason.get(conn) or str(problem) or type(problem).__name__
        except (websockets.WebSocketException, OSError) as problem:
            reason = str(problem) or type(problem).__name__
        finally:
            self.sockets.pop(conn, None)
            up = 0.0 if opened is None else self.monotonic() - opened
            if retired:
                why = self.retire_reason.pop(conn, None)
                if why:
                    with contextlib.suppress(StorageFailed):
                        self.save("retired", {"shard": shard, "reason": why}, conn=conn)
            elif reason is not None:
                self.kick_reason.pop(conn, None)
                self.drops += 1
                self.save("disconnected", {"shard": shard, "reason": reason, "seconds_up": round(up, 3)}, conn=conn)
                log.warning("connection %d (shard %d) dropped after %.0fs: %s", conn, shard, up, reason)
        return up

    async def ping(self, ws) -> None:
        while True:
            await asyncio.sleep(self.settings.app_ping_every)
            await ws.send("PING")

    async def shard(self, shard: int, tokens: list[str], ready: asyncio.Event, generation: Generation) -> None:
        """Keep one connection alive forever for this group of tokens."""
        attempt = 0
        while True:
            self.next_conn += 1
            conn = self.next_conn
            generation.conns.add(conn)
            try:
                up = await self.connection(conn, shard, tokens, ready)
            except asyncio.CancelledError:
                generation.conns.discard(conn)
                raise
            generation.conns.discard(conn)
            if up >= self.settings.healthy_after:
                attempt = 0
            wait = reconnect_wait(attempt, self.settings.reconnect_max_wait)
            attempt += 1
            if wait:
                log.info("reconnecting shard %d in %.1fs", shard, wait)
                await asyncio.sleep(wait)

    def start_generation(self, names: dict[str, str]) -> Generation:
        generation = Generation(names)
        shards = split_into_shards(list(names), self.settings.max_tokens)
        copies = 2 if self.settings.standby else 1
        for number, tokens in enumerate(shards, start=1):
            for _ in range(copies):
                ready = asyncio.Event()
                generation.ready.append(ready)
                task = asyncio.create_task(self.shard(number, tokens, ready, generation))
                task.add_done_callback(self._shard_done)
                generation.tasks.append(task)
        self.shards_total = len(shards) * copies
        log.info("watching %d tokens over %d connection(s)%s", len(names), len(shards) * copies,
                 " (with hot standby)" if copies == 2 else "")
        return generation

    def _shard_done(self, task: asyncio.Task) -> None:
        if task.cancelled() or self.failed is None or self.failed.done():
            return
        problem = task.exception()
        if problem is not None:
            self.failed.set_exception(problem)

    async def retire(self, generation: Generation, reason: str) -> None:
        for conn in list(generation.conns):
            self.retire_reason[conn] = reason
        for task in generation.tasks:
            task.cancel()
        await asyncio.gather(*generation.tasks, return_exceptions=True)

    async def kick_all(self, reason: str) -> None:
        """Close every live connection now (its shard reconnects at once)."""
        for conn, ws in list(self.sockets.items()):
            self.kick_reason[conn] = reason
            with contextlib.suppress(Exception):
                await ws.close(code=1001, reason="reconnecting")

    # ---------- background jobs ----------

    async def housekeeping(self) -> None:
        """Every second: flush the raw file, close it when its hour ends, and notice pauses."""
        s = self.settings
        last_wall, last_mono = self.wall(), self.monotonic()
        while True:
            await asyncio.sleep(s.housekeeping_every)
            try:
                self.store.tick()
            except OSError as problem:
                raise StorageFailed(f"could not save data: {problem}") from problem
            wall, mono = self.wall(), self.monotonic()
            jumped = (wall - last_wall) - (mono - last_mono)  # time that passed without us (sleep)
            frozen = (mono - last_mono) - s.housekeeping_every  # time we were stuck
            last_wall, last_mono = wall, mono
            if jumped > s.pause_limit or frozen > s.pause_limit:
                seconds = max(jumped, 0) + max(frozen, 0)
                why = "computer slept or clock jumped" if jumped > s.pause_limit else "collector was frozen"
                self.pauses += 1
                self.save("paused", {"seconds": round(seconds, 3), "why": why})
                log.warning("paused for %.1fs (%s): reconnecting every connection now", seconds, why)
                await self.kick_all(f"reconnect after pause ({why}, {seconds:.0f}s)")

    async def snapshots(self) -> None:
        """Every snapshot_every seconds, fetch every book over REST and save the reply as received."""
        s = self.settings
        client = self.http_client
        own = client is None
        if own:
            client = httpx.AsyncClient(timeout=config.HTTP_TIMEOUT)
        try:
            while True:
                await asyncio.sleep(s.snapshot_every)
                tokens = self.tokens
                for i in range(0, len(tokens), s.snapshot_batch):
                    batch = tokens[i:i + s.snapshot_batch]
                    try:
                        reply = await arequest(client, "POST", s.books_url, retries=3,
                                               json=[{"token_id": t} for t in batch])
                        received = time.time_ns()
                        self.save("rest_books", reply.text, received)
                        self.snapshots_taken += 1
                    except GiveUp as problem:
                        self.save("rest_books_failed", {"reason": str(problem), "tokens": len(batch)})
                        log.warning("REST book copy failed: %s", problem)
        finally:
            if own:
                await client.aclose()

    async def status(self) -> None:
        last = self.saved
        while True:
            await asyncio.sleep(self.settings.status_every)
            log.info("%d/%d connections up | %s messages saved (%s in the last minute) | drops so far: %d | "
                     "book snapshots: %d | pauses: %d", len(self.sockets), self.shards_total, f"{self.saved:,}",
                     f"{self.saved - last:,}", self.drops, self.snapshots_taken, self.pauses)
            last = self.saved

    # ---------- market list ----------

    async def repick(self) -> bool:
        """Run the market picker in a subprocess. True if it succeeded."""
        s = self.settings
        log_path = self.paths.picker_log
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(log_path, "a") as out:
            try:
                picker = await asyncio.create_subprocess_exec(
                    *s.picker, stdout=out, stderr=subprocess.STDOUT, cwd=str(config.REPO_DIR),
                    env={**os.environ, config.DATA_DIR_ENV: str(self.paths.data)})
            except OSError as problem:
                log.error("could not start the market picker: %s", problem)
                return False
            try:
                code = await asyncio.wait_for(picker.wait(), s.repick_timeout)
            except TimeoutError:
                picker.kill()
                await picker.wait()
                log.error("market picker took more than %.0fs and was stopped", s.repick_timeout)
                return False
        if code != 0:
            log.error("market picker failed (exit code %s, see %s)", code, log_path)
        return code == 0

    async def next_universe(self, picked_at: datetime):
        """Wait until the list is due, re-pick, and return the new list (retrying hourly on failure)."""
        s = self.settings
        due = picked_at + timedelta(hours=s.repick_every_hours)
        while True:
            await asyncio.sleep(max(0.0, (due - datetime.now(UTC)).total_seconds()))
            log.info("market list is due for a re-pick; picking in the background (still saving meanwhile)")
            if await self.repick():
                try:
                    return load_universe(self.paths.universe_file)
                except ValueError as problem:
                    log.error("new market list unreadable: %s", problem)
            log.error("keeping the old market list; trying again in %.1f hour(s)", s.repick_retry_hours)
            due = datetime.now(UTC) + timedelta(hours=s.repick_retry_hours)

    async def switch(self, names: dict[str, str]) -> None:
        """Make before break: open the new connections, wait for their books, then retire the old."""
        old = self.current
        new = self.start_generation(names)
        self.current = new
        if old is None:
            return
        waits = [asyncio.create_task(event.wait()) for event in new.ready]
        _done, pending = await asyncio.wait(waits, timeout=self.settings.switch_timeout)
        for task in pending:
            task.cancel()
        await asyncio.sleep(self.settings.overlap_seconds)
        await self.retire(old, "market list changed")

    # ---------- running ----------

    async def run(self, names: dict[str, str], picked_at: datetime | None = None) -> None:
        """Watch these tokens until cancelled (and follow re-picks if picked_at is given)."""
        self.failed = asyncio.get_running_loop().create_future()
        self.save("started", {"session": self.session, "pid": os.getpid(), "host": socket.gethostname(),
                              "format": 2, "tokens": len(names), "standby": self.settings.standby})
        jobs = [asyncio.create_task(self.housekeeping()), asyncio.create_task(self.status()),
                asyncio.create_task(self.snapshots())]
        try:
            await self.switch(names)
            if picked_at is not None:
                jobs.append(asyncio.create_task(self.follow_universe(names, picked_at)))
            await asyncio.gather(*jobs, self.failed)
        finally:
            tasks = jobs + (self.current.tasks if self.current else [])
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if not self.failed.done():
                self.failed.cancel()

    async def follow_universe(self, names: dict[str, str], picked_at: datetime) -> None:
        while True:
            log.info("using the market list picked at %s", f"{picked_at:%Y-%m-%d %H:%M} UTC")
            new_names, new_picked_at = await self.next_universe(picked_at)
            added, removed = set(new_names) - set(names), set(names) - set(new_names)
            self.save("universe_changed", {"picked_at": new_picked_at.isoformat(),
                                           "tokens_added": sorted(added), "tokens_removed": sorted(removed)})
            log.info("new list: %d tokens added, %d removed; switching over (make before break)", len(added), len(removed))
            await self.switch(new_names)
            names, picked_at = new_names, new_picked_at


# ---------- one copy only ----------

def acquire_lock(path: Path):
    """Refuse to start if another collector holds the lock: two copies would save everything twice
    and once broke a file by writing into it at the same time. The lock is released automatically
    when the process ends, even if it is killed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(path, "a+")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.seek(0)
        holder = handle.read().strip()
        handle.close()
        raise AlreadyRunning(f"another collector is already running (lock {path}, {holder or 'pid unknown'})") from None
    handle.seek(0)
    handle.truncate()
    handle.write(f"pid {os.getpid()}\n")
    handle.flush()
    return handle


async def run_until_stopped(collector: Collector, job) -> str:
    """Run `job` until it ends or SIGTERM/SIGINT arrives. Writes the "stopped" note. Returns why."""
    loop = asyncio.get_running_loop()
    task = asyncio.ensure_future(job)
    reason = {"why": None}

    def stop(name: str) -> None:
        if reason["why"] is None:
            reason["why"] = name
            task.cancel()

    for sig, name in ((signal.SIGTERM, "SIGTERM"), (signal.SIGINT, "Ctrl+C")):
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(sig, stop, name)
    try:
        await task
    except asyncio.CancelledError:
        if reason["why"] is None:
            raise
    except StorageFailed as problem:
        reason["why"] = f"storage failed: {problem}"
        log.critical("%s", problem)
        with contextlib.suppress(StorageFailed):
            collector.save("stopped", {"reason": reason["why"]})
        raise
    finally:
        for sig in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError, RuntimeError):
                loop.remove_signal_handler(sig)
    if reason["why"] is None:
        reason["why"] = "finished"
    collector.save("stopped", {"reason": reason["why"]})
    return reason["why"]
