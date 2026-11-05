"""Every path, URL and limit in one place.

Nothing here touches the disk or the network: importing this module has no side effects.
The data folder defaults to <repo>/data and can be moved with the PM_DATA_DIR environment
variable (tests point it at a temporary folder). Paths are resolved when Paths.from_env() is
called, not at import time, so a test can set the variable after importing the package.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parent.parent
DATA_DIR_ENV = "PM_DATA_DIR"

VENUE = "polymarket"

# ---------- Polymarket endpoints ----------
LIVE_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BOOKS_URL = "https://clob.polymarket.com/books"
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets/keyset"
GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"

# ---------- collector limits ----------
MAX_TOKENS_PER_CONNECTION = 400  # Polymarket allows 500 per connection; stay under it
APP_PING_EVERY = 10.0  # seconds between our "PING" text frames (Polymarket's documented keepalive)
SILENCE_LIMIT = 30.0  # seconds with no frame at all (not even PONG) = dead line
# A connection older than this is replaced by a fresh one that overlaps it (make before break),
# so an ageing connection never has to die on its own. 0 turns rotation off.
ROTATE_EVERY = 600.0
# How long an old and a new connection both run during a rotation or a standby takeover.
OVERLAP_SECONDS = 5.0
RECONNECT_MIN_WAIT = 0.0  # first retry after a drop is immediate (the drop is usually server side)
RECONNECT_MAX_WAIT = 60.0
STATUS_EVERY = 60.0  # seconds between status lines
SNAPSHOT_EVERY = 300.0  # seconds between REST book copies (for checking our rebuilt books)
SNAPSHOT_BATCH = 200  # tokens per REST request
REPICK_EVERY_HOURS = 24
REPICK_RETRY_HOURS = 1
FLUSH_EVERY = 1.0  # seconds between flushes of a compressed raw file

# ---------- market picking ----------
HOW_MANY = 100
MAX_PER_EVENT = 5
GAMMA_PAGE_SIZE = 1000

# ---------- HTTP ----------
HTTP_TIMEOUT = 30.0
HTTP_RETRIES = 6  # attempts per request on 429, 5xx and network errors
HTTP_BACKOFF_MAX = 60.0


@dataclass(frozen=True)
class Paths:
    """Every folder and file the pipeline reads or writes, under one data folder."""

    data: Path

    @classmethod
    def from_env(cls) -> Paths:
        value = os.environ.get(DATA_DIR_ENV)
        return cls(Path(value).expanduser().resolve() if value else REPO_DIR / "data")

    @property
    def raw(self) -> Path:
        return self.data / "raw" / VENUE

    @property
    def raw_markets(self) -> Path:
        return self.data / "raw" / f"{VENUE}_markets"

    @property
    def universe(self) -> Path:
        return self.data / "universe" / VENUE

    @property
    def universe_file(self) -> Path:
        return self.universe / "current.json"

    @property
    def clean(self) -> Path:
        return self.data / "clean" / VENUE

    @property
    def state(self) -> Path:
        return self.clean / "_state"

    @property
    def reports(self) -> Path:
        return self.data / "reports" / VENUE

    @property
    def logs(self) -> Path:
        return self.data / "logs"

    @property
    def lock_file(self) -> Path:
        return self.logs / "collector.lock"

    @property
    def picker_log(self) -> Path:
        return self.logs / "pick_markets.log"
