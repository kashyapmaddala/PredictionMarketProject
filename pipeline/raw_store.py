"""Raw store: save every live message to disk, exactly as received, and never edit it.

Files are split by venue, day and hour (all UTC), one line per message:

    data/raw/polymarket/2026-11-02/18.jsonl.gz

Each line is a JSON object (the "envelope"):

    {"recv_ts_ns": 1793664000123456789, "kind": "message", "session": "a1b2c3d4e5f6", "conn": 3,
     "raw": "<the message text, untouched>"}

- recv_ts_ns: when OUR computer received it, in nanoseconds since 1970 (UTC).
- kind: "message" for exchange data, "rest_books" for a REST copy of books. Other kinds
  ("started", "connected", "subscribed", "disconnected", "retired", "paused", "stopped",
  "universe_changed", "rest_books_failed") are notes from the collector itself, so later we can
  tell a quiet market apart from a time our connection was down.
- session (format 2, added 2026-11-05): one random id per collector run.
- conn (format 2): which WebSocket connection in that run (1, 2, 3 ... never reused in a run).
  Absent on lines that do not belong to a connection (REST copies, run-wide notes).
  Lines written before 2026-11-05 have neither field; readers must accept both.
- raw: the exact text the exchange sent (or a dict, for notes). Never parsed or changed here, so a
  bug in later code can always be fixed by re-reading these files.

compress=True writes <hour>.jsonl.gz (about 7 to 15x smaller). Files are only ever appended to,
and a compressed file is never reopened: if an hour's file already exists (an earlier run was
killed mid-write), a new part is started (19.part2.jsonl.gz ...), because data appended after a
broken ending is hard to read back. Plain files are flushed after every line; compressed files at
most every FLUSH_EVERY seconds (each flush makes everything so far readable), so a crash loses at
most about a second. Call tick() regularly (the collector does, every second) so a quiet stream
still gets flushed and the file is closed soon after its hour ends.

The hour a line goes into never moves backwards, even if the computer's clock does (NTP fixes):
the line keeps its true recv_ts_ns, it just stays in the newer file.
"""
from __future__ import annotations

import gzip
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path

from .config import FLUSH_EVERY

FORMAT_VERSION = 2


def hour_key(recv_ts_ns: int) -> tuple[str, str]:
    """("YYYY-MM-DD", "HH") of a time in nanoseconds, in UTC."""
    moment = datetime.fromtimestamp(recv_ts_ns // 1_000_000_000, tz=UTC)
    return moment.strftime("%Y-%m-%d"), moment.strftime("%H")


class RawStore:
    def __init__(self, folder: Path, compress: bool = False, session: str | None = None,
                 clock=time.time_ns, monotonic=time.monotonic):
        self.folder = Path(folder)
        self.compress = compress
        self.session = session
        self.clock = clock
        self.monotonic = monotonic
        self.current_path: Path | None = None
        self.key: tuple[str, str] | None = None
        self.file = None
        self.lines_written = 0
        self.bytes_written = 0
        self.last_flush = 0.0
        self.dirty = False

    def _open(self, key: tuple[str, str]) -> None:
        day, hour = key
        folder = self.folder / day
        folder.mkdir(parents=True, exist_ok=True)
        if self.compress:
            path, part = folder / f"{hour}.jsonl.gz", 1
            while path.exists():
                part += 1
                path = folder / f"{hour}.part{part}.jsonl.gz"
            # "xb": fail rather than share a file if another writer created it just now
            self.file = gzip.GzipFile(filename=path.name, fileobj=open(path, "xb"), mode="wb", mtime=0)
        else:
            path = folder / f"{hour}.jsonl"
            self.file = open(path, "ab")
        self.current_path = path
        self.key = key

    def _close_file(self) -> None:
        if self.file is None:
            return
        f, self.file = self.file, None
        inner = f.fileobj if isinstance(f, gzip.GzipFile) else None
        try:
            f.close()  # writes the gzip ending
            if inner is not None:
                inner.flush()
                os.fsync(inner.fileno())
        finally:
            if inner is not None:
                inner.close()
        self.dirty = False

    def write(self, kind: str, raw, recv_ts_ns: int | None = None, conn: int | None = None) -> None:
        """Append one line. `raw` is the exchange's text (for messages) or a dict (for notes)."""
        if recv_ts_ns is None:
            recv_ts_ns = self.clock()
        envelope = {"recv_ts_ns": recv_ts_ns, "kind": kind}
        if self.session is not None:
            envelope["session"] = self.session
        if conn is not None:
            envelope["conn"] = conn
        envelope["raw"] = raw
        data = (json.dumps(envelope, ensure_ascii=False) + "\n").encode("utf-8")
        key = hour_key(recv_ts_ns)
        if self.key is not None and key < self.key:
            key = self.key  # the clock went backwards: stay in the newer file
        if self.file is None or key != self.key:  # a new hour started (or the file was closed)
            self._close_file()
            self._open(key)
        self.file.write(data)
        self.lines_written += 1
        self.bytes_written += len(data)
        self.dirty = True
        now = self.monotonic()
        if not self.compress or now - self.last_flush >= FLUSH_EVERY:
            self.flush()

    def flush(self) -> None:
        if self.file is not None and self.dirty:
            self.file.flush()  # for gzip: a sync flush, so everything so far can be decompressed
            if isinstance(self.file, gzip.GzipFile):
                self.file.fileobj.flush()
            self.dirty = False
        self.last_flush = self.monotonic()

    def tick(self) -> None:
        """Call about once a second: flush pending data, close the file once its hour is over."""
        if self.file is not None and self.key is not None and hour_key(self.clock()) > self.key:
            self._close_file()
            return
        if self.dirty and self.monotonic() - self.last_flush >= FLUSH_EVERY:
            self.flush()

    def close(self) -> None:
        self._close_file()
