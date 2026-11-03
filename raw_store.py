"""Raw store: save every live message to disk, exactly as received, and never edit it.

Files are split by venue, day and hour (all UTC), one line per message:

    data/raw/polymarket/2026-11-02/18.jsonl

Each line is a JSON object:

    {"recv_ts_ns": 1793664000123456789, "kind": "message", "raw": "<the message text, untouched>"}

- recv_ts_ns: when OUR computer received it, in nanoseconds since 1970 (UTC).
- kind: "message" for exchange data. Other kinds ("connected", "subscribed",
  "disconnected") are notes from the collector itself, so later we can tell
  a quiet market apart from a time our connection was down.
- raw: the exact text the exchange sent. Never parsed or changed here, so a
  bug in later code can always be fixed by re-reading these files.

Files are only ever appended to. Each line is flushed straight to disk so a crash
loses at most the line being written.
"""
import json
import time
from datetime import datetime, timezone
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data" / "raw"


class RawStore:
    def __init__(self, venue):
        self.folder = DATA_DIR / venue
        self.current_path = None
        self.file = None
        self.lines_written = 0

    def _file_for(self, recv_ts_ns):
        moment = datetime.fromtimestamp(recv_ts_ns / 1e9, tz=timezone.utc)
        path = self.folder / moment.strftime("%Y-%m-%d") / f"{moment:%H}.jsonl"
        if path != self.current_path:  # a new hour started: move to a new file
            if self.file:
                self.file.close()
            path.parent.mkdir(parents=True, exist_ok=True)
            self.file = open(path, "a", encoding="utf-8")
            self.current_path = path
        return self.file

    def write(self, kind, raw, recv_ts_ns=None):
        """Append one line. `raw` is the exchange's text (for messages) or a dict (for notes)."""
        if recv_ts_ns is None:
            recv_ts_ns = time.time_ns()
        line = json.dumps({"recv_ts_ns": recv_ts_ns, "kind": kind, "raw": raw}, ensure_ascii=False)
        f = self._file_for(recv_ts_ns)
        f.write(line + "\n")
        f.flush()
        self.lines_written += 1

    def close(self):
        if self.file:
            self.file.close()
            self.file = None
