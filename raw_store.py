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

compress=True writes <hour>.jsonl.gz instead (about 7 to 15x smaller; read it with
gzip.open). The live Polymarket feed needs this: about 2.3 GB an hour uncompressed
for 100 markets.

Files are only ever appended to. Plain files are flushed after every line. Compressed
files are flushed at most once a second (flushing every line would ruin the
compression), so a crash loses at most about one second of data.
"""
import gzip
import json
import time
from datetime import datetime, timezone
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data" / "raw"


class RawStore:
    def __init__(self, venue, compress=False):
        self.folder = DATA_DIR / venue
        self.compress = compress
        self.current_path = None
        self.hour_key = None
        self.file = None
        self.lines_written = 0
        self.last_flush = 0.0

    def _file_for(self, recv_ts_ns):
        moment = datetime.fromtimestamp(recv_ts_ns / 1e9, tz=timezone.utc)
        folder = self.folder / moment.strftime("%Y-%m-%d")
        name = f"{moment:%H}.jsonl.gz" if self.compress else f"{moment:%H}.jsonl"
        if self.hour_key != (folder, name):  # a new hour started: move to a new file
            self.hour_key = (folder, name)
            path = folder / name
            # Never add onto an existing compressed file: if an earlier run was killed mid-write,
            # its file has a broken ending, and data added after it is hard to read back.
            # Start a new part instead: 19.jsonl.gz, 19.part2.jsonl.gz, 19.part3.jsonl.gz ...
            part = 1
            while self.compress and path.exists():
                part += 1
                path = folder / f"{moment:%H}.part{part}.jsonl.gz"
            if self.file:
                self.file.close()
            path.parent.mkdir(parents=True, exist_ok=True)
            if self.compress:
                self.file = gzip.open(path, "wt", encoding="utf-8")  # always a brand-new file (see above)
            else:
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
        now = time.monotonic()
        if not self.compress or now - self.last_flush >= 1.0:
            f.flush()
            self.last_flush = now
        self.lines_written += 1

    def close(self):
        if self.file:
            self.file.close()
            self.file = None
