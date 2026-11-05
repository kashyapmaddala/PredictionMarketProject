"""Read raw files back, tolerating damage, in bounded memory.

The raw files are written by pipeline.raw_store. This module:

- lists them in the right order: by day, then hour, then part (19.jsonl.gz, 19.part2.jsonl.gz,
  19.part3.jsonl.gz ...), plain .jsonl files included;
- streams each file (a fixed-size chunk at a time, never the whole file in memory);
- survives damage. A .gz file can hold several compressed pieces ("members") one after another.
  If a writer was killed, its last piece just ends early (truncated): we keep everything up to
  the last complete line. If a piece is damaged in the middle (for example two writers once wrote
  into one file at the same time), we keep everything before the damage, skip forward to the next
  piece that decompresses, and carry on;
- reports every problem in RawFile.damage and RawFile.bad_lines, instead of a global list, so a
  caller can say exactly what was lost and where.

Each line is returned as a dict (the "envelope"):

    {"recv_ts_ns": int, "kind": str, "raw": str | dict, "session": str?, "conn": int?}

"session" and "conn" were added in format 2 (2026-11-05). Older lines lack them; readers must
accept both (use .get()).
"""
from __future__ import annotations

import json
import re
import zlib
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from pathlib import Path

CHUNK = 1 << 16  # compressed bytes read at a time
GZIP_MAGIC = b"\x1f\x8b\x08"  # every gzip piece starts with these bytes
PROBE = 4096  # bytes decompressed to check that a candidate piece start is real
OUT_MAX = 1 << 20  # most decompressed bytes produced per step

_NAME = re.compile(r"^(\d{2})(?:\.part(\d+))?\.jsonl(\.gz)?$")


def parse_name(name: str) -> tuple[int, int] | None:
    """'19.part2.jsonl.gz' -> (19, 2); '07.jsonl' -> (7, 1); anything else -> None."""
    match = _NAME.match(name)
    if not match or int(match.group(1)) > 23:
        return None
    return int(match.group(1)), int(match.group(2) or 1)


def file_order(path: Path) -> tuple[int, int, int]:
    """Sort key inside one day folder: hour, then part, then plain before compressed."""
    hour, part = parse_name(path.name) or (99, 99)
    return hour, part, 1 if path.name.endswith(".gz") else 0


def day_folders(root: Path) -> list[Path]:
    """Day folders (YYYY-MM-DD) in date order. Other folders are ignored."""
    if not root.exists():
        return []
    return sorted(p for p in root.iterdir() if p.is_dir() and re.fullmatch(r"\d{4}-\d{2}-\d{2}", p.name))


def raw_files(root: Path, day: str | None = None) -> list[Path]:
    """Every raw file under root (or just one day), in the order they were written."""
    folders = [root / day] if day else day_folders(root)
    files: list[Path] = []
    for folder in folders:
        if folder.is_dir():
            files += sorted((p for p in folder.iterdir() if parse_name(p.name)), key=file_order)
    return files


def raw_hours(root: Path) -> dict[tuple[str, str], list[Path]]:
    """{(day, "HH"): [files of that hour in order]} for every hour with raw data."""
    hours: dict[tuple[str, str], list[Path]] = {}
    for path in raw_files(root):
        hour = parse_name(path.name)[0]  # type: ignore[index]
        hours.setdefault((path.parent.name, f"{hour:02d}"), []).append(path)
    return dict(sorted(hours.items()))


@dataclass
class Damage:
    """One stretch of a file we could not read."""

    file: str
    offset: int  # compressed byte offset where the bad stretch starts
    skipped_bytes: int  # compressed bytes skipped (0 for a truncated ending)
    kind: str  # "corrupt" (damaged in the middle) or "truncated" (file ends mid-piece)

    def describe(self) -> str:
        if self.kind == "truncated":
            return f"{self.file}: ends mid-write at byte {self.offset:,} (writer killed or still writing)"
        return f"{self.file}: {self.skipped_bytes:,} damaged compressed bytes skipped at byte {self.offset:,}"


@dataclass
class RawFile:
    """Iterate over one raw file's envelopes. After iterating, damage and bad_lines say what was lost."""

    path: Path
    damage: list[Damage] = field(default_factory=list)
    bad_lines: int = 0  # lines that were not valid envelopes (torn by a crash, or garbage)
    bad_examples: list[str] = field(default_factory=list)
    lines: int = 0  # every non-empty line seen, good or bad (source_line numbers count these)

    def __iter__(self) -> Iterator[tuple[int, dict]]:
        """Yields (line number starting at 1, envelope)."""
        self.damage, self.bad_lines, self.bad_examples, self.lines = [], 0, [], 0
        texts = self._gz_lines() if self.path.name.endswith(".gz") else self._plain_lines()
        for text in texts:
            if not text.strip():
                continue
            self.lines += 1
            envelope = parse_envelope(text)
            if envelope is None:
                self.bad_lines += 1
                if len(self.bad_examples) < 3:
                    self.bad_examples.append(text.rstrip(b"\r\n")[:200].decode("utf-8", "replace"))
                continue
            yield self.lines, envelope

    def _plain_lines(self) -> Iterator[bytes]:
        with open(self.path, "rb") as f:
            yield from f

    def _gz_lines(self) -> Iterator[bytes]:
        """Decompress piece after piece. A line never continues across pieces: each writer run
        starts a new piece on a fresh line, so a piece's unfinished last line is torn and dropped."""
        name = f"{self.path.parent.name}/{self.path.name}"
        with open(self.path, "rb") as f:
            s = _Stream(f)
            while True:
                s.fill(len(GZIP_MAGIC))
                if not s.buf:
                    return
                if not s.buf.startswith(GZIP_MAGIC):
                    # Not a piece start (garbage after damage): jump to the next real one
                    start = s.offset
                    s.seek(s.find_piece(start + 1))
                    self.damage.append(Damage(name, start, s.offset - start, "corrupt"))
                    continue
                unzip = zlib.decompressobj(31)
                piece_start = s.offset
                s.tail = b""
                pending = b""  # text of the current unfinished line
                broken = False
                while True:
                    if not s.buf and not s.fill(1):
                        break  # file ended mid-piece
                    data = s.buf
                    try:
                        out = unzip.decompress(data, OUT_MAX)  # bounded output, even for a "zip bomb"
                    except zlib.error:
                        broken = True
                        break
                    s.consume(len(data) - len(unzip.unconsumed_tail) - len(unzip.unused_data))
                    lines = (pending + out).split(b"\n")
                    pending = lines.pop()
                    yield from lines
                    if unzip.eof:
                        break
                if broken:
                    if pending:
                        self.bad_lines += 1  # the line cut by the damage
                    s.seek(s.find_piece(piece_start + 1))
                    self.damage.append(Damage(name, piece_start, s.offset - piece_start, "corrupt"))
                    continue
                if not unzip.eof:
                    # A killed writer, or a file still being written: keep every complete line
                    lines = (pending + unzip.flush()).split(b"\n")
                    pending = lines.pop()
                    yield from lines
                    if pending:
                        self.bad_lines += 1  # the half-written last line
                    self.damage.append(Damage(name, s.offset, 0, "truncated"))
                    return
                if pending:
                    yield pending  # a finished piece whose last line had no newline: try it


class _Stream:
    """A file read a chunk at a time. buf holds unread bytes starting at file offset `offset`;
    tail keeps up to one chunk of already-read bytes of the current piece, so a damaged piece can
    be searched for the next piece start a little behind the point where zlib noticed the damage."""

    def __init__(self, f):
        self.f = f
        self.buf = b""
        self.tail = b""
        self.offset = 0
        self.eof = False

    def fill(self, need: int) -> bool:
        while len(self.buf) < need and not self.eof:
            chunk = self.f.read(CHUNK)
            if chunk:
                self.buf += chunk
            else:
                self.eof = True
        return len(self.buf) >= need

    def consume(self, n: int) -> None:
        self.tail = (self.tail + self.buf[:n])[-CHUNK:]
        self.buf = self.buf[n:]
        self.offset += n

    def seek(self, position: int) -> None:
        """Move to an absolute offset inside tail + buf (or the end of the file)."""
        region_start = self.offset - len(self.tail)
        region = self.tail + self.buf
        self.buf = region[max(0, position - region_start):]
        self.offset = position if position >= region_start else region_start
        self.tail = b""

    def find_piece(self, position: int) -> int:
        """Absolute offset of the next real piece start at or after `position`, or the end of the
        file. A candidate counts as real if its first PROBE bytes decompress without error.
        Memory stays bounded: bytes before the search point are dropped as the search moves on."""
        while True:
            region_start = self.offset - len(self.tail)
            region = self.tail + self.buf
            i = region.find(GZIP_MAGIC, max(0, position - region_start))
            if i == -1:
                if self.eof:
                    return region_start + len(region)
                # keep the last two bytes: a piece start may straddle the chunk boundary
                position = max(position, region_start + len(region) - len(GZIP_MAGIC) + 1)
                self._drop_before(position)
                self.fill(len(self.buf) + CHUNK)
                continue
            while len(region) - i < PROBE and not self.eof:
                self.fill(len(self.buf) + CHUNK)
                region_start = self.offset - len(self.tail)
                region = self.tail + self.buf
                i = region.find(GZIP_MAGIC, max(0, position - region_start))
            try:
                zlib.decompressobj(31).decompress(region[i:i + PROBE])
                return region_start + i
            except zlib.error:
                position = region_start + i + 1  # just bytes that looked like a piece start

    def _drop_before(self, position: int) -> None:
        region_start = self.offset - len(self.tail)
        if position >= self.offset:
            self.buf = self.buf[position - self.offset:]
            self.offset = position
            self.tail = b""
        elif position > region_start:
            self.tail = self.tail[position - region_start:]


def parse_envelope(text: bytes | str) -> dict | None:
    """One saved line -> envelope dict, or None if it is not a valid envelope."""
    try:
        line = json.loads(text)
    except ValueError:
        return None
    if not isinstance(line, dict):
        return None
    ts, kind = line.get("recv_ts_ns"), line.get("kind")
    if type(ts) is not int or ts < 0 or not isinstance(kind, str) or "raw" not in line:
        return None
    return line


def read_envelopes(files: Iterable[Path]) -> Iterator[tuple[Path, int, dict]]:
    """(file, line number, envelope) for every good line in these files, in order."""
    for path in files:
        for number, envelope in RawFile(path):
            yield path, number, envelope
