"""The raw writer: envelope format, hourly files, part files, flushing, clock edge cases."""
import gzip
import json
import time

import pytest

from pipeline.raw_reader import RawFile, raw_files
from pipeline.raw_store import RawStore, hour_key

H = 3_600_000_000_000
T = 1_793_750_400_000_000_000  # 2026-11-04 00:00:00 UTC exactly


def lines_of(path):
    return [e for _n, e in RawFile(path)]


def test_hour_key_is_utc_and_handles_boundaries():
    assert hour_key(T) == ("2026-11-04", "00")
    assert hour_key(T - 1) == ("2026-11-03", "23")
    assert hour_key(T + H - 1) == ("2026-11-04", "00")
    assert hour_key(T + H) == ("2026-11-04", "01")
    # leap day and year end
    assert hour_key(1_835_395_200_000_000_000) == ("2028-02-29", "00")


def test_envelope_format_with_session_and_conn(tmp_path):
    store = RawStore(tmp_path, compress=True, session="abc")
    store.write("message", '{"x": 1}', T + 5, conn=3)
    store.write("connected", {"shard": 1}, T + 6, conn=3)
    store.write("rest_books", "[]", T + 7)
    store.close()
    path = tmp_path / "2026-11-04" / "00.jsonl.gz"
    raw = gzip.decompress(path.read_bytes()).decode().splitlines()
    assert json.loads(raw[0]) == {"recv_ts_ns": T + 5, "kind": "message", "session": "abc", "conn": 3, "raw": '{"x": 1}'}
    assert list(json.loads(raw[0])) == ["recv_ts_ns", "kind", "session", "conn", "raw"]
    assert "conn" not in json.loads(raw[2])
    assert store.lines_written == 3


def test_old_style_envelope_without_session(tmp_path):
    store = RawStore(tmp_path, compress=False)
    store.write("message", "text", T)
    store.close()
    assert json.loads((tmp_path / "2026-11-04" / "00.jsonl").read_text()) == {"recv_ts_ns": T, "kind": "message", "raw": "text"}


def test_new_file_every_utc_hour_and_day(tmp_path):
    store = RawStore(tmp_path, compress=True)
    for t in (T - 1, T, T + H - 1, T + H):
        store.write("message", str(t), t)
    store.close()
    names = [f"{p.parent.name}/{p.name}" for p in raw_files(tmp_path)]
    assert names == ["2026-11-03/23.jsonl.gz", "2026-11-04/00.jsonl.gz", "2026-11-04/01.jsonl.gz"]
    assert [e["raw"] for e in lines_of(tmp_path / "2026-11-04" / "00.jsonl.gz")] == [str(T), str(T + H - 1)]


def test_never_reopens_a_compressed_file_starts_a_part_instead(tmp_path):
    for _ in range(3):
        store = RawStore(tmp_path, compress=True)
        store.write("message", "x", T)
        store.close()
    names = [p.name for p in raw_files(tmp_path)]
    assert names == ["00.jsonl.gz", "00.part2.jsonl.gz", "00.part3.jsonl.gz"]


def test_clock_going_backwards_never_moves_to_an_older_file(tmp_path):
    store = RawStore(tmp_path, compress=True)
    store.write("message", "a", T + H + 5)  # 01:00
    store.write("message", "b", T + H - 5)  # clock stepped back into 00:59
    store.close()
    files = raw_files(tmp_path)
    assert [p.name for p in files] == ["01.jsonl.gz"]
    assert [(e["raw"], e["recv_ts_ns"]) for e in lines_of(files[0])] == [("a", T + H + 5), ("b", T + H - 5)]


def test_flushed_data_is_readable_without_closing(tmp_path):
    """A crash after a flush loses nothing that was flushed: the reader sees a truncated ending."""
    store = RawStore(tmp_path, compress=True)
    for i in range(100):
        store.write("message", f"m{i}", T + i)
    store.flush()
    reader = RawFile(store.current_path)
    assert [e["raw"] for e in lines_of(store.current_path)][-1] == "m99"
    list(reader)
    assert [d.kind for d in reader.damage] == ["truncated"]
    store.close()
    reader = RawFile(store.current_path)
    assert len(list(reader)) == 100 and reader.damage == []


def test_compressed_writes_flush_at_most_once_a_second(tmp_path):
    now = [0.0]
    store = RawStore(tmp_path, compress=True, monotonic=lambda: now[0])
    store.write("message", "a", T)
    flushed = store.last_flush
    now[0] = 0.5
    store.write("message", "b", T + 1)
    assert store.dirty and store.last_flush == flushed
    now[0] = 1.6
    store.tick()
    assert not store.dirty
    store.close()


def test_tick_closes_the_file_when_its_hour_is_over(tmp_path):
    clock = [T + 10]
    store = RawStore(tmp_path, compress=True, clock=lambda: clock[0])
    store.write("message", "a")
    clock[0] = T + H + 1
    store.tick()
    assert store.file is None
    assert RawFile(tmp_path / "2026-11-04" / "00.jsonl.gz").damage == []
    lines = lines_of(tmp_path / "2026-11-04" / "00.jsonl.gz")
    assert len(lines) == 1
    store.write("message", "b")  # next write opens the new hour
    store.close()
    assert [p.name for p in raw_files(tmp_path)] == ["00.jsonl.gz", "01.jsonl.gz"]


def test_a_second_writer_never_shares_a_file(tmp_path):
    """The 2026-11-03 19:00 bug: two collectors wrote into one gzip file and broke it. Each
    writer now creates its own file exclusively."""
    a = RawStore(tmp_path, compress=True)
    b = RawStore(tmp_path, compress=True)
    a.write("message", "from a", T)
    b.write("message", "from b", T + 1)
    a.close()
    b.close()
    files = raw_files(tmp_path)
    assert [p.name for p in files] == ["00.jsonl.gz", "00.part2.jsonl.gz"]
    assert [lines_of(p)[0]["raw"] for p in files] == ["from a", "from b"]


def test_plain_files_are_flushed_every_line(tmp_path):
    store = RawStore(tmp_path, compress=False)
    store.write("message", "a", T)
    assert (tmp_path / "2026-11-04" / "00.jsonl").read_text().count("\n") == 1
    store.close()


def test_disk_errors_are_raised_not_swallowed(tmp_path, monkeypatch):
    store = RawStore(tmp_path, compress=False)
    store.write("message", "a", T)

    def full(*_args):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(store.file, "write", full)
    with pytest.raises(OSError):
        store.write("message", "b", T + 1)


def test_default_clock_is_now(tmp_path):
    store = RawStore(tmp_path, compress=True)
    before = time.time_ns()
    store.write("note", {"a": 1})
    store.close()
    (line,) = lines_of(store.current_path)
    assert before <= line["recv_ts_ns"] <= time.time_ns()
