"""The tolerant reader: file order, damaged and truncated gzip files, bad lines, bounded memory."""
import gzip
import json
import os
import random
import tracemalloc
import zlib

from pipeline import raw_reader
from pipeline.raw_reader import RawFile, file_order, parse_envelope, parse_name, raw_files, raw_hours, read_envelopes


def line(i, kind="message"):
    return (json.dumps({"recv_ts_ns": 1_000 + i, "kind": kind, "raw": f"m{i}"}) + "\n").encode()


def member(lines):
    return gzip.compress(b"".join(lines), mtime=0)


def read(path):
    reader = RawFile(path)
    got = [e["raw"] for _n, e in reader]
    return got, reader


def test_names_and_order(tmp_path):
    assert parse_name("19.jsonl.gz") == (19, 1)
    assert parse_name("19.part2.jsonl.gz") == (19, 2)
    assert parse_name("07.jsonl") == (7, 1)
    for bad in ("24.jsonl", "x.jsonl", "19.jsonl.gz.tmp", "19.part.jsonl.gz", "1.jsonl"):
        assert parse_name(bad) is None
    day = tmp_path / "2026-11-03"
    day.mkdir()
    for name in ["19.part10.jsonl.gz", "19.part2.jsonl.gz", "19.jsonl.gz", "00.jsonl", "01.jsonl.gz", "notes.txt", "20.jsonl.gz"]:
        (day / name).write_bytes(b"")
    (tmp_path / "not-a-day").mkdir()
    (tmp_path / "2026-11-02").mkdir()
    (tmp_path / "2026-11-02" / "23.jsonl.gz").write_bytes(b"")
    names = [f"{p.parent.name}/{p.name}" for p in raw_files(tmp_path)]
    assert names == ["2026-11-02/23.jsonl.gz", "2026-11-03/00.jsonl", "2026-11-03/01.jsonl.gz", "2026-11-03/19.jsonl.gz",
                     "2026-11-03/19.part2.jsonl.gz", "2026-11-03/19.part10.jsonl.gz", "2026-11-03/20.jsonl.gz"]
    assert list(raw_hours(tmp_path))[:2] == [("2026-11-02", "23"), ("2026-11-03", "00")]
    assert len(raw_hours(tmp_path)[("2026-11-03", "19")]) == 3
    assert raw_files(tmp_path, "2026-11-02")[0].name == "23.jsonl.gz"
    assert raw_files(tmp_path / "missing") == []
    assert file_order(day / "00.jsonl") < file_order(day / "00.jsonl.gz")


def test_plain_file_with_a_torn_last_line(tmp_path):
    path = tmp_path / "00.jsonl"
    path.write_bytes(line(1) + line(2) + b'{"recv_ts_ns": 12, "ki')
    got, reader = read(path)
    assert got == ["m1", "m2"] and reader.bad_lines == 1 and reader.lines == 3


def test_several_pieces_one_after_another(tmp_path):
    path = tmp_path / "00.jsonl.gz"
    path.write_bytes(member([line(1), line(2)]) + member([line(3)]))
    got, reader = read(path)
    assert got == ["m1", "m2", "m3"] and reader.damage == [] and reader.bad_lines == 0


def test_truncated_last_piece_keeps_every_complete_line(tmp_path):
    """A killed writer: the file just stops. Everything flushed before is kept."""
    compressor = zlib.compressobj(wbits=31)
    data = compressor.compress(line(1) + line(2) + b'{"recv_ts_ns": 3, "kind": "mess') + compressor.flush(zlib.Z_SYNC_FLUSH)
    path = tmp_path / "00.jsonl.gz"
    path.write_bytes(member([line(0)]) + data)
    got, reader = read(path)
    assert got == ["m0", "m1", "m2"]
    assert reader.bad_lines == 1
    assert [d.kind for d in reader.damage] == ["truncated"]
    assert "ends mid-write" in reader.damage[0].describe()


def test_damage_in_the_middle_skips_to_the_next_good_piece(tmp_path):
    good1, bad, good2 = member([line(1)]), bytearray(member([line(i) for i in range(2, 400)])), member([line(999)])
    for i in range(40, 200):
        bad[i] ^= 0x5A  # scramble the middle of the second piece
    path = tmp_path / "00.jsonl.gz"
    path.write_bytes(good1 + bytes(bad) + good2)
    got, reader = read(path)
    assert got[0] == "m1" and got[-1] == "m999"
    assert [d.kind for d in reader.damage] == ["corrupt"]
    assert reader.damage[0].offset == len(good1)
    assert "damaged compressed bytes skipped" in reader.damage[0].describe()


def test_two_writers_interleaved_in_one_file(tmp_path):
    """Regression for 2026-11-03 19:00: two collectors appended to one gzip file at once. The
    reader keeps what decompresses, reports the damage, and recovers any clean piece after it."""
    rng = random.Random(1)
    a = gzip.compress(b"".join(line(i) for i in range(0, 3000)), mtime=0)
    b = gzip.compress(b"".join(line(i) for i in range(5000, 8000)), mtime=0)
    mixed, i, j = bytearray(), 0, 0
    while i < len(a) or j < len(b):  # interleave in random flush-sized chunks
        step = rng.randint(200, 2000)
        mixed += a[i:i + step]
        i += step
        step = rng.randint(200, 2000)
        mixed += b[j:j + step]
        j += step
    after = member([line(9999)])  # a later, clean piece (the next run's part)
    path = tmp_path / "19.jsonl.gz"
    path.write_bytes(bytes(mixed) + after)
    got, reader = read(path)
    assert got[-1] == "m9999"
    assert any(d.kind == "corrupt" for d in reader.damage)
    assert all(g.startswith("m") for g in got)


def test_garbage_before_a_piece(tmp_path):
    path = tmp_path / "00.jsonl.gz"
    path.write_bytes(b"\x00garbage\x1f\x8b\x08nope" + member([line(1)]))
    got, reader = read(path)
    assert got == ["m1"]
    assert reader.damage[0].kind == "corrupt" and reader.damage[0].offset == 0


def test_damage_with_no_good_piece_after_it(tmp_path):
    data = bytearray(member([line(i) for i in range(500)]))
    for i in range(100, len(data) - 20):
        data[i] = 0
    path = tmp_path / "00.jsonl.gz"
    path.write_bytes(bytes(data))
    got, reader = read(path)
    assert [d.kind for d in reader.damage] == ["corrupt"]
    assert reader.damage[0].skipped_bytes == len(data)


def test_piece_start_split_across_reading_chunks(tmp_path, monkeypatch):
    monkeypatch.setattr(raw_reader, "CHUNK", 7)
    path = tmp_path / "00.jsonl.gz"
    bad = bytearray(member([line(i) for i in range(50)]))
    for i in range(30, 60):
        bad[i] ^= 0xFF
    path.write_bytes(bytes(bad) + member([line(77)]))
    got, _reader = read(path)
    assert got[-1] == "m77"


def test_bad_lines_are_counted_with_examples(tmp_path):
    path = tmp_path / "00.jsonl"
    rows = [line(1), b"not json\n", b"[1, 2]\n", b'{"recv_ts_ns": "x", "kind": "m", "raw": 1}\n',
            b'{"recv_ts_ns": -1, "kind": "m", "raw": 1}\n', b'{"recv_ts_ns": 1, "kind": "m"}\n', b"\n", line(2)]
    path.write_bytes(b"".join(rows))
    got, reader = read(path)
    assert got == ["m1", "m2"]
    assert reader.bad_lines == 5 and reader.bad_examples[0] == "not json"
    assert [n for n, _e in RawFile(path)] == [1, 7]  # line numbers count every non-empty line


def test_envelope_validation():
    assert parse_envelope(b'{"recv_ts_ns": 1, "kind": "message", "raw": "x"}')["raw"] == "x"
    assert parse_envelope('{"recv_ts_ns": 1, "kind": "message", "raw": "x", "session": "s", "conn": 2}')["conn"] == 2
    assert parse_envelope('{"recv_ts_ns": true, "kind": "message", "raw": "x"}') is None
    assert parse_envelope('{"recv_ts_ns": 1, "kind": 5, "raw": "x"}') is None


def test_read_envelopes_across_files(tmp_path):
    (tmp_path / "a.jsonl").write_bytes(line(1))
    (tmp_path / "b.jsonl").write_bytes(line(2) + line(3))
    got = [(p.name, n, e["raw"]) for p, n, e in read_envelopes([tmp_path / "a.jsonl", tmp_path / "b.jsonl"])]
    assert got == [("a.jsonl", 1, "m1"), ("b.jsonl", 1, "m2"), ("b.jsonl", 2, "m3")]


def test_memory_stays_bounded_on_a_big_file(tmp_path):
    """The old reader decompressed whole files into memory (several GB for a busy hour)."""
    path = tmp_path / "00.jsonl.gz"
    payload = os.urandom(300).hex()
    with gzip.open(path, "wb") as f:
        for i in range(40_000):
            f.write((json.dumps({"recv_ts_ns": i, "kind": "message", "raw": payload}) + "\n").encode())
    size = 40_000 * 650
    tracemalloc.start()
    count = sum(1 for _ in RawFile(path))
    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    assert count == 40_000
    assert peak < 6_000_000  # bounded (a few MB) although the file holds ~26 MB of text
