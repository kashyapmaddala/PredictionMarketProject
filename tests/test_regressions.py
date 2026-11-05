"""Regression tests built from the real messages of each bug found in the saved data.

The fixtures are short cuts of data/raw/polymarket (only the tokens involved), kept verbatim
except that lists (initial dumps, REST replies, subscriptions) are trimmed to those tokens.
Each test also shows the old behaviour failing on the same messages, so the fix is proven.
"""
from decimal import Decimal as D
from pathlib import Path

from pipeline.book import Book
from pipeline.polymarket import replay
from pipeline.polymarket.replay import Engine
from pipeline.raw_reader import RawFile
from tests.helpers import ListSink

FIXTURES = Path(__file__).parent / "fixtures"


def replay_fixture(name, engine_class=Engine):
    engine = engine_class(ListSink())
    for number, envelope in RawFile(FIXTURES / name):
        engine.feed(envelope, name, number)
    engine.finish()
    return engine


class OldBehaviour(Engine):
    """The version 1 rules: a full book always replaces the book; a REST copy must equal the book
    exactly at its own timestamp (after every change stamped at or before it)."""

    def live_book(self, event, recv, conn):
        book = self.books.setdefault(conn, {}).setdefault(event.token, Book())
        if book.started and event.stamp < book.last_stamp:
            book.last_stamp = 0  # pretend it is new, so the parent replaces the book
        super().live_book(event, recv, conn)

    def check_copy(self, token, saved):
        copy, _recv = saved
        conn = self.owner.get(token)
        book = self.books.get(conn, {}).get(token)
        if book is None or not book.can_rewind_to(copy["stamp"]):
            return
        theirs = (replay._levels(copy["bids"]), replay._levels(copy["asks"]))
        if book.as_of(copy["stamp"]) != theirs:
            self.stats.problem(replay.P_REST, "old rule")


def test_out_of_order_full_book_2026_11_05_023646():
    """12 best-price mismatches at 02:36:46 UTC (Mississippi Senate, Republicans, "No" token
    4639340586...): a change stamped ...206103 set bid 0.981 = 50, then a full book stamped
    ...206098 (5 ms OLDER) arrived. Replacing the book with it deleted the newer 0.981 level, so
    every later check said ours 0.98 vs Polymarket 0.981. Our bug, now fixed: an older full book
    is compared with our book at its own moment instead of replacing newer data."""
    old = replay_fixture("regression_2026-11-05_stale_book.jsonl.gz", OldBehaviour)
    assert old.stats.problems[replay.P_BEST] >= 6
    assert "ours 0.98/0.984, Polymarket 0.981/0.984" in old.stats.examples[replay.P_BEST][0]

    new = replay_fixture("regression_2026-11-05_stale_book.jsonl.gz")
    assert not new.stats.problems
    assert new.stats.count["out-of-order full books that matched"] >= 1
    assert new.stats.count["price changes checked"] > 50


def test_rest_copy_between_two_changes_of_one_millisecond_2026_11_05_002509():
    """2 REST mismatches (copy at 1793838309032, Mississippi Senate, Democrats): two changes were
    both stamped ...309032 (0.981 ask 170, then 120); the REST copy stamped ...309032 showed 170.
    Millisecond timestamps cannot order them: not a bug in our data, not Polymarket's either."""
    old = replay_fixture("regression_2026-11-05_rest_same_millisecond.jsonl.gz", OldBehaviour)
    assert old.stats.problems[replay.P_REST] == 2

    new = replay_fixture("regression_2026-11-05_rest_same_millisecond.jsonl.gz")
    assert not new.stats.problems
    assert new.stats.count["REST books matched a state inside their millisecond"] == 2
    assert not new.sink.get("resyncs")  # v1 "corrected" a right book to a mid-millisecond state


def test_rest_copy_stamped_a_few_ms_off_2026_11_03_204440():
    """v1 resynced here: the REST copy stamped ...680000 already held the change stamped
    ...680001 (0.99 ask 302429.29 -> 2429.29). REST's stamps run a few ms off the live feed's."""
    old = replay_fixture("regression_2026-11-03_rest_clock_offset.jsonl.gz", OldBehaviour)
    assert old.stats.problems[replay.P_REST] == 2

    new = replay_fixture("regression_2026-11-03_rest_clock_offset.jsonl.gz")
    assert not new.stats.problems
    assert new.stats.count["REST books matched a state within 10 ms of their timestamp"] == 2


def test_fixture_books_are_exact():
    new = replay_fixture("regression_2026-11-05_stale_book.jsonl.gz")
    for book in new.books["legacy"].values():
        assert all(isinstance(p, D) and isinstance(s, D) for p, s in book.bids.items())
