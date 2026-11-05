"""The replay engine: rebuilding books, checks, connections, outages, rows, saved state."""
import json
from decimal import Decimal as D

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from pipeline.polymarket import replay
from pipeline.polymarket.replay import Engine
from tests.helpers import (
    T0_MS,
    T0_NS,
    TOKEN_A,
    TOKEN_B,
    ListSink,
    book,
    change,
    env,
    msg,
    price_change,
    rest,
    run_engine,
    trade,
)

S = 1_000_000  # one millisecond in ns


def legacy_start(ns=T0_NS, tokens=(TOKEN_A,)):
    return [env("connected", {"connection": 1, "url": "wss://x"}, ns),
            env("subscribed", {"connection": 1, "request": {"type": "market", "assets_ids": list(tokens)}}, ns + 1)]


def top(engine, token=TOKEN_A):
    rows = [r for r in engine.sink.get("top_of_book") if r[2] == token]
    return [(r[0], r[3], r[5], r[11]) for r in rows]  # stamp, best bid, best ask, cause


def test_book_then_changes_matching_polymarket():
    engine = run_engine([*legacy_start(), msg([book()], T0_NS + 10 * S),
                         msg(price_change(change(price="0.45", size="3", best_bid="0.45"), stamp=T0_MS + 20), T0_NS + 30 * S),
                         msg(price_change(change(price="0.45", size="0", best_bid="0.40"), stamp=T0_MS + 40), T0_NS + 50 * S)])
    assert engine.stats.count["matched Polymarket's best prices"] == 2
    assert not engine.stats.problems
    assert top(engine) == [(T0_MS, D("0.40"), D("0.60"), "full_book"), (T0_MS + 20, D("0.45"), D("0.60"), "change"),
                           (T0_MS + 40, D("0.40"), D("0.60"), "change")]
    deltas = engine.sink.get("book_deltas")
    assert [(r[4], r[5], r[6]) for r in deltas] == [("bid", D("0.45"), D("3")), ("bid", D("0.45"), D("0"))]
    assert deltas[0][-3:] == ("legacy", "test.jsonl", 4)  # conn, source_file, source_line


def test_mismatch_is_reported():
    engine = run_engine([*legacy_start(), msg([book()], T0_NS),
                         msg(price_change(change(price="0.45", size="3", best_bid="0.46"), stamp=T0_MS + 20), T0_NS + S)])
    assert engine.stats.problems[replay.P_BEST] == 1
    assert "ours 0.45/0.60, Polymarket 0.46/0.60" in engine.stats.examples[replay.P_BEST][0]


def test_changes_before_the_first_full_book_are_recorded_but_skipped():
    engine = run_engine([*legacy_start(), msg(price_change(change()), T0_NS + S), msg([book()], T0_NS + 2 * S)])
    assert engine.stats.count["changes before first full book (skipped)"] == 1
    assert len(engine.sink.get("book_deltas")) == 1


def test_trades_ticks_and_sanity_checks():
    engine = run_engine([
        *legacy_start(), msg([book(bids=[("0.40", "1")], asks=[("0.60", "1")], tick="0.01")], T0_NS),
        msg(trade(price="0.5"), T0_NS + S),
        msg(trade(price="1"), T0_NS + 2 * S),
        msg(price_change(change(price="0.455", size="1", best_bid="0.455"), stamp=T0_MS + 1), T0_NS + 3 * S),
        msg({"event_type": "tick_size_change", "asset_id": TOKEN_A, "market": "m", "old_tick_size": "0.01",
             "new_tick_size": "0.001", "timestamp": str(T0_MS + 2)}, T0_NS + 4 * S),
        msg(price_change(change(price="0.456", size="1", best_bid="0.456"), stamp=T0_MS + 3), T0_NS + 5 * S),
        msg(price_change(change(side="BUY", price="0.7", size="1", best_bid="0.7"), stamp=T0_MS + 4), T0_NS + 6 * S),
    ])
    problems = engine.stats.problems
    assert problems[replay.P_TRADE] == 1 and problems[replay.P_TICK] == 1 and problems[replay.P_CROSSED] == 1
    assert [r[4] for r in engine.sink.get("trades")] == [D("0.5"), D("1")]
    assert engine.stats.count["tick_size_change"] == 1


def test_same_timestamp_batch_spread_over_messages_is_checked_once_complete():
    """Regression (2026-11-03): changes from one exchange moment can arrive in several messages,
    each reporting the best prices after ALL of them. Checking after the first gave false alarms."""
    stamp = T0_MS + 50
    engine = run_engine([*legacy_start(), msg([book()], T0_NS),
                         msg(price_change(change(price="0.45", size="3", best_bid="0.47"), stamp=stamp), T0_NS + S),
                         msg(price_change(change(price="0.47", size="2", best_bid="0.47"), stamp=stamp), T0_NS + 2 * S),
                         msg(price_change(change(price="0.40", size="9", best_bid="0.47"), stamp=stamp + 1), T0_NS + 3 * S)])
    assert not engine.stats.problems
    assert engine.stats.count["price changes checked"] == 2
    assert [t[0] for t in top(engine)] == [T0_MS, stamp, stamp + 1]


def test_rest_copy_arriving_after_later_updates_is_compared_by_rewinding():
    """Regression (2026-11-03): a REST copy can arrive after live updates newer than it."""
    copy = book(stamp=T0_MS + 10, bids=[("0.40", "10")])
    engine = run_engine([*legacy_start(), msg([book()], T0_NS),
                         msg(price_change(change(price="0.40", size="11", best_bid="0.40"), stamp=T0_MS + 20), T0_NS + S),
                         env("rest_books", rest(copy), T0_NS + 2 * S)])
    assert engine.stats.count["REST books matched exactly"] == 1 and not engine.stats.problems


def test_rest_copy_arriving_before_updates_waits_for_one_after_its_moment():
    copy = book(stamp=T0_MS + 30, bids=[("0.40", "12")])
    engine = run_engine([*legacy_start(), msg([book()], T0_NS),
                         env("rest_books", rest(copy), T0_NS + S),
                         msg(price_change(change(price="0.40", size="12", best_bid="0.40"), stamp=T0_MS + 30), T0_NS + 2 * S),
                         msg(price_change(change(price="0.40", size="13", best_bid="0.40"), stamp=T0_MS + 35), T0_NS + 3 * S)],
                        finish=False)
    assert engine.stats.count["REST books compared"] == 0  # 35 is within REST's 10 ms tolerance: keep waiting
    engine.feed(msg(price_change(change(price="0.40", size="14", best_bid="0.40"), stamp=T0_MS + 41), T0_NS + 4 * S))
    assert engine.stats.count["REST books matched exactly"] == 1 and not engine.stats.problems


def test_rest_copy_waiting_at_the_end_is_checked_by_finish():
    copy = book(stamp=T0_MS + 30)
    engine = run_engine([*legacy_start(), msg([book()], T0_NS), env("rest_books", rest(copy), T0_NS + S)], finish=False)
    assert engine.stats.count["REST books compared"] == 0
    engine.finish()
    assert engine.stats.count["REST books matched exactly"] == 1


def test_rest_copy_taken_between_two_changes_of_the_same_millisecond_matches():
    """Regression (2026-11-05 00:25:09): two changes stamped ...032 (170 then 120); the REST copy
    stamped ...032 showed 170. Millisecond timestamps cannot order them, so it is not an error."""
    stamp = T0_MS + 32
    copy = book(stamp=stamp, asks=[("0.60", "170")])
    engine = run_engine([*legacy_start(), msg([book(asks=[("0.60", "220")])], T0_NS),
                         msg(price_change(change(side="SELL", price="0.60", size="170", best_ask="0.60"), stamp=stamp), T0_NS + S),
                         msg(price_change(change(side="SELL", price="0.60", size="120", best_ask="0.60"), stamp=stamp), T0_NS + S + 10),
                         msg(price_change(change(side="SELL", price="0.60", size="70", best_ask="0.60"), stamp=stamp + 4), T0_NS + 2 * S),
                         env("rest_books", rest(copy), T0_NS + 3 * S)])
    assert engine.stats.count["REST books matched a state inside their millisecond"] == 1
    assert not engine.stats.problems and not engine.sink.get("resyncs")


def test_rest_copy_stamped_a_few_ms_off_matches():
    """Regression (2026-11-03 20:44:40): a REST copy stamped ...680000 already held the change
    stamped ...680001 (302429.29 -> 2429.29). REST's timestamps are a few ms off the live feed's."""
    stamp = T0_MS + 1000
    copy = book(stamp=stamp, asks=[("0.99", "2429.29")])
    engine = run_engine([*legacy_start(), msg([book(asks=[("0.99", "302429.29")])], T0_NS),
                         msg(price_change(change(side="SELL", price="0.99", size="2429.29", best_bid="0.40", best_ask="0.99"),
                                          stamp=stamp + 1), T0_NS + S),
                         env("rest_books", rest(copy), T0_NS + 2 * S),
                         msg(price_change(change(price="0.40", size="1", best_bid="0.40", best_ask="0.99"), stamp=stamp + 50), T0_NS + 3 * S)])
    assert engine.stats.count["REST books matched a state within 10 ms of their timestamp"] == 1
    assert not engine.stats.problems and not engine.sink.get("resyncs")


def test_rest_copy_that_disagrees_corrects_the_book_and_replays_later_changes():
    copy = book(stamp=T0_MS + 10, bids=[("0.40", "10"), ("0.30", "5")])
    engine = run_engine([*legacy_start(), msg([book()], T0_NS),
                         msg(price_change(change(price="0.41", size="1", best_bid="0.41"), stamp=T0_MS + 20), T0_NS + S),
                         env("rest_books", rest(copy), T0_NS + 2 * S)])
    assert engine.stats.problems[replay.P_REST] == 1
    (resync,) = engine.sink.get("resyncs")
    assert resync[:6] == (T0_MS + 10, T0_NS + 2 * S, TOKEN_A, 1, "bid 0.30: ours None, theirs 5", "rest")
    book_now = engine.books["legacy"][TOKEN_A]
    assert book_now.bids == {D("0.40"): D("10"), D("0.30"): D("5"), D("0.41"): D("1")}
    assert top(engine)[-1][3] == "resync"
    rows = [r for r in engine.sink.get("book_snapshots") if r[1] == "rest"]
    assert len(rows) == 3  # every level of the copy, from the REST source


def test_rest_copy_for_a_token_without_a_book():
    engine = run_engine([*legacy_start(), env("rest_books", rest(book(TOKEN_B)), T0_NS)])
    assert engine.stats.count["REST copies with no live book to compare"] == 1


def test_out_of_order_full_book_does_not_wipe_newer_changes():
    """Regression (2026-11-05 02:36:46, 12 false mismatches): a full book stamped ...098 arrived
    after a change stamped ...103. Replacing the book with it deleted the newer level."""
    engine = run_engine([*legacy_start(), msg([book()], T0_NS),
                         msg(price_change(change(price="0.45", size="50", best_bid="0.45"), stamp=T0_MS + 103), T0_NS + S),
                         msg([book(stamp=T0_MS + 98)], T0_NS + 2 * S),
                         msg(price_change(change(price="0.44", size="1", best_bid="0.45"), stamp=T0_MS + 144), T0_NS + 3 * S)])
    assert not engine.stats.problems
    assert engine.stats.count["out-of-order full books that matched"] == 1
    assert engine.books["legacy"][TOKEN_A].bids[D("0.45")] == D("50")


def test_out_of_order_full_book_that_disagrees_is_trusted_and_newer_changes_replayed():
    engine = run_engine([*legacy_start(), msg([book()], T0_NS),
                         msg(price_change(change(price="0.45", size="50", best_bid="0.45"), stamp=T0_MS + 103), T0_NS + S),
                         msg([book(stamp=T0_MS + 98, bids=[("0.40", "10"), ("0.35", "7")])], T0_NS + 2 * S)])
    assert engine.stats.problems[replay.P_STALE_DIFF] == 1
    assert engine.books["legacy"][TOKEN_A].bids == {D("0.40"): D("10"), D("0.35"): D("7"), D("0.45"): D("50")}
    assert engine.sink.get("resyncs")[0][5] == "live_book"


def test_out_of_order_full_book_older_than_history_is_ignored():
    engine = run_engine([*legacy_start(), msg([book(stamp=T0_MS + 50)], T0_NS),
                         msg([book(stamp=T0_MS + 10)], T0_NS + S)])
    assert engine.stats.count["out-of-order full books too old to check (ignored)"] == 1


# ---------- connections, notes, outages (old-style lines) ----------

def test_legacy_drop_and_reconnect_records_the_real_blind_time():
    last_exchange = T0_MS + 1000
    engine = run_engine([*legacy_start(), msg([book()], T0_NS),
                         msg(price_change(change(), stamp=last_exchange), T0_NS + 5000 * S),  # arrived late
                         env("disconnected", {"connection": 1, "reason": "no close frame received or sent"}, T0_NS + 6000 * S),
                         *legacy_start(T0_NS + 7000 * S),
                         msg([book(stamp=T0_MS + 7100)], T0_NS + 7100 * S)])
    (outage,) = engine.stats.outages
    assert outage.reason == "no close frame received or sent"
    assert outage.start_ns == last_exchange * S and outage.note_ns == T0_NS + 6000 * S
    assert outage.end_ns == T0_NS + 7100 * S and outage.tokens == 1


def test_legacy_subscribe_resets_books():
    engine = run_engine([*legacy_start(), msg([book()], T0_NS), *legacy_start(T0_NS + S),
                         msg(price_change(change()), T0_NS + 2 * S)])
    assert engine.stats.count["changes before first full book (skipped)"] == 1


def test_crash_without_a_stopped_note_is_detected():
    """Regression (2026-11-03): the collector (then called listener.py) was killed and restarted with no "stopped" note."""
    engine = run_engine([*legacy_start(), msg([book()], T0_NS + S),
                         msg(price_change(change(), stamp=T0_MS + 2), T0_NS + 2 * S),
                         *legacy_start(T0_NS + 60_000 * S), msg([book()], T0_NS + 60_001 * S)])
    (outage,) = engine.stats.outages
    assert "without a stop note" in outage.reason
    assert outage.note_ns == T0_NS + 2 * S  # the last line saved before the crash
    assert outage.end_ns == T0_NS + 60_001 * S


def test_stopped_then_started_again():
    engine = run_engine([*legacy_start(), msg([book()], T0_NS), env("stopped", {"reason": "Ctrl+C"}, T0_NS + S),
                         *legacy_start(T0_NS + 100 * S)])
    reasons = [o.reason for o in engine.stats.outages]
    assert reasons == ["collector was not running"]


def test_legacy_universe_change_drops_books_on_purpose():
    engine = run_engine([*legacy_start(), msg([book()], T0_NS), env("universe_changed", {"tokens_added": []}, T0_NS + S),
                         *legacy_start(T0_NS + 2 * S)])
    assert engine.stats.outages == []
    assert engine.stats.count["tokens dropped on purpose (list change or retired connection)"] == 1


# ---------- connections (lines with session and conn) ----------

def new(kind, raw, ns, conn=None, session="s1"):
    return env(kind, raw, ns, session, conn)


def new_conn(conn, ns, tokens=(TOKEN_A,), session="s1"):
    return [new("connected", {"shard": 1}, ns, conn, session),
            new("subscribed", {"shard": 1, "request": {"assets_ids": list(tokens)}}, ns + 1, conn, session)]


def test_standby_failover_loses_nothing_and_records_nothing_twice():
    engine = run_engine([
        new("started", {}, T0_NS), *new_conn(1, T0_NS), *new_conn(2, T0_NS + 1),
        msg([book()], T0_NS + S, "s1", 1), msg([book()], T0_NS + S + 1, "s1", 2),
        msg(price_change(change(price="0.41", size="1", best_bid="0.41"), stamp=T0_MS + 5), T0_NS + 2 * S, "s1", 1),
        msg(price_change(change(price="0.41", size="1", best_bid="0.41"), stamp=T0_MS + 5), T0_NS + 2 * S + 1, "s1", 2),
        msg(trade(), T0_NS + 3 * S, "s1", 1), msg(trade(), T0_NS + 3 * S + 1, "s1", 2),
        new("disconnected", {"reason": "no close frame received or sent"}, T0_NS + 4 * S, 1),
        msg(price_change(change(price="0.42", size="1", best_bid="0.42"), stamp=T0_MS + 6), T0_NS + 5 * S, "s1", 2),
    ])
    assert engine.stats.outages == []
    assert engine.stats.count["failovers to a standby connection"] == 1
    assert len(engine.sink.get("book_deltas")) == 2 and len(engine.sink.get("trades")) == 1
    assert len([r for r in engine.sink.get("book_snapshots") if r[1] == "live"]) == 2  # one book, two levels
    assert [r[-3] for r in engine.sink.get("book_deltas")] == ["s1:1", "s1:2"]
    assert top(engine)[-2][3] == "failover"
    assert not engine.stats.problems


def test_make_before_break_switch_keeps_shared_tokens_and_drops_removed_ones():
    engine = run_engine([
        new("started", {}, T0_NS), *new_conn(1, T0_NS, (TOKEN_A, TOKEN_B)),
        msg([book(TOKEN_A), book(TOKEN_B)], T0_NS + S, "s1", 1),
        new("universe_changed", {"tokens_added": [], "tokens_removed": [TOKEN_B]}, T0_NS + 2 * S),
        *new_conn(2, T0_NS + 3 * S, (TOKEN_A,)), msg([book(TOKEN_A)], T0_NS + 4 * S, "s1", 2),
        new("retired", {"reason": "market list changed"}, T0_NS + 5 * S, 1),
    ])
    assert engine.stats.outages == []
    assert engine.owner == {TOKEN_A: "s1:2"}
    assert engine.stats.count["tokens dropped on purpose (list change or retired connection)"] == 1


def test_new_session_without_stop_note_is_a_crash():
    engine = run_engine([new("started", {}, T0_NS), *new_conn(1, T0_NS), msg([book()], T0_NS + S, "s1", 1),
                         new("started", {}, T0_NS + 100 * S, session="s2"), *new_conn(1, T0_NS + 100 * S, session="s2"),
                         msg([book()], T0_NS + 101 * S, "s2", 1)])
    (outage,) = engine.stats.outages
    assert "without a stop note" in outage.reason and outage.end_ns == T0_NS + 101 * S


def test_clean_stop_and_restart_in_new_format():
    engine = run_engine([new("started", {}, T0_NS), *new_conn(1, T0_NS), msg([book()], T0_NS + S, "s1", 1),
                         new("stopped", {"reason": "SIGTERM"}, T0_NS + 2 * S),
                         new("started", {}, T0_NS + 50 * S, session="s2")])
    assert [o.reason for o in engine.stats.outages] == ["collector was not running"]


def test_paused_note_becomes_an_outage():
    engine = run_engine([new("started", {}, T0_NS), new("paused", {"seconds": 300.5, "why": "computer slept"}, T0_NS + 400 * S)])
    (outage,) = engine.stats.outages
    assert outage.end_ns - outage.start_ns == 300_500_000_000


def test_unsupported_and_bad_messages_are_kept():
    engine = run_engine([*legacy_start(), env("message", "INVALID OPERATION", T0_NS),
                         msg({"event_type": "new_market", "x": 1}, T0_NS + S),
                         msg(price_change(change(price="2")), T0_NS + 2 * S),
                         env("rest_books", "not json", T0_NS + 3 * S)])
    rows = engine.sink.get("unsupported_events")
    assert [(r[1], r[2]) for r in rows] == [("message", "not_json"), ("message", "new_market"),
                                           ("message", "price_change"), ("rest_books", "rest_books")]
    assert rows[0][4] == "INVALID OPERATION"
    assert engine.stats.problems[replay.P_BAD] == 3 and engine.stats.warnings[replay.W_UNKNOWN] == 1


def test_clock_going_backwards_and_late_messages_are_reported():
    engine = run_engine([*legacy_start(), msg([book()], T0_NS + 10 * S), msg([book()], T0_NS + 5 * S),
                         msg([book()], T0_NS + 6 * S),  # still behind: the same step, counted once
                         msg([book()], T0_NS + 11 * S),
                         msg([book()], T0_NS + 2000 * S), msg([book()], T0_NS + 100 * S),  # 1.9 s back: a problem
                         msg([book(stamp=T0_MS)], T0_NS + 60_000 * S),  # an old book is not "late"
                         msg(trade(stamp=T0_MS), T0_NS + 60_001 * S)])  # a trade 60 s old is
    assert engine.stats.warnings[replay.W_CLOCK] == 1
    assert engine.stats.problems[replay.P_CLOCK] == 1
    assert engine.stats.warnings[replay.W_LATE] == 1


def test_mismatches_are_split_into_one_offs_and_persistent_ones():
    engine = run_engine([*legacy_start(), msg([book()], T0_NS),
                         # Polymarket reports a best bid that its own next message contradicts
                         msg(price_change(change(price="0.45", size="0", best_bid="0.45"), stamp=T0_MS + 1), T0_NS + S),
                         msg(price_change(change(price="0.45", size="0", best_bid="0.40"), stamp=T0_MS + 2), T0_NS + 2 * S),
                         # a wrong book: two wrong reports in a row
                         msg(price_change(change(price="0.30", size="1", best_bid="0.41"), stamp=T0_MS + 3), T0_NS + 3 * S),
                         msg(price_change(change(price="0.31", size="1", best_bid="0.41"), stamp=T0_MS + 4), T0_NS + 4 * S)])
    assert engine.stats.problems[replay.P_BEST] == 3
    assert engine.stats.count[replay.C_HEALED] == 1 and engine.stats.count[replay.C_PERSISTENT] == 1


def test_collector_events_rows_and_no_rows_without_a_sink():
    engine = run_engine([*legacy_start(), env("rest_books_failed", {"reason": "timeout"}, T0_NS)])
    assert [r[1] for r in engine.sink.get("collector_events")] == ["connected", "subscribed", "rest_books_failed"]
    quiet = run_engine([*legacy_start(), msg([book()], T0_NS)], sink=False)
    assert quiet.sink is None and quiet.stats.count["book"] == 1


def test_messages_with_no_connected_note_still_count():
    engine = run_engine([msg([book()], T0_NS)])
    assert engine.owner == {TOKEN_A: "legacy"}


# ---------- saving the state between hours ----------

def test_state_rejects_another_version():
    import pytest
    with pytest.raises(ValueError):
        Engine.from_state({"version": 99})


EVENT_KINDS = ["change", "change", "change", "change", "book", "stale_book", "trade", "rest", "drop", "reconnect", "tick"]


@st.composite
def streams(draw):
    """Random but plausible streams: books, change batches, trades, REST copies, drops, reconnects."""
    out = [*legacy_start()]
    ns, stamp = T0_NS + 10, T0_MS
    tokens = [TOKEN_A, TOKEN_B]
    out.append(msg([book(TOKEN_A), book(TOKEN_B)], ns))
    prices = [f"0.{i:02d}" for i in range(30, 70)]
    for _ in range(draw(st.integers(5, 60))):
        kind = draw(st.sampled_from(EVENT_KINDS))
        ns += draw(st.integers(1, 5)) * S
        stamp += draw(st.integers(0, 3))
        token = draw(st.sampled_from(tokens))
        if kind == "change":
            changes = [change(token, draw(st.sampled_from(["BUY", "SELL"])), draw(st.sampled_from(prices)),
                              str(draw(st.integers(0, 9))), "0.3", "0.7") for _ in range(draw(st.integers(1, 3)))]
            out.append(msg(price_change(*changes, stamp=stamp), ns))
        elif kind == "book":
            out.append(msg([book(token, stamp)], ns))
        elif kind == "stale_book":
            out.append(msg([book(token, max(T0_MS, stamp - 2))], ns))
        elif kind == "trade":
            out.append(msg(trade(token, stamp), ns))
        elif kind == "rest":
            out.append(env("rest_books", rest(book(token, max(T0_MS, stamp - draw(st.integers(0, 3))))), ns))
        elif kind == "drop":
            out.append(env("disconnected", {"connection": 1, "reason": "drop"}, ns))
        elif kind == "reconnect":
            out += legacy_start(ns, tokens)
            out.append(msg([book(TOKEN_A, stamp), book(TOKEN_B, stamp)], ns + 2))
        elif kind == "tick":
            out.append(msg({"event_type": "tick_size_change", "asset_id": token, "market": "m",
                            "new_tick_size": "0.01", "timestamp": str(stamp)}, ns))
    return out


def run_split(envelopes, cuts):
    """Feed the stream, saving and reloading the whole state (through JSON) at every cut."""
    sink = ListSink()
    engine = Engine(sink)
    for i, e in enumerate(envelopes, start=1):
        if i in cuts:
            stats = engine.stats  # counters are for reports, not part of the saved state
            engine = Engine.from_state(json.loads(json.dumps(engine.state())), sink)
            engine.stats = stats
        engine.feed(e, "f", i)
    engine.finish()
    return engine


@settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(streams(), st.sets(st.integers(1, 80), max_size=6))
def test_replay_split_at_any_checkpoints_equals_one_pass(envelopes, cuts):
    whole = run_split(envelopes, set())
    split = run_split(envelopes, cuts)
    assert split.sink.rows == whole.sink.rows
    assert split.state() == whole.state()
    assert split.stats.problems == whole.stats.problems


@settings(max_examples=100, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(streams())
def test_replay_is_deterministic(envelopes):
    assert run_split(envelopes, set()).sink.rows == run_split(envelopes, set()).sink.rows
