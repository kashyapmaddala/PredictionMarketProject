"""The order book: exact decimals, rewinding, resyncing, and saving between runs."""
from decimal import Decimal as D

from hypothesis import given, settings
from hypothesis import strategies as st

from pipeline import book as book_module
from pipeline.book import ASK, BID, Book, diff


def make(bids=None, asks=None, stamp=100):
    b = Book()
    b.replace(bids or {D("0.4"): D("10")}, asks or {D("0.6"): D("20")}, stamp, D("0.01"))
    return b


def test_new_book_is_not_started_and_has_no_best_prices():
    b = Book()
    assert not b.started
    assert b.best_bid() is None and b.best_ask() is None
    assert b.as_of(0) is None


def test_replace_and_best_prices():
    b = make({D("0.4"): D("10"), D("0.45"): D("1")}, {D("0.6"): D("20"), D("0.55"): D("2")})
    assert b.started and b.tick == D("0.01")
    assert b.best_bid() == D("0.45") and b.best_ask() == D("0.55")


def test_apply_sets_and_removes_levels_with_absolute_sizes():
    b = make()
    b.apply(BID, D("0.41"), D("3"), 101)
    assert b.bids[D("0.41")] == D("3")
    b.apply(BID, D("0.41"), D("7"), 102)  # absolute, not added
    assert b.bids[D("0.41")] == D("7")
    b.apply(BID, D("0.41"), D("0"), 103)
    assert D("0.41") not in b.bids
    b.apply(ASK, D("0.99"), D("0"), 104)  # removing a level that is not there is harmless
    assert b.last_stamp == 104


def test_decimals_are_exact():
    b = make()
    b.apply(BID, D("0.1"), D("0.1"), 101)
    b.apply(BID, D("0.2"), D("0.2"), 101)
    assert b.bids[D("0.1")] + b.bids[D("0.2")] == D("0.3")  # 0.1 + 0.2 != 0.3 in floats


def test_as_of_rewinds_later_changes():
    b = make()
    b.apply(BID, D("0.41"), D("3"), 110)
    b.apply(BID, D("0.4"), D("0"), 120)
    assert b.as_of(105) == ({D("0.4"): D("10")}, {D("0.6"): D("20")})
    assert b.as_of(115) == ({D("0.4"): D("10"), D("0.41"): D("3")}, {D("0.6"): D("20")})
    assert b.as_of(120) == (b.bids, b.asks)
    assert b.as_of(99) is None  # before the full book: unknown


def test_states_at_lists_every_state_inside_one_millisecond():
    b = make()
    b.apply(ASK, D("0.6"), D("170"), 200)
    b.apply(ASK, D("0.6"), D("120"), 200)
    b.apply(ASK, D("0.6"), D("70"), 201)
    sizes = [asks[D("0.6")] for _bids, asks in b.states_at(200)]
    assert sizes == [D("120"), D("170"), D("20")]  # after both, after the first, before both


def test_resync_replays_later_changes():
    b = make()
    b.apply(BID, D("0.41"), D("3"), 110)
    b.apply(BID, D("0.42"), D("4"), 130)
    b.resync({D("0.3"): D("1")}, {D("0.7"): D("2")}, 120)
    assert b.bids == {D("0.3"): D("1"), D("0.42"): D("4")}  # 0.41@110 was before the copy: dropped
    assert b.asks == {D("0.7"): D("2")}
    assert b.last_stamp == 130 and b.horizon == 120


def test_history_is_bounded_by_time(monkeypatch):
    monkeypatch.setattr(book_module, "HISTORY_MS", 1000)
    b = make(stamp=0)
    for t in range(0, 5000, 10):
        b.apply(BID, D("0.41"), D(t + 1), t)
    assert b.history[0][0] >= 4990 - 1000
    assert not b.can_rewind_to(3000) and b.can_rewind_to(4000)


def test_history_has_a_hard_cap(monkeypatch):
    monkeypatch.setattr(book_module, "HISTORY_CAP", 50)
    b = make()
    for i in range(200):
        b.apply(BID, D("0.41"), D(i + 1), 100)
    assert len(b.history) == 50
    assert b.as_of(100) == (b.bids, b.asks)  # changes at the newest time can still be read
    assert b.as_of(99) is None


def test_state_round_trip():
    b = make()
    b.apply(BID, D("0.41"), D("3.5"), 110)
    b.apply(ASK, D("0.6"), D("0"), 111)
    copy = Book.from_state(b.to_state())
    assert (copy.bids, copy.asks, copy.tick, copy.started, copy.horizon, copy.last_stamp) == \
        (b.bids, b.asks, b.tick, b.started, b.horizon, b.last_stamp)
    assert list(copy.history) == list(b.history)
    assert copy.to_state() == b.to_state()


def test_diff_describes_every_wrong_level():
    ours = ({D("0.4"): D("1")}, {D("0.6"): D("2")})
    theirs = ({D("0.4"): D("1"), D("0.3"): D("5")}, {})
    assert diff(ours, theirs) == ["bid 0.3: ours None, theirs 5", "ask 0.6: ours 2, theirs None"]
    assert diff(ours, ours) == []


# ---------- properties ----------

PRICES = [D(f"0.{i:02d}") for i in range(1, 100)]
sides = st.sampled_from([BID, ASK])
prices = st.sampled_from(PRICES)
sizes = st.integers(0, 50).map(lambda n: D(n) / 4)  # 0 (remove) and exact quarter sizes
changes = st.lists(st.tuples(sides, prices, sizes, st.integers(0, 3)), max_size=60)
levels = st.dictionaries(prices, sizes.filter(lambda s: s > 0), max_size=10)


def run(changes_, start=100):
    b = Book()
    b.replace({}, {}, start)
    t = start
    for side, price, size, step in changes_:
        t += step
        b.apply(side, price, size, t)
    return b


def full_replay(changes_, start=100, until=None):
    """Reference model: plain dicts, no history."""
    bids, asks, t = {}, {}, start
    for side, price, size, step in changes_:
        t += step
        if until is not None and t > until:
            break
        target = bids if side == BID else asks
        if size == 0:
            target.pop(price, None)
        else:
            target[price] = size
    return bids, asks


@settings(max_examples=300, deadline=None)
@given(changes)
def test_incremental_replay_equals_reference(changes_):
    b = run(changes_)
    assert (b.bids, b.asks) == full_replay(changes_)
    assert all(size > 0 for size in list(b.bids.values()) + list(b.asks.values()))


@settings(max_examples=300, deadline=None)
@given(changes, st.integers(100, 300))
def test_rewind_equals_replay_up_to_that_time(changes_, moment):
    b = run(changes_)
    assert b.as_of(moment) == full_replay(changes_, until=moment)


@settings(max_examples=200, deadline=None)
@given(changes, st.integers(0, 60))
def test_rewind_then_forward_gives_the_original(changes_, cut):
    """Rewind to time t, then apply every change after t again: same book as before."""
    b = run(changes_)
    times, t = [], 100
    for _side, _price, _size, step in changes_:
        t += step
        times.append(t)
    moment = times[cut] if cut < len(times) else 100
    rewound = Book()
    rewound.replace(*b.as_of(moment), moment)
    for (side, price, size, _step), when in zip(changes_, times, strict=True):
        if when > moment:
            rewound.apply(side, price, size, when)
    assert (rewound.bids, rewound.asks) == (b.bids, b.asks)


@settings(max_examples=200, deadline=None)
@given(changes, levels, levels, st.integers(100, 300))
def test_resync_invariants(changes_, copy_bids, copy_asks, moment):
    """After resync(copy at t): the book equals the copy plus every change after t, its rewind to
    t equals the copy, and its newest time never goes backwards."""
    b = run(changes_)
    newest = b.last_stamp
    b.resync(copy_bids, copy_asks, moment)
    expected_bids, expected_asks, t = dict(copy_bids), dict(copy_asks), 100
    for side, price, size, step in changes_:
        t += step
        if t > moment:
            target = expected_bids if side == BID else expected_asks
            if size == 0:
                target.pop(price, None)
            else:
                target[price] = size
    assert (b.bids, b.asks) == (expected_bids, expected_asks)
    assert b.as_of(moment) == (copy_bids, copy_asks)
    assert b.last_stamp == max(newest, moment)


@settings(max_examples=150, deadline=None)
@given(changes, st.integers(0, 60))
def test_state_round_trip_mid_stream_changes_nothing(changes_, cut):
    """Saving and loading the book at any point, then carrying on, equals never saving."""
    whole = run(changes_)
    b = Book()
    b.replace({}, {}, 100)
    t = 100
    for i, (side, price, size, step) in enumerate(changes_):
        if i == cut:
            b = Book.from_state(b.to_state())
        t += step
        b.apply(side, price, size, t)
    assert b.to_state() == whole.to_state()
