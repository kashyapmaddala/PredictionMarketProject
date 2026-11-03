"""Check that the saved Polymarket data is right (step 4).

Reads the raw files saved by listener.py, rebuilds every order book from them
message by message, and runs these checks:

1. Rebuilt book vs Polymarket's own numbers. Every "price_change" message tells us
   Polymarket's best buy and best sell price right after the change. Our rebuilt
   book must show exactly the same two prices, every single time.
   Also: every few minutes the listener saves an independent copy of every book fetched
   straight from Polymarket (REST). Our rebuilt book at that exact moment must match it
   level by level (every price and size, not just the best ones).
2. Sanity. No crossed books (best buy must be below best sell), prices between 0
   and 1 and on the market's tick, sizes positive.
3. Trades. Every trade price must be between 0 and 1. (Checking it against the book
   comes later.)
4. Gaps. Every time a connection dropped or the listener was stopped, how long we
   were blind before it came back.

Nothing is changed: the raw files are only read, so this can be re-run any time.
The report is printed and saved to data/reports/polymarket/<UTC time>.txt.

    python3 check_data.py                  # every saved file
    python3 check_data.py 2026-11-03       # one UTC day
"""
import gzip
import json
import sys
import zlib
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

HERE = Path(__file__).resolve().parent
RAW_DIR = HERE / "data" / "raw" / "polymarket"
REPORT_DIR = HERE / "data" / "reports" / "polymarket"
MAX_EXAMPLES = 5  # how many examples of each problem to show


# ---------- reading the raw files ----------

def file_order(path):
    """19.jsonl.gz, then 19.part2.jsonl.gz, 19.part3.jsonl.gz ... (one hour, restarted listener)."""
    pieces = path.name.split(".")
    part = int(pieces[1][4:]) if pieces[1].startswith("part") else 1
    return int(pieces[0]), part


def raw_files(day=None):
    folders = [RAW_DIR / day] if day else sorted(p for p in RAW_DIR.iterdir() if p.is_dir())
    for folder in folders:
        yield from sorted(folder.glob("*.jsonl*"), key=file_order)


GZIP_START = b"\x1f\x8b\x08"  # every compressed piece starts with these bytes


BROKEN = []  # (file name, bytes skipped) for every unreadable stretch, reported as a problem


def unzip_tolerant(data, name=""):
    """Decompress a .gz file that may contain broken pieces.

    A compressed file can hold several pieces one after another. If the listener was killed
    mid-write, a piece just ends early (normal: at most about a second lost, and the gap check
    sees the restart). If a piece is damaged in the middle, everything after the damage in that
    piece is lost: that is recorded in BROKEN and reported as a problem. Either way this keeps
    whatever decompresses, then jumps to the next good piece.
    """
    pos = 0
    while pos < len(data):
        unzip = zlib.decompressobj(31)
        out, damaged = [], False
        try:
            for i in range(pos, len(data), 1 << 16):  # feed it in chunks, keep what works
                out.append(unzip.decompress(data[i:i + (1 << 16)]))
                if unzip.eof:
                    break
        except zlib.error:
            damaged = True
        yield b"".join(out)
        if unzip.eof:
            pos = len(data) - len(unzip.unused_data)
            continue
        nxt = data.find(GZIP_START, pos + 1)
        while nxt != -1:
            try:
                zlib.decompressobj(31).decompress(data[nxt:nxt + 1024])
                break  # a real piece starts here
            except zlib.error:
                nxt = data.find(GZIP_START, nxt + 1)  # just bytes that looked like a start
        if damaged:
            BROKEN.append((name, (nxt if nxt != -1 else len(data)) - pos))
        if nxt == -1:
            return
        pos = nxt


def read_lines(path):
    """Every saved line in one file, skipping half-written lines (from a killed listener or a
    file that is still being written)."""
    if path.suffix == ".gz":
        text = b"".join(unzip_tolerant(path.read_bytes(), path.name)).decode("utf-8", errors="replace")
        lines = text.split("\n")
    else:
        lines = open(path, encoding="utf-8")
    for text in lines:
        if not text.strip():
            continue
        try:
            line = json.loads(text)
            if isinstance(line, dict) and "recv_ts_ns" in line and "kind" in line:
                yield line
        except ValueError:
            continue  # half-written line


def events_in(line):
    data = json.loads(line["raw"])
    for event in data if isinstance(data, list) else [data]:
        if isinstance(event, dict):
            yield event


def dec(text):
    try:
        return Decimal(str(text))
    except (InvalidOperation, TypeError):
        return None


def when(ns):
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ---------- the order book ----------

class Book:
    def __init__(self):
        self.bids = {}  # price -> size
        self.asks = {}
        self.tick = None
        self.started = False  # only trust it after a full "book" message on the current connection
        self.history = deque(maxlen=20000)  # (exchange time, side, price, size before, size after) to rewind or replay
        self.replaced_at = 0  # exchange time of the last full book
        self.last_stamp = 0  # exchange time of the latest update applied

    def replace(self, event):
        self.bids = {dec(l["price"]): dec(l["size"]) for l in event.get("bids", [])}
        self.asks = {dec(l["price"]): dec(l["size"]) for l in event.get("asks", [])}
        self.tick = dec(event.get("tick_size")) or self.tick
        self.started = True
        self.history.clear()
        self.replaced_at = self.last_stamp = int(event.get("timestamp") or 0)

    def change(self, side, price, size, stamp=0):
        levels = self.bids if side == "BUY" else self.asks
        self.history.append((stamp, side, price, levels.get(price), size))
        if size == 0:
            levels.pop(price, None)
        else:
            levels[price] = size
        self.last_stamp = max(self.last_stamp, stamp)

    def as_of(self, stamp):
        """The book as it was at exchange time `stamp`, by undoing every later change.
        Returns None if we can't rewind that far."""
        if stamp < self.replaced_at:
            return None
        bids, asks = dict(self.bids), dict(self.asks)
        for when_, side, price, before, _after in reversed(self.history):
            if when_ <= stamp:
                break
            levels = bids if side == "BUY" else asks
            if before is None:
                levels.pop(price, None)
            else:
                levels[price] = before
        else:
            if len(self.history) == self.history.maxlen:
                return None  # older changes were already forgotten
        return bids, asks

    def resync(self, bids, asks, stamp):
        """Make the book match a trusted copy taken at exchange time `stamp`, then replay our own
        later changes on top (they happened after the copy was taken)."""
        later = [(when_, side, price, after) for when_, side, price, _before, after in self.history if when_ > stamp]
        self.bids, self.asks = dict(bids), dict(asks)
        self.history.clear()
        self.replaced_at = self.last_stamp = stamp
        for when_, side, price, after in later:
            self.change(side, price, after, when_)

    def best_bid(self):
        return max(self.bids) if self.bids else None

    def best_ask(self):
        return min(self.asks) if self.asks else None


# ---------- the checks ----------

class Checker:
    def __init__(self):
        self.books = defaultdict(Book)
        self.pending = {}  # token -> (exchange timestamp, last change, recv ns) not yet compared
        self.snapshots = {}  # token -> REST book waiting until our book has caught up to its moment
        self.count = Counter()
        self.problems = defaultdict(list)  # name -> examples
        self.problem_count = Counter()
        self.first_ns = self.last_ns = None
        self.down_since = {}  # connection -> when it dropped
        self.outages = []  # (start_ns, end_ns, connection, reason)
        self.stopped_at = None
        self.live = set()  # connection numbers we believe are connected
        self.prev_ns = None  # time of the line before the current one

    def finish_waiting(self):
        """Run the comparisons still waiting for more data (end of data, or the listener stopped)."""
        for stamp, change, ns in self.pending.values():
            self.compare(change, ns)
        self.pending.clear()
        for token in list(self.snapshots):
            self.snapshot_due(token, None)

    def problem(self, name, text):
        self.problem_count[name] += 1
        if len(self.problems[name]) < MAX_EXAMPLES:
            self.problems[name].append(text)

    def note(self, line):
        kind, info, ns = line["kind"], line["raw"], line["recv_ts_ns"]
        number = info.get("connection", 1) if isinstance(info, dict) else 1
        if kind == "subscribed":
            # A new connection: books from before are stale until its fresh full copy arrives
            request = info.get("request", {}) if isinstance(info, dict) else {}
            for token in request.get("assets_ids", []):
                self.books[token].started = False
                self.pending.pop(token, None)
                self.snapshots.pop(token, None)
        elif kind == "disconnected":
            self.down_since.setdefault(number, (ns, info.get("reason", "")))
        elif kind == "connected":
            if number in self.live and number not in self.down_since and not self.stopped_at:
                # Connected again with no drop or stop note: the listener was killed or crashed
                self.outages.append((self.prev_ns, ns, "listener", "restarted without a stop note (killed, crashed or Mac shut down)"))
                self.finish_waiting()
                for book in self.books.values():
                    book.started = False
            self.live.add(number)
            if number in self.down_since:
                start, reason = self.down_since.pop(number)
                self.outages.append((start, ns, f"connection {number}", reason))
            if self.stopped_at:
                self.outages.append((self.stopped_at, ns, "listener", "listener was not running"))
                self.stopped_at = None
        elif kind == "stopped":
            self.stopped_at = ns
            self.down_since.clear()
            self.live.clear()
            self.finish_waiting()
            for book in self.books.values():
                book.started = False
        elif kind == "universe_changed":
            self.live.clear()  # every connection is about to be reopened on purpose

    def message(self, line):
        ns = line["recv_ts_ns"]
        for event in events_in(line):
            kind = event.get("event_type") or ("price_change" if "price_changes" in event else None)
            self.count[kind] += 1
            if kind == "book":
                self.snapshot_due(event["asset_id"], event.get("timestamp"))
                self.pending.pop(event["asset_id"], None)  # a full book replaces everything
                book = self.books[event["asset_id"]]
                book.replace(event)
                self.sanity(event["asset_id"], book, ns)
            elif kind == "price_change":
                # Changes made at the same moment (same exchange timestamp) can arrive spread over
                # several entries or messages, and each one reports the best prices AFTER all of
                # them. So for each token, apply everything with one timestamp, and compare only
                # when that token's next timestamp (or a full book) shows the batch is finished.
                stamp = event.get("timestamp")
                for change in event.get("price_changes", []):
                    token = change["asset_id"]
                    self.snapshot_due(token, stamp)
                    waiting = self.pending.get(token)
                    if waiting and waiting[0] != stamp:
                        self.compare(*waiting[1:])
                        del self.pending[token]
                    if self.apply_change(change, ns, stamp):
                        self.pending[token] = (stamp, change, ns)
            elif kind == "tick_size_change":
                self.books[event["asset_id"]].tick = dec(event.get("new_tick_size"))
            elif kind == "last_trade_price":
                price = dec(event.get("price"))
                if price is None or not (0 < price < 1):
                    self.problem("trade price outside 0 to 1", f"{when(ns)} token {event.get('asset_id', '?')[:12]}... price {event.get('price')}")

    def rest_books(self, line):
        """An independent copy of books fetched over REST, each taken at one exact exchange time.
        Live updates and the copy arrive in either order, so:
        - if our book already has updates from after that time, rewind it and compare now;
        - otherwise hold the copy until an update from after that time shows we've caught up."""
        for snap in json.loads(line["raw"]):
            token = snap.get("asset_id")
            book = self.books[token] if token else None
            if not book or not book.started:
                continue
            self.snapshot_due(token, None)  # an older copy still waiting: check it now
            if book.last_stamp > int(snap.get("timestamp", 0)):
                self.compare_snapshot(token, snap)
            else:
                self.snapshots[token] = snap

    def snapshot_due(self, token, next_stamp):
        """Compare a waiting copy once the next update is from after the copy's moment."""
        snap = self.snapshots.get(token)
        if not snap:
            return
        if next_stamp is not None and int(next_stamp) <= int(snap.get("timestamp", 0)):
            return  # this update happened before the copy was taken: apply it first
        del self.snapshots[token]
        self.compare_snapshot(token, snap)

    def compare_snapshot(self, token, snap):
        book = self.books[token]
        if not book.started:
            return
        ours = book.as_of(int(snap.get("timestamp", 0)))
        if ours is None:
            self.count["REST copies too old to compare"] += 1
            return
        bids, asks = ours
        theirs_bids = {dec(l["price"]): dec(l["size"]) for l in snap.get("bids", [])}
        theirs_asks = {dec(l["price"]): dec(l["size"]) for l in snap.get("asks", [])}
        self.count["REST books compared"] += 1
        if theirs_bids == bids and theirs_asks == asks:
            self.count["REST books matched exactly"] += 1
            return
        wrong = [f"buy {p}: ours {bids.get(p)}, REST {theirs_bids.get(p)}" for p in set(theirs_bids) | set(bids) if theirs_bids.get(p) != bids.get(p)]
        wrong += [f"sell {p}: ours {asks.get(p)}, REST {theirs_asks.get(p)}" for p in set(theirs_asks) | set(asks) if theirs_asks.get(p) != asks.get(p)]
        self.problem("rebuilt book differs from Polymarket's REST copy",
                     f"copy at {snap.get('timestamp')} token {token[:12]}... {len(wrong)} levels differ: {'; '.join(wrong[:3])}")

    def apply_change(self, change, ns, stamp=None):
        """Apply one change to its book. Returns False if it was skipped."""
        token = change["asset_id"]
        book = self.books[token]
        if not book.started:
            self.count["changes before first full book (skipped)"] += 1
            return False
        price, size = dec(change["price"]), dec(change["size"])
        if price is None or size is None or size < 0:
            self.problem("bad price or size", f"{when(ns)} token {token[:12]}... {change}")
            return False
        book.change(change["side"], price, size, int(stamp or 0))
        return True

    def compare(self, change, ns):
        token = change["asset_id"]
        book = self.books[token]
        self.count["price changes checked"] += 1

        # Check 1: our best prices must equal Polymarket's best prices
        theirs_bid, theirs_ask = dec(change.get("best_bid")), dec(change.get("best_ask"))
        ours_bid, ours_ask = book.best_bid(), book.best_ask()
        same_bid = (ours_bid == theirs_bid) or (ours_bid is None and theirs_bid in (None, 0))
        same_ask = (ours_ask == theirs_ask) or (ours_ask is None and theirs_ask in (None, 1, 0))
        if same_bid and same_ask:
            self.count["matched Polymarket's best prices"] += 1
        else:
            self.problem("rebuilt book differs from Polymarket's best prices",
                         f"{when(ns)} token {token[:12]}... ours {ours_bid}/{ours_ask}, Polymarket {theirs_bid}/{theirs_ask}")
        self.sanity(token, book, ns)

    def sanity(self, token, book, ns):
        bid, ask = book.best_bid(), book.best_ask()
        if bid is not None and ask is not None and bid >= ask:
            self.problem("crossed book (best buy >= best sell)", f"{when(ns)} token {token[:12]}... buy {bid} sell {ask}")
        for price, size in list(book.bids.items()) + list(book.asks.items()):
            if price is None or not (0 < price < 1):
                self.problem("price outside 0 to 1", f"{when(ns)} token {token[:12]}... price {price}")
            elif book.tick and price % book.tick != 0:
                self.problem("price not on the tick", f"{when(ns)} token {token[:12]}... price {price}, tick {book.tick}")
            if size is None or size <= 0:
                self.problem("size not positive", f"{when(ns)} token {token[:12]}... size {size}")

    def run(self, files):
        for path in files:
            self.count["files read"] += 1
            for line in read_lines(path):
                ns = line["recv_ts_ns"]
                self.first_ns = self.first_ns or ns
                self.prev_ns, self.last_ns = self.last_ns, ns
                self.count["lines read"] += 1
                if line["kind"] in ("message", "rest_books"):
                    try:
                        self.message(line) if line["kind"] == "message" else self.rest_books(line)
                    except (ValueError, KeyError) as problem:
                        self.problem("message we could not read", f"{when(ns)} {problem}")
                else:
                    self.note(line)
        self.finish_waiting()
        if self.stopped_at:
            self.outages.append((self.stopped_at, None, "listener", "stopped and not restarted (yet)"))

    def report(self):
        out = ["Polymarket data check", "=" * 21]
        if not self.first_ns:
            return "No saved data found."
        hours = (self.last_ns - self.first_ns) / 3.6e12
        out.append(f"Data from {when(self.first_ns)} to {when(self.last_ns)} ({hours:.1f} hours span)")
        out.append(f"Files: {self.count['files read']}, lines: {self.count['lines read']:,}, tokens with books: {len(self.books)}")
        out.append(f"Messages: {self.count['book']:,} full books, {self.count['price_change']:,} price-change messages, "
                   f"{self.count['last_trade_price']:,} trades")

        checked = self.count["price changes checked"]
        matched = self.count["matched Polymarket's best prices"]
        out.append("")
        out.append("CHECK 1: rebuilt books vs Polymarket's own best prices")
        if checked:
            out.append(f"  {matched:,} of {checked:,} changes matched ({100 * matched / checked:.4f}%)")
        compared = self.count["REST books compared"]
        if compared:
            exact = self.count["REST books matched exactly"]
            out.append(f"  {exact:,} of {compared:,} REST book copies matched our rebuilt book exactly, every level ({100 * exact / compared:.2f}%)")
        else:
            out.append("  (no REST book copies saved yet; the listener saves one every 5 minutes)")
        if self.count["changes before first full book (skipped)"]:
            out.append(f"  ({self.count['changes before first full book (skipped)']:,} changes arrived before that token's first full book and were skipped)")

        out.append("")
        out.append("CHECK 2 and 3: sanity and trades")
        names = [n for n in self.problem_count if n != "rebuilt book differs from Polymarket's best prices"]
        out.append("  all passed" if not names else "")
        for name in names:
            out.append(f"  {name}: {self.problem_count[name]:,}")

        out.append("")
        out.append("CHECK 4: gaps (times we were blind)")
        if not self.outages:
            out.append("  none")
        total = 0.0
        for start, end, who, reason in self.outages:
            if end:
                secs = (end - start) / 1e9
                total += secs
                out.append(f"  {when(start)}  {secs:,.1f}s  {who}: {reason}")
            else:
                out.append(f"  {when(start)}  open  {who}: {reason}")
        if total:
            out.append(f"  total blind time: {total:,.1f}s")

        if self.problems:
            out.append("")
            out.append("EXAMPLES OF PROBLEMS")
            for name, examples in self.problems.items():
                out.append(f"  {name} ({self.problem_count[name]:,}):")
                out.extend(f"    {e}" for e in examples)

        for name, skipped in BROKEN:
            self.problem("unreadable stretch in a saved file (data lost)", f"{name}: {skipped:,} compressed bytes in a damaged piece (data after the damage is lost)")
        if BROKEN:
            out.append("")
            out.append("UNREADABLE DATA")
            out.extend(f"  {name}: {skipped:,} compressed bytes in a damaged piece (data after the damage is lost)" for name, skipped in BROKEN)

        failed = sum(self.problem_count.values())
        out.append("")
        out.append("RESULT: " + ("PASS, no problems found" if not failed else f"{failed:,} problems found, see above"))
        return "\n".join(out)


def main():
    day = sys.argv[1] if len(sys.argv) > 1 else None
    checker = Checker()
    checker.run(raw_files(day))
    text = checker.report()
    print(text)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORT_DIR / f"{datetime.now(timezone.utc):%Y-%m-%dT%H%M%SZ}.txt"
    path.write_text(text + "\n")
    print(f"\nSaved: {path}")


if __name__ == "__main__":
    main()
