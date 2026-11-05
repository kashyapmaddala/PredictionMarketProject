"""End to end on a temporary data folder: collect (fake server) -> validate -> build -> query."""
import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal as D

import query_data as query_cli
from pipeline.config import Paths
from pipeline.polymarket import query, tables
from pipeline.polymarket.collector import Collector, run_until_stopped
from pipeline.polymarket.validate import validate
from pipeline.raw_store import RawStore
from tests.fake_polymarket import FakePolymarket
from tests.test_collector import TOKENS, settings


def test_collect_validate_build_query(tmp_path, monkeypatch, capsys):
    paths = Paths(tmp_path / "data")
    paths.universe.mkdir(parents=True)
    (paths.universe / "2026-11-05T000000Z.json").write_text(
        '{"picked_at": "2026-11-05T00:00:00+00:00", "markets": [{"rank": 1, "market_id": "1", "condition_id": "0xm",'
        ' "question": "Will the test pass?", "slug": "t", "event_id": "e", "event_slug": "e", "event_title": "E",'
        f' "outcomes": [{{"name": "Yes", "token_id": "{TOKENS[0]}"}}, {{"name": "No", "token_id": "{TOKENS[1]}"}}],'
        ' "tick_size": "0.01", "min_order_size": "5", "neg_risk": false, "end_date": null, "volume_24h": 10, "liquidity": 5}]}')

    async def collect():
        async with FakePolymarket("abort", abort_after=8, tick=0.03) as fake:
            store = RawStore(paths.raw, compress=True, session="e2e")
            c = Collector(store, settings(fake.url), paths=paths)
            job = asyncio.ensure_future(run_until_stopped(c, c.run({TOKENS[0]: "Yes", TOKENS[1]: "No"})))
            await asyncio.sleep(1.2)
            job.cancel()
            await asyncio.gather(job, return_exceptions=True)
            c.save("stopped", {"reason": "test over"})
            store.close()
            return c

    c = asyncio.run(collect())
    assert c.drops == 1 and c.saved > 20

    report = validate(paths)
    assert not report.failed, report.text
    assert "no close frame" in report.text

    summary = tables.build(paths, include_current=True, now=datetime.now(UTC) + timedelta(hours=3))
    built = summary["built"] + summary["partial"]
    assert built and summary["markets"] == 2

    db = query.connect(paths)
    assert {"book_deltas", "book_snapshots", "top_of_book", "collector_events", "markets"} <= query.tables(db)
    deltas = db.sql("SELECT count(*), count(DISTINCT conn) FROM book_deltas").fetchall()[0]
    assert deltas[0] > 20 and deltas[1] == 2  # both connections, before and after the drop
    ((bid, ask, mid),) = db.sql("SELECT best_bid, best_ask, mid FROM top_of_book ORDER BY recv_ts_ns DESC LIMIT 1").fetchall()
    assert (bid, ask, mid) == (D("0.400000"), D("0.600000"), D("0.5000000"))
    assert query.find_tokens(db, "test pass")[0][0] == TOKENS[0]

    monkeypatch.setenv("PM_DATA_DIR", str(paths.data))
    query_cli.main(["markets"])
    query_cli.main(["history", "test pass"])
    query_cli.main(["sql", "SELECT count(*) AS n FROM collector_events"])
    out = capsys.readouterr().out
    assert "Will the test pass?" in out and "full book (live)" in out and "Price, one row per minute" in out
