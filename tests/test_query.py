"""The DuckDB query helpers and the query CLI, including when tables are missing."""
from datetime import UTC, datetime

import pytest

import query as query_cli
from pipeline.config import Paths
from pipeline.polymarket import query, tables
from tests.helpers import TOKEN_A, scripted_session

T = 1_793_750_400_000_000_000


def test_no_tables_yet(tmp_path, monkeypatch):
    paths = Paths(tmp_path / "it's here")  # a quote in the path must not break the SQL
    db = query.connect(paths)
    assert query.tables(db) == set()
    assert query.find_tokens(db, "house") == []
    assert query.find_tokens(db, TOKEN_A) == [(TOKEN_A, "?", "?")]
    assert query.timeline(db, TOKEN_A) is None
    assert query.price_per_minute(db, TOKEN_A) is None and query.trade_summary(db, TOKEN_A) is None
    monkeypatch.setenv("PM_DATA_DIR", str(paths.data))
    with pytest.raises(SystemExit):
        query_cli.main(["markets"])
    with pytest.raises(SystemExit):
        query_cli.main(["history", "anything"])
    with pytest.raises(SystemExit):
        query_cli.main(["bogus"])


def test_history_by_token_without_a_markets_table(tmp_path, monkeypatch, capsys):
    paths = Paths(tmp_path / "data")
    scripted_session(paths.raw, T + 60 * 1_000_000_000)
    tables.build(paths, now=datetime(2026, 11, 6, tzinfo=UTC))
    db = query.connect(paths)
    assert "markets" not in query.tables(db)
    rows = query.timeline(db, TOKEN_A, limit=500).fetchall()
    assert {r[1] for r in rows} >= {"book change", "full book (live)", "trade"}
    monkeypatch.setenv("PM_DATA_DIR", str(paths.data))
    query_cli.main(["history", TOKEN_A])
    assert "token " + TOKEN_A in capsys.readouterr().out
