"""Search the clean tables with DuckDB.

connect() makes one DuckDB view per table over every hourly Parquet file, plus a "markets" view
with the latest pick of every token. Prices and sizes are exact DECIMAL(18,6) columns (mid is
DECIMAL(18,7)); arithmetic on them stays exact in DuckDB.
"""
from __future__ import annotations

import duckdb

from ..config import Paths
from .schemas import HOURLY_TABLES


def _sql_path(path) -> str:
    return str(path).replace("'", "''")


def connect(paths: Paths) -> duckdb.DuckDBPyConnection:
    db = duckdb.connect()
    for name in HOURLY_TABLES:
        if any((paths.clean / name).glob("*/*/*.parquet")):
            files = _sql_path(paths.clean / name / "*" / "*" / "*.parquet")
            db.sql(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{files}', hive_partitioning = true)")
    markets = paths.clean / "markets" / "markets.parquet"
    if markets.exists():
        # latest pick per token (the same market shows up in every daily pick)
        db.sql(f"""CREATE VIEW markets AS SELECT * FROM read_parquet('{_sql_path(markets)}')
                   QUALIFY row_number() OVER (PARTITION BY token_id ORDER BY picked_at DESC) = 1""")
    return db


def tables(db) -> set[str]:
    return {row[0] for row in db.sql("SHOW TABLES").fetchall()}


def find_tokens(db, words: str) -> list[tuple]:
    if "markets" not in tables(db):
        return [(words, "?", "?")] if words.isdigit() else []
    if words.isdigit() and len(words) > 20:
        found = db.sql("SELECT token_id, outcome, question FROM markets WHERE token_id = ?", params=[words]).fetchall()
        return found or [(words, "?", "?")]
    return db.sql("""SELECT token_id, outcome, question FROM markets
                     WHERE question ILIKE ? ORDER BY rank, outcome_index""", params=[f"%{words}%"]).fetchall()


def timeline(db, token: str, limit: int = 15):
    """Every book change, full book and trade of one token, in time order."""
    have = tables(db)
    parts = []
    if "book_deltas" in have:
        parts.append("""SELECT exchange_ts, 'book change' AS what,
                               side || ' ' || CAST(price AS DOUBLE) || ' now ' || CAST(new_size AS DOUBLE) AS detail
                        FROM book_deltas WHERE token_id = $t""")
    if "book_snapshots" in have:
        parts.append("""SELECT exchange_ts, 'full book (' || source || ')', count(*) || ' levels'
                        FROM book_snapshots WHERE token_id = $t GROUP BY exchange_ts, source""")
    if "trades" in have:
        parts.append("""SELECT exchange_ts, 'trade', coalesce(taker_side, '?') || ' ' || CAST(size AS DOUBLE)
                               || ' @ ' || CAST(price AS DOUBLE)
                        FROM trades WHERE token_id = $t""")
    if not parts:
        return None
    union = "\nUNION ALL\n".join(parts)
    return db.sql(f"""WITH timeline AS ({union})
                      SELECT make_timestamp(exchange_ts * 1000) AS time_utc, what, detail
                      FROM timeline ORDER BY exchange_ts, what LIMIT {int(limit)}""", params={"t": token})


def price_per_minute(db, token: str, limit: int = 10):
    if "top_of_book" not in tables(db):
        return None
    return db.sql(f"""
        SELECT time_bucket(INTERVAL 1 MINUTE, make_timestamp(exchange_ts * 1000)) AS minute_utc,
               last(best_bid ORDER BY exchange_ts) AS best_bid, last(best_ask ORDER BY exchange_ts) AS best_ask,
               round(last(mid ORDER BY exchange_ts), 4) AS mid, round(last(spread ORDER BY exchange_ts), 4) AS spread,
               count(*) AS book_updates
        FROM top_of_book WHERE token_id = $t AND exchange_ts IS NOT NULL
        GROUP BY 1 ORDER BY 1 DESC LIMIT {int(limit)}""", params={"t": token})


def trade_summary(db, token: str):
    if "trades" not in tables(db):
        return None
    return db.sql("""SELECT count(*) AS trades, round(sum(size), 2) AS shares, round(sum(size * price), 2) AS dollars,
                            min(price) AS low, max(price) AS high,
                            count(*) FILTER (WHERE taker_side = 'buy') AS buyer_started,
                            count(*) FILTER (WHERE taker_side = 'sell') AS seller_started
                     FROM trades WHERE token_id = $t""", params={"t": token})
