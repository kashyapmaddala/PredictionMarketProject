"""Search the clean Polymarket tables with DuckDB (step 5).

Run normalize.py first; it builds the tables this reads.

    python3 query.py markets                      # markets we watch, with their token IDs
    python3 query.py history "house"              # one market's full history (question words or a token ID)
    python3 query.py sql "SELECT count(*) FROM trades"   # any SQL you like

Tables you can use in SQL (all of them have token_id, and exchange_ts in milliseconds UTC):
  book_deltas, book_snapshots, trades, top_of_book, resyncs, collector_events, markets
"""
import sys
from pathlib import Path

import duckdb

HERE = Path(__file__).resolve().parent
CLEAN_DIR = HERE / "data" / "clean" / "polymarket"
HOURLY_TABLES = ["book_deltas", "book_snapshots", "trades", "top_of_book", "resyncs", "collector_events"]


def connect():
    db = duckdb.connect()
    for name in HOURLY_TABLES:
        files = CLEAN_DIR / name / "*" / "*" / "*.parquet"
        if list((CLEAN_DIR / name).glob("*/*/*.parquet")):
            db.sql(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{files}', hive_partitioning = true)")
    markets = CLEAN_DIR / "markets" / "markets.parquet"
    if markets.exists():
        # latest pick per token (the same market shows up in every daily pick)
        db.sql(f"""CREATE VIEW markets AS SELECT * FROM read_parquet('{markets}')
                   QUALIFY row_number() OVER (PARTITION BY token_id ORDER BY picked_at DESC) = 1""")
    return db


def find_tokens(db, words):
    if words.isdigit() and len(words) > 20:
        return db.sql("SELECT token_id, outcome, question FROM markets WHERE token_id = ?", params=[words]).fetchall() or [(words, "?", "?")]
    return db.sql("""SELECT token_id, outcome, question FROM markets
                     WHERE question ILIKE ? ORDER BY rank, outcome_index""", params=[f"%{words}%"]).fetchall()


def history(db, words):
    found = find_tokens(db, words)
    if not found:
        sys.exit(f"No market matching '{words}'. Try: python3 query.py markets")
    token, outcome, question = found[0]
    if len(found) > 1:
        print(f"{len(found)} tokens match; showing the first. Be more specific to pick another.")
    print(f"\n{question}  [{outcome}]\ntoken {token}\n")

    # The whole story of one contract, in one query: every book update, full book and trade, in time order
    print("Everything in one query (first 15 of the full timeline):")
    db.sql("""
        WITH timeline AS (
            SELECT exchange_ts, 'book change' AS what, side || ' ' || price || ' now ' || new_size AS detail
              FROM book_deltas WHERE token_id = $t
            UNION ALL
            SELECT exchange_ts, 'full book (' || source || ')', count(*) || ' levels'
              FROM book_snapshots WHERE token_id = $t GROUP BY exchange_ts, source
            UNION ALL
            SELECT exchange_ts, 'trade', taker_side || ' ' || size || ' @ ' || price
              FROM trades WHERE token_id = $t)
        SELECT make_timestamp(exchange_ts * 1000) AS time_utc, what, detail
        FROM timeline ORDER BY exchange_ts LIMIT 15""", params={"t": token}).show(max_width=200)

    print("Price, one row per minute (from our rebuilt book):")
    db.sql("""
        SELECT time_bucket(INTERVAL 1 MINUTE, make_timestamp(exchange_ts * 1000)) AS minute_utc,
               last(best_bid ORDER BY exchange_ts) AS best_bid, last(best_ask ORDER BY exchange_ts) AS best_ask,
               round(last(mid ORDER BY exchange_ts), 4) AS mid, round(last(spread ORDER BY exchange_ts), 4) AS spread,
               count(*) AS book_updates
        FROM top_of_book WHERE token_id = $t GROUP BY 1 ORDER BY 1 DESC LIMIT 10""", params={"t": token}).show()

    if "trades" in [r[0] for r in db.sql("SHOW TABLES").fetchall()]:
        print("Trades:")
        db.sql("""SELECT count(*) AS trades, round(sum(size), 2) AS shares, round(sum(size * price), 2) AS dollars,
                         round(min(price), 4) AS low, round(max(price), 4) AS high,
                         count(*) FILTER (WHERE taker_side = 'buy') AS buyer_started,
                         count(*) FILTER (WHERE taker_side = 'sell') AS seller_started
                  FROM trades WHERE token_id = $t""", params={"t": token}).show()


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ("markets", "history", "sql"):
        sys.exit(__doc__)
    db = connect()
    command = sys.argv[1]
    if command == "markets":
        db.sql("""SELECT rank, outcome, left(question, 70) AS question, token_id
                  FROM markets ORDER BY rank, outcome_index""").show(max_rows=400, max_width=250)
    elif command == "history":
        history(db, " ".join(sys.argv[2:]))
    else:
        db.sql(" ".join(sys.argv[2:])).show(max_width=250)


if __name__ == "__main__":
    main()
