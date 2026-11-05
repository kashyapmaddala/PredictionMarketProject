"""Search the clean Polymarket tables with DuckDB.

Run normalize_data.py first; it builds the tables this reads.

    python3 query_data.py markets                      # markets we watch, with their token IDs
    python3 query_data.py history "house"              # one market's full history (question words or a token ID)
    python3 query_data.py sql "SELECT count(*) FROM trades"   # any SQL you like

Tables you can use in SQL (all of them have token_id, and exchange_ts in milliseconds UTC):
  book_deltas, book_snapshots, trades, top_of_book, resyncs, collector_events,
  unsupported_events, markets
Prices and sizes are exact decimals (DECIMAL(18,6); mid is DECIMAL(18,7)).
"""
import sys

from pipeline.config import Paths
from pipeline.polymarket import query


def history(db, words):
    found = query.find_tokens(db, words)
    if not found:
        sys.exit(f"No market matching '{words}'. Try: python3 query_data.py markets")
    token, outcome, question = found[0]
    if len(found) > 1:
        print(f"{len(found)} tokens match; showing the first. Be more specific to pick another.")
    print(f"\n{question}  [{outcome}]\ntoken {token}\n")
    for title, result in (("Everything in one query (first 15 of the full timeline):", query.timeline(db, token)),
                          ("Price, one row per minute (from our rebuilt book):", query.price_per_minute(db, token)),
                          ("Trades:", query.trade_summary(db, token))):
        if result is not None:
            print(title)
            result.show(max_width=200)


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if not argv or argv[0] not in ("markets", "history", "sql"):
        sys.exit(__doc__)
    db = query.connect(Paths.from_env())
    command = argv[0]
    if command == "markets":
        if "markets" not in query.tables(db):
            sys.exit("No markets table yet. Run normalize_data.py first.")
        db.sql("""SELECT rank, outcome, left(question, 70) AS question, token_id
                  FROM markets ORDER BY rank, outcome_index""").show(max_rows=400, max_width=250)
    elif command == "history":
        history(db, " ".join(argv[1:]))
    else:
        db.sql(" ".join(argv[1:])).show(max_width=250)


if __name__ == "__main__":
    main()
