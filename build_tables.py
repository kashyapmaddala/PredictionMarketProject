"""Turn the raw Polymarket files into clean tables.

Reads data/raw/polymarket/ (exactly what the exchange sent) and writes Parquet tables to
data/clean/polymarket/<table>/date=YYYY-MM-DD/hour=HH/part.parquet, one file per UTC hour.
DuckDB (see query_data.py) can then search all of them at once.

Tables:
  book_deltas        every change to a book: one row per price level that changed
  book_snapshots     every full book: from the live feed ("live") and from the 5-minute REST
                     copies ("rest"), one row per price level
  trades             every trade: price, size, which side started it, fee, transaction hash
  top_of_book        our rebuilt book after every batch of changes: best buy/sell, their sizes,
                     mid price and spread
  resyncs            every time a REST copy (or an out-of-order full book) disagreed with our book
                     and the book was corrected to it (our later changes replayed on top)
  collector_events   the collector's own notes: started, connected, disconnected, paused, stopped...
  unsupported_events every message (or part) we could not use, exactly as received, with why
  markets            (one file, not per hour) every market we picked, from every pick

Prices and sizes are exact decimals, never floats. Every row has normalizer_version (now 2) and
says which raw line it came from (source_file, source_line). The raw files are never changed:
fixing a bug here means re-running this. Re-running gives byte-identical files.

    python3 build_tables.py              # build every finished hour not built yet (or changed)
    python3 build_tables.py --rebuild    # rebuild everything from scratch
    python3 build_tables.py --current    # also build the hour still being written (partial)
"""
import logging
import sys

from pipeline import logs
from pipeline.config import Paths
from pipeline.polymarket.tables import build

log = logging.getLogger("build_tables")


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    unknown = [a for a in argv if a not in ("--rebuild", "--current")]
    if unknown:
        sys.exit(__doc__)
    logs.setup()
    paths = Paths.from_env()
    summary = build(paths, rebuild="--rebuild" in argv, include_current="--current" in argv)
    for problem in summary["damage"]:
        log.warning("raw file problem: %s", problem)
    log.info("Built %d hour(s) (%d already up to date) in %.0fs. Markets table: %s rows. Tables are in %s",
             len(summary["built"]), summary["skipped"], summary["seconds"], f"{summary['markets']:,}", paths.clean)


if __name__ == "__main__":
    main()
