"""The Polymarket data pipeline as an importable package (no side effects on import).

    pipeline.config        every path, URL and limit; data folder overridable with PM_DATA_DIR
    pipeline.logs          logging setup for the unattended scripts
    pipeline.http          HTTP requests with retries (429, 5xx, network errors)
    pipeline.raw_store     writes raw lines, exactly as received, to hourly files
    pipeline.raw_reader    reads them back in order, tolerating damage, in bounded memory
    pipeline.book          one order book with exact decimals, rewind and resync
    pipeline.polymarket    Polymarket specifics: message parsing, the replay engine, tables,
                           validation, market picking, the live collector, queries

The scripts in the project folder are thin command-line wrappers around these modules.
"""

__version__ = "0.2.0"
