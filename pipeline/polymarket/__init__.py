"""Polymarket specifics.

    events      raw messages -> typed events, every field checked, exact decimals
    schemas     the clean tables' columns and the checks on our own output
    replay      the engine that replays raw lines, rebuilds books, checks and emits rows
    tables      builds the hourly Parquet tables (incremental, byte-identical rebuilds)
    validate    the data check report
    universe    which markets to watch: download, filter, rank, cap, save
    collector   the live collector (WebSocket + REST copies + re-picks)
    query       DuckDB views over the tables
"""
