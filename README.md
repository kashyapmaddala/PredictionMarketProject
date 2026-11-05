# Prediction Market Data Pipeline

[![CI](https://github.com/kashyapmaddala/PredictionMarketProject/actions/workflows/ci.yml/badge.svg)](https://github.com/kashyapmaddala/PredictionMarketProject/actions/workflows/ci.yml)

A BoilerQuant (Purdue) project that records live order book data from prediction markets, proves the data matches the exchange, and turns it into clean, exactly typed tables you can query with SQL. It collects every message from Polymarket's public market feed for the most traded markets, stores it untouched, rebuilds every order book from it, checks those books against the exchange's own numbers, and writes versioned Parquet tables that DuckDB can search. It is the foundation for the project's research (volatility, liquidity and informed-flow measurement). Polymarket is supported today; Kalshi comes next. The project plan, team and research directions are in [PROJECT.md](PROJECT.md).

## Status

| Area | State |
|---|---|
| Polymarket collection, validation, clean tables (steps 1 to 5) | Built and validated on live data (November 2026) |
| Hardening: shared `pipeline/` package, exact decimals, 172 tests (96% branch coverage), CI (Step H) | Done (November 5, 2026) |
| 24/7 server, scheduled jobs, alerts, backups (step 6) | **Next.** Collection is stopped until the server exists |
| Outcomes, historical backfill, scale-up, two clean weeks (steps 7 to 9) | Not started |
| Kalshi | Not started. Blocked on written permission to store data and an API key |

Full step tables and the plan are in [PROJECT.md](PROJECT.md#level-1-the-data-pipeline).

## Contents

- [What it does and guarantees](#what-it-does-and-guarantees)
- [Architecture](#architecture)
- [Repository layout](#repository-layout)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Data model](#data-model)
- [Data quality and validation](#data-quality-and-validation)
- [Operations](#operations)
- [Failure modes and recovery](#failure-modes-and-recovery)
- [Testing](#testing)
- [Development workflow](#development-workflow)
- [Roadmap](#roadmap)
- [Troubleshooting and FAQ](#troubleshooting-and-faq)
- [Team and contact](#team-and-contact)
- [License](#license)
- [Acknowledgements](#acknowledgements)

## What it does and guarantees

**Features**

- Picks a universe of markets: every open Polymarket market is pulled, filtered to ones tradeable on the order book, ranked by 24 hour volume, capped at 5 per event, top 100 by default. Every pick is kept, so we always know what was watched when.
- Collects live data for every token in the universe over WebSocket, sharded 400 tokens per connection: immediate reconnect with fresh books, connections replaced before they age (make before break), sleep and freeze detection, an optional hot standby, and a daily re-pick.
- Saves an independent REST copy of every book every 5 minutes, used only to check our rebuilt books.
- Rebuilds every order book from the saved messages and checks it against the exchange (see [Data quality](#data-quality-and-validation)).
- Builds clean Parquet tables per UTC hour (book changes, full books, trades, top of book, resyncs, collector events, unsupported events, markets) with exact decimal prices and sizes.
- Answers "give me this contract's full history" in one DuckDB query.

**Guarantees.** Each one is enforced in code and covered by tests.

1. **Raw is append-only and never edited.** Every message is saved exactly as the exchange sent it. Fixing a bug means re-running downstream code, never patching raw files.
2. **Single writer.** A lock file stops a second collector from starting. (Two writers once corrupted part of an hour of data.)
3. **Crash safety.** At most about one second of data is lost in a crash (the gzip flush interval). A crash is detectable, because no `stopped` note is written, and it is reported as a gap.
4. **Deterministic, idempotent builds.** The same raw bytes and the same code version produce byte-identical Parquet files. Hours are rewritten, never appended to.
5. **Versioned output.** Every clean row records `normalizer_version`. Any change to output bumps it.
6. **Exact numbers.** Prices and sizes are exact decimals end to end, never floats.
7. **No silent loss.** Every disconnect, crash, damaged file stretch, REST disagreement and unrecognized message shows up in a table or a report.
8. **Point-in-time.** Nothing computed for time $t$ may use data from after $t$. The `markets` table keeps every pick with its `picked_at` time so metadata can be joined as it was known then.

## Architecture

```
                 Gamma API (all open markets)
                          |
                          |  pick_markets.py: keep tradeable, rank by 24h volume, max 5 per event
                          v
   UNIVERSE   <data>/universe/polymarket/<UTC time>.json  +  current.json   (history kept)
                          |
                          v
   CLOB WebSocket ---> collect_live.py ---------------------> RAW  <data>/raw/polymarket/<day>/<HH>.jsonl.gz
   (book, price_change,    | 400 tokens per connection          append-only, exactly as received,
    last_trade_price,      | reconnect + fresh books            plus collector notes
    tick_size_change)      | REST /books every 5 min  ------->  (same raw files, kind "rest_books")
                           | re-pick every 24h
                           v
                 validate_data.py   replay books, compare to exchange, find gaps  ---> REPORTS <data>/reports/
                           |
                 normalize_data.py  deterministic, versioned, hourly book checkpoints
                           v
   CLEAN      <data>/clean/polymarket/<table>/date=YYYY-MM-DD/hour=HH/part.parquet
                           |
                 query_data.py      DuckDB views over all Parquet  --->  research (Level 2)
```

`<data>` is the data root (`./data` by default, see [Configuration](#configuration)).

**Layers.** Each layer reads only the layer before it and can be rebuilt from it. Raw and universe history are the only layers that cannot be rebuilt.

| Layer | Location | Rebuildable? | Written by |
|---|---|---|---|
| Raw | `<data>/raw/<venue>/` | **No.** Back it up | `collect_live.py`, `pick_markets.py` (market pages) |
| Universe | `<data>/universe/<venue>/` | No (record of what we watched when) | `pick_markets.py` |
| Clean | `<data>/clean/<venue>/` | Yes, from raw | `normalize_data.py` |
| Reports | `<data>/reports/<venue>/` | Yes, from raw | `validate_data.py` |
| Features, research (future) | not built yet | Yes, from clean | Level 2 |

**Process model.** One long-running async process (`collect_live.py`) per venue does all network I/O: one WebSocket task per shard of 400 tokens, a REST snapshot task, a status task, and a background re-pick that runs `pick_markets.py` as a subprocess every 24 hours. Batch jobs (`validate_data.py`, `normalize_data.py`, `query_data.py`) are separate processes that only read files, so they can run any time, or on a schedule, without touching collection. All shared logic (config, raw I/O, the order book, Polymarket parsing and schemas) lives in the importable `pipeline/` package; the root scripts only handle arguments, files and the network.

## Repository layout

```
.
├── pick_markets.py              CLI: choose the universe
├── collect_live.py              CLI: the live collector (was listener.py)
├── validate_data.py             CLI: rebuild books and check them (was check_data.py)
├── normalize_data.py            CLI: raw files into clean Parquet tables (was normalize.py)
├── query_data.py                CLI: DuckDB search over clean tables (was query.py)
├── pipeline/                    importable package, no side effects on import
│   ├── config.py                every path, URL and limit in one place; PM_DATA_DIR override
│   ├── logs.py                  logging with UTC timestamps and levels
│   ├── http.py                  HTTP with retries on 429, 5xx and network errors (honours Retry-After)
│   ├── raw_store.py             append-only raw writer (partitioned, gzip, part files, exclusive create)
│   ├── raw_reader.py            streaming, damage-tolerant raw reader with bounded memory
│   ├── book.py                  order book in exact Decimal: apply, history, rewind, resync, save/load
│   └── polymarket/
│       ├── events.py            exchange frames into typed, checked events
│       ├── replay.py            the one replay engine shared by validation and table building
│       ├── validate.py          checks and the report
│       ├── tables.py            hourly incremental builds, byte-identical rebuilds
│       ├── schemas.py           table schemas (exact decimals) and checks on our own output
│       ├── universe.py          market filters, ranking, per-event cap
│       ├── collector.py         the live collector
│       └── query.py             DuckDB views
├── tests/                       pytest suite, fake Polymarket server, real-data fixtures
├── pyproject.toml               ruff and pytest configuration
├── requirements.txt             pinned runtime dependencies
├── requirements-dev.txt         pinned test and lint dependencies
├── .github/workflows/ci.yml     ruff + pytest on Python 3.11 and 3.13
├── PROJECT.md                   project plan, team, research directions
├── LICENSE
└── README.md
```

The data root (`data/` by default) is created at run time and is never committed.

## Requirements

- Python 3.11 or newer (CI runs 3.11 and 3.13).
- macOS or Linux. The collector's single-writer lock uses `fcntl`, which Windows does not have.
- A machine that stays awake. A sleeping computer collects nothing (see [Operations](#operations)).
- A steady internet connection. Collecting 100 markets is about 800 messages a second.
- Disk: about 0.26 GB per hour of compressed raw data for 100 markets (about 6 GB a day). Plan for 100 GB or more for a two week run.
- Runtime libraries (pinned in `requirements.txt`): `websockets`, `httpx`, `certifi`, `pyarrow`, `duckdb`.
- **No Polymarket account or API key.** Everything used is public market data, limited only by IP rate limits.
- Kalshi (not built yet) will need written permission from Kalshi to store their data, plus an account and API key.

## Installation

```sh
git clone https://github.com/kashyapmaddala/PredictionMarketProject.git
cd PredictionMarketProject
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt          # to run the pipeline
pip install -r requirements-dev.txt      # to run tests and lint
```

Check it works without touching the network:

```sh
pytest
```

## Configuration

All paths, URLs and limits are defined once in `pipeline/config.py`. Environment variables override the data root (`PM_DATA_DIR`) and, for tests against a fake server, the two collector endpoints (`PM_LIVE_URL`, `PM_BOOKS_URL`).

| Setting | Default | How to change |
|---|---|---|
| Data root | `./data` (repo root) | Environment variable `PM_DATA_DIR`, for example `export PM_DATA_DIR=/mnt/pm-data` |
| Markets to watch | 100 | Argument to `pick_markets.py` (for example `pick_markets.py 250`) |
| Max markets per event | 5 | `MAX_PER_EVENT` |
| Tokens per WebSocket connection | 400 (Polymarket allows 500) | `MAX_TOKENS_PER_CONNECTION` |
| Silence before a connection counts as dead | 30 s (we send `PING` every 10 s) | `SILENCE_LIMIT`, `APP_PING_EVERY` |
| Reconnect | First retry immediate, then doubling waits capped at 60 s | `RECONNECT_MIN_WAIT`, `RECONNECT_MAX_WAIT` |
| Connection rotation | Each connection is replaced every 600 s by a new one that overlaps it for 5 s (0 turns it off) | `ROTATE_EVERY`, `OVERLAP_SECONDS` |
| HTTP retries | 6 attempts on 429, 5xx and network errors, backoff up to 60 s | `HTTP_RETRIES`, `HTTP_BACKOFF_MAX` |
| REST book copy interval | 300 s, 200 tokens per request | `SNAPSHOT_EVERY`, `SNAPSHOT_BATCH` |
| Re-pick interval | 24 h | `REPICK_EVERY_HOURS` |
| Status line interval | 60 s | `STATUS_EVERY` |

Endpoints used (Polymarket, public):

| Purpose | Endpoint |
|---|---|
| All open markets | `https://gamma-api.polymarket.com/markets/keyset` (v1 and v2 markets) |
| One event by slug | `https://gamma-api.polymarket.com/events` |
| Live market feed | `wss://ws-subscriptions-clob.polymarket.com/ws/market` |
| Book copies | `POST https://clob.polymarket.com/books` |

Paths under the data root:

| What | Path |
|---|---|
| Raw live data | `raw/polymarket/<YYYY-MM-DD>/<HH>.jsonl.gz` (UTC; `<HH>.part2.jsonl.gz` and so on after a restart in the same hour) |
| Raw market list pages | `raw/polymarket_markets/<YYYY-MM-DD>/<HH>.jsonl.gz` |
| Universe | `universe/polymarket/<UTC time>.json`, `universe/polymarket/current.json` |
| Clean tables | `clean/polymarket/<table>/date=<YYYY-MM-DD>/hour=<HH>/part.parquet`, `clean/polymarket/markets/markets.parquet` |
| Book checkpoints | `clean/polymarket/_state/<YYYY-MM-DD>T<HH>.json` |
| Validation reports | `reports/polymarket/<UTC time>.txt` |
| Collector log | `logs/collector.log` (when started as shown in [Operations](#operations); the program logs to stdout) |
| Re-pick log | `logs/pick_markets.log` |
| Lock file | `logs/collector.lock` (holds the PID) |

There are no secrets today. When Kalshi is added, its API key goes in an environment variable or an untracked `.env` file, never in git.

## Usage

Run every command from the repo root with the virtual environment active.

### 1. Pick the markets: `pick_markets.py`

```sh
python3 pick_markets.py          # top 100 by 24h volume, max 5 per event
python3 pick_markets.py 250      # top 250
```

Pulls every open market (about 234,000, about 10 minutes, about 95 MB compressed per pull), saves each raw page, keeps markets that are tradeable on the order book (order book enabled, accepting orders, not closed, two outcome tokens), ranks by 24 hour volume and applies the per-event cap. Prints one line per page, then writes the new universe file and `current.json`. You only need to run it once: the collector re-picks every 24 hours, and runs it for you if no universe exists.

### 2. Collect live data: `collect_live.py`

```sh
python3 collect_live.py                    # every market in current.json
python3 collect_live.py --verbose          # also print every change in plain English
python3 collect_live.py --standby          # two connections per group: no gap on a drop, twice the disk
python3 collect_live.py <event-slug>       # only the markets in one event (slug from polymarket.com/event/<slug>)
```

Runs until stopped. Ctrl+C or `SIGTERM` stops it cleanly and writes a `stopped` note. Once a minute it logs a status line:

```
2026-11-05 17:36:05Z INFO collector: 1/1 connections up | 7,612 messages saved (3,593 in the last minute) | drops so far: 0 | ...
```

With `--verbose` it prints lines like:

```
FULL BOARD  Yes        | Will the Democratic Party control the House...: best buy 0.75, best sell 0.76
UPDATE      Yes        | Will the Democratic Party control the House...: buyers at 0.74 now total 4604.5
TRADE       Yes        | Will the Democratic Party control the House...: 120 traded at 0.75
```

FULL BOARD is the whole book ("best buy" is the most anyone will pay, "best sell" the least anyone will sell for). UPDATE is one price level changing ("now total" is the shares resting there; 0 means none left). TRADE is an actual trade.

For unattended runs see [Operations](#operations).

### 3. Validate: `validate_data.py`

```sh
python3 validate_data.py               # everything collected so far
python3 validate_data.py 2026-11-03    # one UTC day (starts from the table builder's saved books for the hour before, if built)
```

Read-only: it never changes raw files. Prints a report, saves it under `reports/polymarket/`, and ends with `RESULT: PASS, no problems found` or a count of problems with up to five examples of each. Exit code 0 on pass and 1 on problems, so a scheduler can alert on it. Streaming with bounded memory: all 63 hours (22 million messages) take about 9 minutes in under 190 MB.

### 4. Normalize into clean tables: `normalize_data.py`

```sh
python3 normalize_data.py              # every finished UTC hour not built yet
python3 normalize_data.py --rebuild    # delete clean tables and rebuild everything from raw
python3 normalize_data.py --current    # also build the hour still being written (rebuilt again next run)
```

It logs one line per hour built, then a summary such as `Built 1 hour(s) (46 already up to date) in 20s. Markets table: ... rows.`

Book state carries from one hour to the next through the checkpoint files, so building one new hour does not replay the whole history. A full rebuild of 47 hours (22 million messages) takes about 15 minutes; one hour takes seconds to about 20 s.

### 5. Query: `query_data.py`

```sh
python3 query_data.py markets                                   # markets we watch, with token IDs
python3 query_data.py history "control the House"               # one contract's full history
python3 query_data.py sql "SELECT count(*) FROM trades"         # any SQL over the clean tables
```

`history` takes words from the question (case-insensitive) or a token ID. It prints the first rows of the full timeline (book changes, full books and trades in one query), the price per minute from our rebuilt book, and a trade summary (count, shares, dollars, low, high, buyer-started and seller-started).

Example SQL:

```sql
-- best bid and ask per minute for one token
SELECT time_bucket(INTERVAL 1 MINUTE, make_timestamp(exchange_ts * 1000)) AS minute_utc,
       last(best_bid ORDER BY exchange_ts) AS best_bid,
       last(best_ask ORDER BY exchange_ts) AS best_ask
FROM top_of_book
WHERE token_id = '<token id>'
GROUP BY 1 ORDER BY 1;
```

## Data model

### Identity, units and time

| Concept | Meaning |
|---|---|
| `token_id` | One outcome's order book. YES and NO (or two team names) are separate tokens with separate books |
| `market_id` | Polymarket condition ID, shared by a market's two tokens. Gamma's numeric ID is `gamma_market_id` |
| `event_id` | Group of related markets (for example every candidate in one election) |
| Price | Probability in $[0, 1]$, in dollars per share. A share pays \$1 if its outcome happens |
| Size | Shares |
| `exchange_ts` | Exchange time, **milliseconds** since 1970 UTC. In DuckDB: `make_timestamp(exchange_ts * 1000)` |
| `recv_ts_ns` | Our receive time, **nanoseconds** since 1970 UTC. Always kept next to `exchange_ts` |
| Partitions | `date=` and `hour=` are the UTC hour the message was **received**, matching the raw file it came from |

**Ordering rule.** Never sort by `exchange_ts` alone. Polymarket sends many changes in one batch under a single timestamp, and REST copies can arrive after later live updates. Replay order is: raw file order (hour, then part), then line order, then position inside the message. Clean tables are written in that order, and every row carries `source_file`, `source_line` (and `change_index` in `book_deltas`), so the exact capture order can always be recovered: `ORDER BY source_file, source_line, change_index`.

### Raw envelope

Every raw line is one JSON object:

```json
{"recv_ts_ns": 1793664000123456789, "kind": "message", "session": "a1b2c3d4e5f6", "conn": 3, "raw": "<exact text the exchange sent>"}
```

| Field | Meaning |
|---|---|
| `recv_ts_ns` | When our machine received it (ns, UTC) |
| `kind` | What the line is (table below) |
| `raw` | For exchange data: the exact text received, never parsed before saving. For collector notes: a small JSON object |
| `session` | Random id, one per collector run (added November 5, 2026) |
| `conn` | Which WebSocket connection in that run (1, 2, 3 ..., never reused in a run) |

`session` and `conn` are **backward compatible**: lines written before November 5 do not have them, and readers treat them as missing.

| `kind` | Meaning |
|---|---|
| `message` | WebSocket frame from the exchange, untouched |
| `rest_books` | Body of a `POST /books` reply (independent book copy, every 5 minutes) |
| `rest_books_failed` | A book copy request failed (reason saved) |
| `gamma_markets_page` | One page of the full market list (in `raw/polymarket_markets/`) |
| `connected`, `subscribed`, `disconnected` | Connection lifecycle, with connection number and reason |
| `universe_changed` | The daily re-pick switched the watched set |
| `started`, `retired` | Collector run started; a connection was replaced on purpose (rotation, standby takeover) |
| `paused` | The machine slept or the process froze; how long, and that every connection was cut and reopened |
| `message_binary` | A binary frame (saved as base64 so nothing is lost) |
| `stopped` | Clean shutdown. A restart with no `stopped` before it means a crash |

### Clean tables

Hourly tables live at `clean/polymarket/<table>/date=YYYY-MM-DD/hour=HH/part.parquet` (zstd). Every row carries `venue` and `normalizer_version` (currently **2**). Prices and sizes are `decimal128(18, 6)` and `mid` is `decimal128(18, 7)`. Scale 6 is the protocol limit (USDC and outcome tokens have 6 decimals on chain); a scan of all 22 million messages found at most 3 decimals in prices and 6 in trade sizes. A value that does not fit is never rounded: its event goes to `unsupported_events`. Every hourly table also ends with `conn`, `source_file` and `source_line`, which point to the raw line each row came from.

| Table | Grain (one row per) | Key columns |
|---|---|---|
| `book_deltas` | price level that changed | `exchange_ts`, `recv_ts_ns`, `token_id`, `market_id`, `side`, `price`, `new_size` (absolute size now resting; 0 removes the level), `reported_best_bid`, `reported_best_ask` (what Polymarket says the best prices are after the change), `hash`, `change_index` (position inside the message) |
| `book_snapshots` | price level in a full book | `snapshot_id`, `source` (`live` from the feed, `rest` from the 5 minute copy), `exchange_ts`, `recv_ts_ns`, `token_id`, `market_id`, `side`, `price`, `size`, `tick_size`, `hash` |
| `trades` | trade | `exchange_ts`, `recv_ts_ns`, `token_id`, `market_id`, `price`, `size`, `taker_side` (`buy` or `sell`, the side that started the trade, as reported by the exchange), `fee_rate_bps`, `transaction_hash` |
| `top_of_book` | moment our rebuilt book changed (after each batch) | `exchange_ts`, `recv_ts_ns`, `token_id`, `best_bid`, `best_bid_size`, `best_ask`, `best_ask_size`, `mid`, `spread`, `bid_levels`, `ask_levels`, `cause` (`change`, `full_book`, `resync`) |
| `resyncs` | correction from a REST copy that disagreed with our book | `exchange_ts`, `recv_ts_ns`, `token_id`, `levels_wrong`, `example`, `source` |
| `collector_events` | collector note | `recv_ts_ns`, `kind`, `details`, `session` |
| `unsupported_events` | exchange message or event type the builder does not parse yet | `recv_ts_ns`, `envelope_kind`, `event_type`, `reason`, `payload` (the raw text), `session` |
| `markets` (one file, not hourly) | (token, pick) | `picked_at`, `rank`, `token_id`, `outcome`, `outcome_index`, `market_id`, `gamma_market_id`, `question`, `slug`, `event_id`, `event_slug`, `event_title`, `tick_size`, `min_order_size`, `neg_risk`, `end_date`, `volume_24h`, `liquidity` |

Notes:

- `query_data.py` exposes `markets` as the **latest** pick per token. That is fine for lookup. For research, join on `picked_at` so you only use what was known at the time.
- Book state between hours is stored in `clean/polymarket/_state/`. It is a build artifact, not a table.

### Changing a contract

The raw envelope and the clean schemas are interfaces. Changing one is a breaking change: the PR must say so, bump `NORMALIZER_VERSION` for clean tables, update this section, and rebuild with `normalize_data.py --rebuild`. Raw changes must stay backward compatible, because raw files are never rewritten.

## Data quality and validation

`validate_data.py` replays every book from raw and runs these checks:

| Check | What must hold |
|---|---|
| Live consistency | After each same-timestamp batch, our best bid and ask equal the `best_bid` and `best_ask` Polymarket reports in `price_change` |
| REST reconciliation | Every 5 minutes, our full book (every level, price and size), rewound to the copy's exact exchange timestamp, equals the REST copy |
| Sanity | Book not crossed (best bid below best ask), prices in $[0, 1]$ and on the market's tick, sizes positive |
| Trades | Trade price in $[0, 1]$ |
| Gaps | Every disconnect, stop and crash restart, with how long we were blind |
| Damage | Unreadable gzip stretches located and measured |

**Results so far.**

| Run | Live consistency | REST reconciliation |
|---|---|---|
| Nov 3, 2026 (v1 code) | 644,154 of 644,154 updates matched (100%) | 1,530 of 1,560 copies matched exactly (98%) |
| Nov 5, 2026 (v1 code) | 677,466 of 677,478 updates matched (99.998%) | 618 of 620 copies matched exactly |
| Nov 5, 2026, all 63 hours, 22 million messages (v2 code) | 99.9997% of change batches matched | 99.97% of copies matched |
| Nov 5, 2026, 20 minute live run (v2 code) | 201,562 of 201,562 batches (100%) | 372 of 372 (100%) |

Sanity and trade checks passed. The gap check found every outage, including a crash restart with no `stopped` note. One damaged file was found and measured (Nov 3, 19:00 UTC, from a killed collector and a second copy writing to the same file; this is why the lock file and part files exist).

**Known discrepancies.**

- Most of the Nov 3 REST misses are one market (the 2028 Democratic nomination market for Adam Hamilton), where the REST copy shows 11 resting buy levels that the live feed never sent. This is an inconsistency on Polymarket's side. Policy: log and resync. `normalize_data.py` corrects the book to the REST copy, replays our later changes on top, and records it in `resyncs`. After one resync the market matched.
- The 12 Nov 5 mismatches (02:36:46 UTC) were **our bug**: a full book stamped 5 ms older than a change we had already applied overwrote it. Now an older full book is checked against our book at its own moment instead. All 86 such books in the data match.
- REST copies are stamped a few milliseconds off the live feed, and several changes can share one millisecond. A copy now matches if it equals any state of our book within 10 ms. The old code "corrected" correct books in these cases (149 resyncs in v1, 5 in v2 for the same hours).
- The remaining 136 best-price mismatches across all data are 62 episodes (YES and NO token each) that all clear by themselves on later updates. The ones traced are Polymarket reporting a removed level as the best price. The feed has no sequence numbers, so a missed message cannot be ruled out completely. 15 REST mismatches (0.03%) were corrected by resync and not traced one by one.

**How resync works.** When a REST copy disagrees, the book becomes the copy plus every change we received after the copy's timestamp. The correction is written to `resyncs` and a `top_of_book` row with `cause = resync`.

**Not checked yet** (later Level 1 work): tick size changes (216 seen, no table yet), trade prices against the book at that moment, on-chain fill reconciliation, spot checks against the Polymarket-v1 dataset, exchange-to-receive lag percentiles, and completeness against expected trade counts.

## Operations

Collection belongs on an always-on server (step 6, not set up yet). **A sleeping computer collects nothing.** The 89 "drops" seen on Nov 3 to 5 were almost all a Mac sleeping with the lid closed (`caffeinate -i` does not stop lid-close sleep): about 5.7 hours lost in total. The collector now detects a sleep or freeze, writes a `paused` note and reconnects at once, but only staying awake prevents the loss.

**Run unattended:**

```sh
mkdir -p data/logs
nohup python3 -u collect_live.py >> data/logs/collector.log 2>&1 &                 # Linux server
nohup caffeinate -s python3 -u collect_live.py >> data/logs/collector.log 2>&1 &   # Mac: plugged in, lid open
```

`caffeinate -s` stops sleep while on power. Keep the lid open (or closed with an external display, power and keyboard). Locking the screen is fine. A service with automatic restart (for example systemd) is part of step 6.

| Task | Command |
|---|---|
| Is it running? | `pgrep -fl collect_live.py` |
| Follow the log | `tail -f data/logs/collector.log` (Ctrl+C stops watching, not the collector) |
| Stop it cleanly | `pkill -f collect_live.py` (sends SIGTERM, which writes a `stopped` note) |
| Fresh tables | `python3 normalize_data.py` (by hand until step 6 schedules it hourly) |
| Check the data | `python3 validate_data.py <day>` |

**Lock file.** The collector takes an exclusive lock on `<data>/logs/collector.lock` and refuses to start if another copy holds it; the file holds the running PID. The lock is released automatically when the process exits, even after a crash, so a stale lock file on disk is harmless. Raw files are also created exclusively, so two writers can never share one file.

**Restarts.** After a reboot or crash, check `pgrep` and start the collector again. On every connect it asks for fresh full books, so nothing is built on stale state. The time it was down is reported as a gap.

**Capacity (measured, 100 markets).**

| Metric | Value |
|---|---|
| Message rate | about 800 per second |
| Raw size | about 2.3 GB/hour uncompressed, **about 0.26 GB/hour gzip** |
| Per day / per two weeks | about 6 GB / about 90 GB |
| Collector memory | about 63 MB |
| Validation of all data | about 9 minutes for 63 hours, under 190 MB |
| Full table rebuild | about 15 minutes for 47 hours; clean tables about 2.2 GB |
| Full universe pull | about 10 minutes, about 95 MB compressed |

Volume is concentrated in the busiest markets, so 500 or 1,000 markets may grow less than linearly. That is measured before each scale step, never assumed. Raw is the only irreplaceable layer: step 6 adds daily off-machine backups.

## Failure modes and recovery

| Failure | How it is detected | What happens / what to do |
|---|---|---|
| WebSocket drop | Exception, or 30 s of silence | Reconnects at once (then doubling waits up to 60 s), resubscribes, gets fresh full books, logs a `disconnected` note. `--standby` keeps a second connection so a drop loses nothing |
| Machine sleep or frozen process | Clock jump between ticks | `paused` note, every connection cut and reopened, gap reported. Prevent it: run on a server or keep the Mac awake |
| Collector crash | Restart with no `stopped` note | Reported as a gap by `validate_data.py`. Restart the collector. Auto-restart and alerts come with step 6 |
| Second collector started | Lock file | Refuses to start |
| Collector killed mid-write | Damaged gzip ending | Reader skips to the next readable gzip member, reports bytes lost. Next run writes `<HH>.partN.jsonl.gz`, never appends to an old `.gz` |
| Our book drifts from the exchange | REST reconciliation | Resync to the REST copy, recorded in `resyncs` |
| Best price disagrees with Polymarket | Live consistency check | Listed in the report with examples. Investigate before trusting that stretch |
| Message type we do not parse | Builder | Row in `unsupported_events`, never silently dropped |
| Universe change | Daily re-pick | Shards switch, `universe_changed` note written, old picks kept |
| Gamma rate limit (HTTP 429) or errors | Status code | 6 attempts with backoff, honouring `Retry-After`. A repeated page cursor stops the download instead of looping |
| REST book copy fails | HTTP error | `rest_books_failed` note; collection continues |
| Disk fills | Write error | Collector stops with exit code 2 instead of looping. Step 6 adds disk alerts and backups |
| Bug found in clean tables | Tests, validation, review | Fix the code, bump `NORMALIZER_VERSION`, run `normalize_data.py --rebuild`. Raw stays as is |

## Testing

```sh
pytest                       # full offline suite, 171 tests in about 25 s (live tests are skipped by default)
pytest --cov                 # with branch coverage (96% of pipeline/)
pytest -m live               # opt-in tests that talk to Polymarket's real endpoints
pytest tests/test_book.py    # one file
ruff check . && ruff format --check .
```

| Type | Count | What it covers |
|---|---|---|
| Unit | about 120 | Book apply, rewind, resync, save/load; tolerant reader; part files; event parsing; schemas; universe filters and cap; HTTP retries; queries; config |
| Property (hypothesis) | 7 | Incremental replay equals full replay; rewind then forward equals the original; resync invariants; save/load mid-stream; split at checkpoints equals one pass; determinism |
| Regression | about 15 | Every bug fixed so far, most built from the real messages: same-timestamp batches, late REST copies, REST clock offset, same-millisecond REST copies, stale full books, two writers, crash without `stopped`, and more |
| Golden file | 3 | A 283 KB cut of real data (tick size change, REST copies, a drop and reconnect, a stop) must give exactly the expected tables |
| Collector integration | 19 | Against a local fake WebSocket and REST server: subscription, initial books, silence timeout, server close, malformed frames, sharding, universe switch, SIGTERM, lock |
| Crash | 2 | `kill -9` of the real collector mid-stream: everything up to the last flush is readable and the gap is reported |
| End to end | 1 | Collect, validate, build, query on a temporary data folder |
| Live (`-m live`) | 1 | About 60 s against the real Polymarket, then validation. Not run in CI |

CI (`.github/workflows/ci.yml`) runs ruff and the offline pytest suite on Python 3.11 and 3.13 for every push and pull request.

## Development workflow

1. **Check existing work first.** `git fetch --all` and look at open branches and PRs, so nobody builds the same thing twice.
2. **Branch from the latest `main`.** Names like `feature/...`, `fix/...`, `docs/...`.
3. **Small PRs, one concern each.** The description says what changed, why, how it was tested, and whether a data contract changed.
4. **Review.** At least one teammate reviews. CI must pass. `main` must always run.
5. **Commits.** Short imperative summary line; the body explains why.

Code rules:

- **Exact decimals.** Parse prices and sizes with `Decimal` and store them as Parquet decimals. Never do money or probability math in floats.
- **Pure core, thin scripts.** Book logic, parsing and schemas go in `pipeline/` with no I/O side effects. Scripts wire arguments, files and the network.
- **One config.** New paths, URLs or limits go in `pipeline/config.py`.
- **No mutable globals.** Pass state explicitly.
- **Fail loud on our own contract violations, tolerate exchange noise.** Malformed raw lines are counted and reported; a schema mismatch in our own output stops the job.
- Type hints on public functions. Logging with timestamps and levels for anything that runs unattended.
- Dependencies are pinned. Byte-identical rebuilds depend on it.
- **No secrets in git.** API keys only in environment variables or an untracked `.env`.
- **Data never goes in git.** Nothing under the data root is committed, including small samples outside `tests/` fixtures.
- **Docs in the same PR.** If a change affects usage, layout or a data contract, update this README (and PROJECT.md if it changes the plan) in the same PR.

## Roadmap

Next for the pipeline: step 6 (24/7 server with scheduled builds and validation, alerts, raw backups), step 7 (market outcomes and free historical data), step 8 (scale from 100 to 500 to 1,000+ markets), and step 9 (two clean weeks). Kalshi follows the same steps once permission and an API key are in place. Research (Level 2) starts after that. Details: [PROJECT.md](PROJECT.md).

## Troubleshooting and FAQ

**`python3: command not found`.** Install Python 3.11+ from python.org. On Windows the collector is not supported (see Requirements).

**`No module named ...`.** Activate the virtual environment and run `pip install -r requirements.txt` again.

**"another collector is already running (lock ..., pid ...)".** One is already running: `pgrep -fl collect_live.py`. Do not start a second copy.

**`Connection N dropped ... Reconnecting`.** Normal now and then. It reconnects at once and gets fresh books.

**`paused for ...s`.** The computer slept (or the program was frozen) that long. Nothing was collected meanwhile. See [Operations](#operations).

**`query_data.py` says a table does not exist.** Run `normalize_data.py` first. A table only appears once at least one hour has rows for it.

**`history` matched several tokens.** It shows the first. Use more words from the question, or pass the token ID from `query_data.py markets`.

**Why are recent minutes missing from the tables?** The hour still being written is skipped unless you pass `--current`.

**Prices look like `0.010000000000000009`.** You are reading tables built before Step H (`normalizer_version` 1, floats). Rebuild with `normalize_data.py --rebuild`.

**Do I need a Polymarket account?** No. Kalshi will need an account, an API key and written permission to store data.

**Can I share the raw data?** No. Raw exchange data is for internal research only. See PROJECT.md.

**How much disk will this use?** About 6 GB a day for 100 markets. Check `du -sh data/raw` regularly.

## Team and contact

| Person | Role |
|---|---|
| Dhruv Jhamb | Project lead |
| Kashyap Maddala | Team member, repository owner |
| David Oprea | Team member |

Questions about direction go to the lead. Questions about code go in the PR or an issue. The full team and club context are in [PROJECT.md](PROJECT.md#team-and-club).

## License

MIT. Copyright (c) 2026 Kashyap Maddala. See [LICENSE](LICENSE).

The license covers our code only. Market data collected from exchanges is subject to each venue's terms of use and is not redistributed.

## Acknowledgements

- [Polymarket](https://polymarket.com) for its public Gamma, CLOB REST and WebSocket market data APIs.
- BoilerQuant at Purdue University.
- Built on [DuckDB](https://duckdb.org), [Apache Arrow / PyArrow](https://arrow.apache.org), [websockets](https://websockets.readthedocs.io) and [HTTPX](https://www.python-httpx.org).
