# Prediction Market Volatility and Analytics Platform

BoilerQuant (Purdue), Fall 2026. Lead: Dhruv Jhamb. Venues: Polymarket, then Kalshi.

We are building the **measurement layer for prediction markets**: our own validated order book data, per-contract microstructure analytics (volatility, liquidity, informed flow) computed from it, and every forecast graded publicly against what actually happened.

This README has two parts:

- **Part I: Engineering.** The system as it exists today: architecture, data contracts, the guarantees we make about the data, how we prove them, how it fails, and the standards every change must meet. Read this before touching code.
- **Part II: Project plan.** The original handoff: vision, decisions, research directions, reading lists. Read this to understand *why* we are building it.

> **Golden rule.** Everything downstream (features, models, backtests, the public track record) is only as trustworthy as the data pipeline. A wrong number in a clean table becomes a wrong research result months later, silently. So the pipeline is built like production software: tested, versioned, reproducible, and checked against the exchange itself.

---

# Part I: Engineering

## E1. Status at a glance

Level 1 (the data pipeline) is in progress. It is done when both venues have run two weeks with no unrepaired gaps and our books match the exchange (Section 7.8).

**Polymarket**

| # | Step | Status |
|---|---|---|
| 1 | Raw store: every message saved exactly as received | Done |
| 2 | Universe: pick markets to watch (top 100 by 24h volume, max 5 per event) | Done |
| 3 | Collector: watch every picked market, reconnect, re-pick daily, never quit | Done |
| 4 | Validation: rebuild books, compare to the exchange, find gaps | Done |
| 5 | Normalizer: raw files into versioned Parquet tables, DuckDB query layer | Done |
| H | **Hardening: test suite, shared core module, exact decimals, CI** | **Next** (see E10) |
| 6 | Operations: 24/7 server, scheduled normalize + validate, alerts, raw backups | After H |
| 7 | Outcomes and backfill: resolutions, Polymarket-v1 history, reconciliation | |
| 8 | Scale: 100, then 500, then 1,000+ markets, with measured capacity | |
| 9 | Two clean weeks | |

**Kalshi:** not started. Blocked on written permission to store data (their terms) and an API key. Same steps, reusing the shared raw store and core.

## E2. Quick start

Requires Python 3.11+. No Polymarket account is needed (public market data, IP rate limited).

```sh
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python3 pick_markets.py                 # choose the universe (about 10 min, pulls all ~234k open markets)
python3 listener.py                     # collect forever (Ctrl+C stops cleanly); --verbose to watch changes
python3 check_data.py                   # validate everything collected so far; ends with PASS or a problem list
python3 normalize.py                    # build clean tables for every finished hour
python3 query.py history "control the House"   # one contract's full history in one DuckDB query
python3 query.py sql "SELECT count(*) FROM trades"
```

Data lands in `data/` (never committed). Long-running collection: `nohup python3 -u listener.py >> data/logs/listener.log 2>&1 &`. Only one listener may run at a time; a lock file enforces it.

## E3. Architecture

```
            Gamma API (all open markets)
                      │  pick_markets.py: filter tradeable, rank by 24h volume, cap per event
                      ▼
            data/universe/polymarket/<UTC>.json, current.json   (universe history kept)
                      │
                      ▼
  CLOB WebSocket ──► listener.py ──────────────► data/raw/polymarket/<day>/<HH>.jsonl.gz
  (book, price_change,  │  shards 400 tokens/conn      (append-only, exactly as received,
   last_trade_price,    │  reconnect + fresh books      plus collector notes)
   tick_size_change)    │  REST /books every 5 min  ──►  (same raw files, kind = rest_books)
                        │  re-pick every 24h
                        ▼
              check_data.py  (validation: replay books, compare to exchange, gaps, sanity)
                        │    shared Book logic
                        ▼
              normalize.py   (deterministic, versioned, idempotent, hourly checkpoints)
                        │
                        ▼
  data/clean/polymarket/<table>/date=YYYY-MM-DD/hour=HH/part.parquet
                        │
                        ▼
              query.py   (DuckDB views over all Parquet)  ──►  Level 2 research
```

**Layering.** Each layer only reads the layer above it and can be rebuilt from it. Raw is the only irreplaceable layer; everything else is a pure function of raw plus code version.

| Layer | Where | Rebuildable? | Owner |
|---|---|---|---|
| Raw | `data/raw/<venue>/` | **No** (back it up) | `listener.py`, `raw_store.py` |
| Universe | `data/universe/<venue>/` | No (history of what we watched when) | `pick_markets.py` |
| Clean | `data/clean/<venue>/` | Yes, from raw | `normalize.py` |
| Reports | `data/reports/<venue>/` | Yes, from raw | `check_data.py` |
| Features / research (Level 2) | `data/features/`, notebooks | Yes, from clean | future |

**Process model.** One long-running async process (`listener.py`) per venue does all network I/O: one WebSocket task per shard of 400 tokens, a REST snapshot task, a status task, and a background re-pick subprocess. Batch jobs (`check_data.py`, `normalize.py`) are separate processes that only read raw files, so they can run on a schedule without touching collection.

## E4. Repository map

Everything is currently flat at the repo root (scripts, not a package). See E10 for the planned module layout.

| File | Responsibility | Reads | Writes |
|---|---|---|---|
| `raw_store.py` | Append-only raw writer, partitioned by venue/UTC day/UTC hour; gzip, flushed once a second; never reopens a finished `.gz` (writes `HH.part2.jsonl.gz` instead) | | `data/raw/` |
| `pick_markets.py` | Pulls every open market from Gamma `/markets/keyset` with retries and 429 handling; keeps tradeable binary markets; ranks by 24h volume; caps per event; saves the universe and the raw market pages | Gamma API | `data/universe/`, `data/raw/polymarket_markets/` |
| `listener.py` | The collector: shards tokens across WebSockets, PINGs, 30 s silence timeout, reconnect with 1 to 60 s backoff, fresh books on every connect, REST snapshots every 5 min, daily re-pick, lock file, clean shutdown on SIGTERM/Ctrl+C | CLOB WS + REST, universe | raw via `raw_store` |
| `check_data.py` | Validation: tolerant raw reader (survives damaged gzip), `Book` reconstruction with history for rewind, all checks in E7, saved report | raw | `data/reports/` |
| `normalize.py` | Raw into seven Parquet tables (E5), hourly book state checkpoints, REST resync policy, `--rebuild` / `--current` | raw, universe | `data/clean/` |
| `query.py` | DuckDB views over the clean tables; `markets`, `history`, `sql` commands | clean | |
| `requirements.txt` | Runtime dependencies | | |

## E5. Data contracts

These are the interfaces between layers. Changing one is a breaking change: it needs a PR that says so, a `NORMALIZER_VERSION` bump (for clean tables), and a rebuild.

### Raw envelope (one JSON object per line)

```json
{"recv_ts_ns": 1793664000123456789, "kind": "message", "raw": "<exact text the exchange sent>"}
```

| `kind` | Meaning |
|---|---|
| `message` | Exchange frame, untouched (`raw` is a string, never parsed before saving) |
| `rest_books` | Body of a `POST /books` snapshot response (independent copy for validation) |
| `connected`, `subscribed`, `disconnected` | Connection lifecycle, with connection number and reason |
| `universe_changed` | The daily re-pick switched the watched set |
| `stopped` | Clean shutdown. A restart with no `stopped` before it means a crash, and the gap is counted |
| `rest_books_failed` | A snapshot request failed (reason saved) |

### Clean tables

Partitioned `date=YYYY-MM-DD/hour=HH` by the UTC receive hour, matching the raw file they came from. Every row carries `venue` and `normalizer_version`.

| Table | Grain (one row per) | Key columns |
|---|---|---|
| `book_deltas` | price level that changed | `exchange_ts, recv_ts_ns, token_id, market_id, side, price, new_size` (absolute, 0 removes the level), `reported_best_bid/ask`, `hash` |
| `book_snapshots` | level in a full book | `snapshot_id, source` (`live` or `rest`), `side, price, size, tick_size` |
| `trades` | trade observed | `price, size, taker_side, fee_rate_bps, transaction_hash` |
| `top_of_book` | moment our rebuilt book changed | `best_bid/ask` and sizes, `mid, spread, bid_levels, ask_levels, cause` (`change`, `full_book`, `resync`) |
| `resyncs` | correction from a disagreeing REST copy | `levels_wrong, example` |
| `collector_events` | collector note | `kind, details` |
| `markets` | (token, pick) | question, outcome, event, tick size, neg risk, end date, `picked_at`, rank |

**Units and identity.**
- `exchange_ts` is exchange time in ms since epoch UTC; `recv_ts_ns` is our receive time in ns. Always keep both (Section 7.2).
- `token_id` is one outcome's book (YES and NO are separate books). `market_id` is the Polymarket condition ID. Gamma's numeric ID is `gamma_market_id`.
- Prices are probabilities in [0, 1]. **Known debt:** prices and sizes are float64 today; moving to exact decimals is part of step H.
- Ordering: exchange timestamps tie (Polymarket batches many changes under one timestamp), so replay order is file order, then line, then position in the message. Never sort by timestamp alone.

## E6. Guarantees (invariants)

The pipeline promises these. Each is enforced in code and, after step H, each has a test.

1. **Raw is append-only and never edited.** Fixing any bug means re-running downstream code, never patching raw.
2. **Single writer.** A lock file (`data/logs/listener.lock`) stops a second listener; two writers once corrupted an hour of data.
3. **Crash safety.** At most about one second of data is lost on a crash (gzip flush interval). A crash is detectable (no `stopped` note) and reported as a gap.
4. **Determinism and idempotence.** The same raw bytes plus the same code version produce byte-identical Parquet (verified by rebuild). Hours are rewritten, never appended.
5. **Versioning.** Every clean row records `normalizer_version`. Output changes require a bump.
6. **No silent loss.** Every disconnect, crash, damaged file stretch and REST disagreement appears in a table or report. Nothing is dropped quietly.
7. **Point-in-time.** Nothing at time *t* may use data from after *t* (Section 0, rule 4). The `markets` table keeps every pick with `picked_at` so metadata can be joined as known at the time.

## E7. Correctness: how we know the data is right

`check_data.py` replays every book from raw and runs:

| Check | What must hold | Latest result (Nov 3, 2026) |
|---|---|---|
| Live consistency | After each same-timestamp batch, our best bid/ask equals the `best_bid/best_ask` Polymarket reports in `price_change` | 644,154 of 644,154 (100%) |
| REST reconciliation | Every 5 min, our full book (every level), rewound to the snapshot's exact exchange timestamp, equals the REST copy | 1,530 of 1,560 (98%) |
| Sanity | Not crossed, prices in [0, 1] and on tick, sizes positive | Pass |
| Trades | Price in [0, 1] | Pass |
| Gaps | Every disconnect, stop and crash restart, with blind duration | All found, including a crash with no stop note |
| Damage | Unreadable gzip stretches located and measured | One damaged file (Nov 3, 19 UTC) reported |

**Known discrepancy.** The REST misses are mostly one market (Adam Hamilton, 2028 Democratic nomination) where REST shows 11 resting levels the live feed never sent: a Polymarket-side inconsistency. Policy (Section 7.5, "log and resync"): the normalizer corrects the book to the REST copy, replays our later changes on top, and records it in `resyncs`.

**Not yet checked** (Level 1 work): trade-book consistency, on-chain fill reconciliation, Polymarket-v1 spot checks, exchange-to-receive lag percentiles, completeness vs expected trade counts.

## E8. Failure modes

| Failure | Detection | Response |
|---|---|---|
| WebSocket drops | Exception or 30 s silence | Reconnect with 1 to 60 s backoff; resubscribe; exchange resends full books; gap logged |
| Process crash / machine sleep | No `stopped` note before restart | Gap counted in report. Step 6 adds auto-restart and alerts |
| Second listener started | Lock file | Refuses to start |
| Damaged gzip (killed mid-write) | Tolerant reader scans for gzip members | Reads past it, reports bytes lost; new runs write part files instead of appending |
| Book drift from exchange | REST reconciliation | Resync to REST, logged in `resyncs` |
| Universe change | Daily re-pick | Switch shards; `universe_changed` note; history kept |
| Gamma rate limit (429) | HTTP status | Retry with backoff |
| Disk fills | Step 6 alert (planned) | Raw backup to cloud storage, then prune local |
| Unknown message type | Counted, not tabled yet | Step H: `unsupported_events` table |

## E9. Capacity (measured, 100 markets)

| Metric | Value |
|---|---|
| Message rate | about 800 per second |
| Raw size | 2.3 GB/hour uncompressed, **0.26 GB/hour gzip** (about 6 GB/day, about 90 GB per two weeks) |
| Collector memory | about 63 MB |
| Normalize speed | about 4 hours of data in 2 seconds |
| Full universe pull | about 10 minutes, 95 MB compressed |

Volume concentrates in the most active markets, so 500 or 1,000 markets should grow less than linearly. This is measured before each scale step, never assumed (step 8). The step 6 server needs 100+ GB of disk and backups from day one.

## E10. Engineering standards

Every change, by anyone (human or AI), meets these.

**Workflow**
- Always branch from the latest `main` (`git fetch` and check open branches first, so nobody builds the same thing twice). Name branches `feature/...`, `fix/...`, `docs/...`.
- Open a pull request. At least one teammate reviews. `main` must always run.
- Small PRs, one concern each. Say what changed, why, how it was tested, and whether any data contract changed.
- Commit messages: imperative summary line, body explains why.

**Code**
- **Money and probabilities are exact.** Parse prices and sizes with `Decimal` (or exact decimal Parquet types), never accumulate floats.
- **Pure core, thin scripts.** Book logic, parsing and schemas live in importable modules with no I/O side effects; scripts only wire arguments, files and network.
- **No mutable globals.** Pass state explicitly.
- **One config.** Paths, URLs and limits are defined once.
- **Fail loud on contract violations, tolerate exchange noise.** Malformed raw lines are counted and reported; a schema mismatch in our own output stops the job.
- Type hints on public functions. `ruff` for lint and format. Logging with timestamps and levels, not print, for anything that runs unattended.
- Dependencies pinned (`requirements.txt` with exact versions; byte-identical Parquet depends on them).
- Secrets (Kalshi keys) only in environment variables or an untracked `.env`. Data never goes in git.

**Testing** (pytest, run in CI on every PR)
- **Unit:** book apply/remove/resync/rewind, tolerant gzip reader, part-file naming, universe cap and filters, schema builders.
- **Regression:** one test per bug we have fixed (same-timestamp batches, REST rewind, damaged gzip, double writer, crash detection).
- **Golden file:** a small checked-in raw fixture must produce exactly the expected clean tables.
- **Property:** random snapshot + delta sequences; replaying in pieces with checkpoints equals replaying all at once.
- **Point-in-time:** any feature or model test that touches data after *t* fails.

**Docs:** this README is updated in the same PR as the change it describes. Decisions that change direction get a dated line in Part II.

### Planned layout (step H)

```
pm/                        importable package, no side effects on import
  config.py                paths, URLs, limits (one place)
  raw/store.py, reader.py  append-only writer; tolerant reader
  book.py                  order book, rewind, resync (pure)
  polymarket/parse.py      exchange frames into typed events
  polymarket/schemas.py    clean table schemas (exact decimals)
scripts/ or root           listener.py, pick_markets.py, check_data.py, normalize.py, query.py (thin)
tests/                     unit, regression, golden fixtures, property
pyproject.toml             ruff, pytest config
.github/workflows/ci.yml   tests + lint on every PR
```

### Technical debt (honest list)

| Item | Why it matters | Plan |
|---|---|---|
| No automated tests | Fixed bugs can silently return | Step H |
| float64 prices and sizes | Rounding noise (0.010000000000000009), unsafe for money | Exact decimals, version bump |
| `normalize.py` imports from `check_data.py` | Core logic trapped in a script | Extract `pm/book.py`, `pm/raw/reader.py` |
| Global `BROKEN` list in `check_data.py` | Hidden shared state | Return damage from the reader |
| Paths repeated in 5 files | Drift | `pm/config.py` |
| No connection ID on exchange frames | Cannot tie a frame to its session | Add to envelope (backward compatible) |
| Unknown message types only counted | Silent coverage gaps | `unsupported_events` table |
| `query.py markets` uses the latest pick | Fine for lookup, look-ahead risk in research | Point-in-time join helper |
| `check_data.py` decompresses whole files in memory and replays all selected days in one pass | Fine at 100 markets; will not be at 1,000+ over weeks | Run per hour from the normalizer's checkpoints |

A teammate's branch (`pipeline/normalization-reconstruction-validation`) independently built tests, exact decimals, manifests and an unsupported-events table. Step H merges those ideas into the main pipeline rather than running two normalizers.

## E11. Contributing

1. Read Part I, then Part II Sections 0, 2 and 7.
2. Run the quick start against a few minutes of live data.
3. Pick an item from E1 or the debt table, say so in the team chat, branch from `main`, open a PR.
4. Questions about direction go to the lead. Questions about code go in the PR.

---

# Part II: Project plan (original handoff)

> Written October 4, 2026 as the single source of truth for what this project is and what comes next. Part I describes what has since been built; where they differ on implementation details (file layout, task list), Part I is current. Decisions and direction below still stand.

## 0. How to Use This File (instructions for Claude Code)

1. **Current phase:** Level 0 is done and **Level 1 (Data Pipeline) is in progress** (status in Part I, E1). Do not start Level 2 work until Level 1's "done" criteria (Section 7.8) are met, unless the lead explicitly asks.
2. **Verify every external API detail before relying on it.** Endpoint URLs, message formats, auth methods, fee formulas, and rate limits in this file were written from research as of **October 2026** and may have changed. Always check the current official Kalshi and Polymarket docs and say so when something differs.
3. **Data integrity beats speed.** The raw store is append-only and never edited. Every transformation must be reproducible from raw data.
4. **No look-ahead, ever.** Any model, metric, or forecast computed at time $t$ may only use data with timestamps $\le t$. This is the most important research rule in the project.
5. **Ask before:** adding paid services, changing the storage format, publishing anything publicly, trading with real money, or redistributing raw exchange data.
6. **Writing style for anything the lead reads** (docs, READMEs, reports, comments meant for humans): do **not** use em dashes. Use commas, colons, parentheses, or separate sentences.
7. **Math in human-facing docs** should be properly rendered (LaTeX in Markdown, e.g. `$$ ... $$`), and explanations should build from the ground up, assuming no prior knowledge of the theorem being used.

---

## 1. Project Identity

| Field | Value |
|---|---|
| **Title** | Prediction Market Volatility and Analytics Platform |
| **Organization** | BoilerQuant (Purdue University quantitative finance club), Fall 2026 |
| **Project lead** | Dhruv Jhamb (applied math, Purdue) |
| **Team size** | 3 to 5 members (club includes freshmen new to the material) |
| **Venues** | Kalshi and Polymarket |
| **Status** | Level 0 done; Level 1 in progress (Polymarket steps 1 to 5 built and validated, November 2026) |

### 1.1 Public description (as presented to the club)

> Kalshi and Polymarket now trade billions of dollars a month, and plenty of tools already track their order books, whales, and arbitrage. But when you see an event's price, how much should you trust it? That comes down to liquidity (is there real money behind it?) and volatility (how much is it about to move?). We're building a platform that measures both for every contract, starting by testing which of the volatility models proposed by recent research actually works.

**Prerequisites listed for members:** Programming (Python, SQL, JavaScript, React basics); Math (probability and statistics basics).

### 1.2 One-sentence vision

Build the measurement layer for prediction markets: rigorous, per-contract microstructure analytics (volatility, liquidity, order flow, and more) computed from our own validated data pipeline, with every forecast publicly graded against what actually happened, delivered through a usable product.

### 1.3 The plain-language version

Every prediction market price is the crowd's probability (70 cents means about 70%). But a price alone does not say whether it is **jumpy** (volatility), **real** (liquidity), or being **pushed by people who know something** (informed flow). We measure those things for every contract, test which published methods actually work, and show our report card publicly.

---

## 2. Where We Stand (current state of decisions)

### 2.1 Decided

- **Structure:** the project runs in levels.
  - **Level 0:** Foundations (minimal, four must-reads).
  - **Level 1:** Data Pipeline (most important; everything depends on it).
  - **Level 2:** Broad microstructure analysis and the directions it unlocks (A through F, Section 8).
- **Level 2 is intentionally broad,** not volatility-only. Volatility is one workstream inside "Direction A: Microstructure Measurement." The pattern for every feature: implement the framework from the literature, measure it across all markets, test how well it forecasts or explains what happens next.
- **The end goal is a working product** (website/app) that lets users do many things, built from whatever the team actually produces.
- **Staging:** Level 0 → Level 1 → Direction A → one of Direction B or C → Direction E (product) → D and F later or in collaboration.
- **Raw data is infrastructure, not the product.** We collect it for our own research. We do not sell or redistribute raw orderbooks.
- **Public outputs are derived analytics only** (and only after checking Kalshi and Polymarket terms of use).

### 2.2 Scrapped or deprioritized (do not build unless the lead revives them)

| Idea | Why it was dropped |
|---|---|
| Selling raw orderbook data | Oddpool, Predexon, CryptoStruct already dominate; history cannot be backfilled |
| Cross-venue arbitrage scanner | Very crowded (Oddpool dashboard, many open-source bots) |
| Whale/wallet tracking | Very crowded (PrediEdge, Hashdive, Polymarket Analytics, many more) |
| AI/LLM outcome forecasting and news-signal products | Crowded (Alphascope and others); also a prediction/edge claim we decided not to make |
| "Implied News Oracle" (LLM explains vol spikes, claims headlines are underpriced) | News attribution is taken; "underpriced" is an unprovable edge claim; too slow vs professionals |
| "Cross-venue volatility arbitrage" | Broken premise: there are no options on these contracts, so volatility cannot be traded separately |
| "Greeks portfolio optimizer" | An optimizer needs expected returns (edge) we do not have; a risk visualizer may return later |
| Standalone "Greeks" section on contract pages | Delta and gamma are functions of price alone; replaced by an Expected Move Range |
| Market-wide "Dashboards" section | Redundant with sortable columns in All Markets; correlation matrix moved to Research |
| "OracleAgent" (LLM agent as AMM + information arbitrageur) | Market making and information arbitrage conflict; needs capital; crowded; low quant learning value |
| Thematic benchmark indices | Belief Systems already does this for institutions |

### 2.3 Salvaged pieces from scrapped ideas (allowed as experiments later)

- Jump detection plus matching jumps to **scheduled** events (CPI, FOMC, games), no scraping required.
- A research study on **which venue leads price discovery** (Kalshi vs Polymarket).
- An experiment: in thin markets, does giving a market maker an outside fair-value prior reduce adverse selection vs standard orderbook-only quoting?
- A correlated-scenario **risk visualizer** (not optimizer) for portfolios of event contracts, much later.

---

## 3. Competitive Landscape (as researched, October 2026)

Verify before citing publicly; this space moves monthly.

### 3.1 Raw data and infrastructure (saturated)

- **Oddpool (YC-backed):** tick-level Kalshi + Polymarket orderbook deltas and trades, normalized schema, canonical cross-venue event IDs, settlement-rule registry. Found only about a third of matched events are truly settlement-fungible. Retail side: arbitrage dashboard, whale tracking, paper trading, topic pages. Excludes parlays/combos.
- **Predexon:** raw tick history (placements, updates, cancels, fills) for Polymarket, Kalshi, Opinion; mempool-aware WebSocket; wallet analytics. Pay-per-GiB with free credits. Kalshi history from January 2026.
- **CryptoStruct, CCXT (prediction market namespace), Tatum, marketlens (Polymarket L2 replay backtester with queue modeling), Oddpool PredictionMarketBench:** more data and backtesting infrastructure.
- **Public dataset:** Polymarket-v1 (Qin and Yang, arXiv 2606.04217): about 1.2 billion trades, 1.3 million markets, with ground-truth trade direction.

### 3.2 Retail signals (crowded)

- **Alphascope:** AI predictions vs market, news impact ranking, cross-platform odds, alerts, wallet tracker, payout/EV calculator.
- **PrediEdge:** whale tape, leaderboard, heat map, insider activity.
- **Prediction Edge:** election-only odds-move news feed with auto-generated "insights" (some describe zero-size moves as significant, which illustrates why our measurements matter).

### 3.3 Closest to us

- **Belief Systems (James Stewart):** "measurement layer for prediction markets." Thematic indices (about 13, updating frequently) and Forward Belief Volatility (FBV). Volatility product covered very few indices (one listed index, MIDVOL, at last check), not per-contract. Paper: "Forward Belief Volatility in Prediction Markets: A Budget-Anchored Term Structure," SSRN 7220678 (August 2026). Full methodology may not be public.
- **bs-p / polymarket-kernel (Holy Poly Foundation):** open-source Rust market-making kernel implementing Dalen's logit jump-diffusion plus Avellaneda-Stoikov in logit space. A quoting library, not an analytics product.

### 3.4 Novelty audit summary (most important research context)

The core question "what predicts how much a prediction market price will move" was largely answered for Kalshi in July 2026:

- **Xi, Moallemi, Pai, Wang (Columbia), "Volatility in Prediction Markets: A Structural Approach," arXiv 2607.08199.** Hourly Kalshi panel (Aug 2021 to Apr 2026), about 880,000 out-of-sample forecasts, Winkler interval score. Models: ARCH/GARCH, a Wright-Fisher deadline-resolution term $p(1-p)/\tau$, a Glosten-Milgrom adverse-selection term from spread and volume (DR-AS), hybrids. **GARCH + DR-AS is best or near-best across categories.** Stylized facts: volatility peaks near 50 cents, rises as time to resolution shrinks; economics contracts are smooth, sports contracts are jump-like.
  - **This is the incumbent model every new approach must beat.**

Other already-published findings (do not claim these as novel):

- Calibration improves with volume and number of traders (Kalshi research, 2026, 2.2M+ resolved markets).
- VPIN (with true trade direction) predicts worse Brier scores; Gibbs spread negatively predicts Brier scores (Qin and Yang).
- Tick rule and bulk volume classification are near-random for trade direction in these markets (Qin and Yang). **Never build VPIN on inferred direction.**
- PIN and VPIN disagree in some Polymarket market types (Le, SSRN 6933527).
- Favorite-longshot bias on Kalshi (Bürgi, Deng, Whelan, "Makers and Takers").
- Polymarket orderbook stylized facts (arXiv 2604.24366).

**What remains genuinely open (our novelty):**

1. A **multi-framework, cross-venue** volatility benchmark: realized logit vol vs Xi et al. structural and GARCH+DR-AS vs Dalen RN-JD vs Stewart FBV vs Black-Scholes digital IV, on the same data, Kalshi **and** Polymarket, with QLIKE and Diebold-Mariano tests.
2. **Per-contract** forward volatility, expected-move bands, volatility surfaces, and liquidity grades for every contract.
3. A **public, point-in-time, self-graded track record** of every published metric (the most defensible novel piece).
4. Whether **true-direction VPIN predicts future volatility** (not just accuracy).
5. **The calibration conflict:** volume/trader count say thin markets are less accurate, but wider spreads predict *better* accuracy. Explain why, controlling for category and time to resolution.
6. Empirical **cross-market belief correlations** (probably open, low confidence).
7. **Combo/parlay implied correlation** (open if combo data is accessible).

---

## 4. Team, Club Context, and Collaboration

- Team: 3 to 5 members, mixed experience. Everyone should understand every part of the system; avoid silos.
- Suggested roles (adjust to team size): lead (benchmark design, models, methodology), research member (models, liquidity, flow), data member (pipeline, reliability), web member (frontend, API, kept lean).
- Other BoilerQuant Fall 2026 projects with overlap (possible collaboration, not competition):
  - **Order Book Modeling** (simulated LOB with stochastic order flow, agents, L3 training): overlaps with Direction D (simulator).
  - **MacroCast** (open-weight macro nowcasting with a live dashboard of timestamped predictions and evaluation scores): shares the "grade forecasts publicly" idea; Kalshi lists contracts on the same indicators. Do not pitch "we grade ourselves" as unique within the club; pitch "volatility for prediction markets, where nobody knows which model works."
  - **Options Pricing and Visualization Screener:** overlaps with Black-Scholes digital IV and strike-ladder work.
  - **MD Feed Handler** (Nasdaq ITCH to orderbook): conceptual cousin of our Level 1 collectors.

---

## 5. Key Concepts Reference (for the codebase and new members)

### 5.1 Contract vs event

- **Contract (market):** one binary outcome. Pays \$1 if YES, \$0 if NO. Price $p \in (0, 1)$.
- **Event:** a group of related contracts (e.g., Fed December decision: Hold / Cut 25 / Cut 50).
- **Strike ladder:** an event whose contracts are thresholds on an underlying number (Bitcoin above \$X, S&P ranges, CPI brackets).
- **Dated ladder:** "happens by date X" contracts for several dates.

### 5.2 Log-odds transform

Prices are bounded, so all volatility work happens in log-odds:

$$x = \log\left(\frac{p}{1-p}\right), \qquad p = \frac{1}{1+e^{-x}}$$

Clip prices away from 0 and 1 before transforming (e.g., $p \in [\epsilon, 1-\epsilon]$ with a documented $\epsilon$) to avoid infinities.

### 5.3 Realized volatility (baseline)

With log-odds sampled at regular intervals, $\Delta x_i = x_i - x_{i-1}$:

$$\hat{\sigma}^2 = \frac{1}{n}\sum_{i=1}^{n} (\Delta x_i)^2$$

### 5.4 Expected move in probability points

For small moves over horizon $h$ with belief volatility $\sigma$:

$$\text{Expected move} \approx p(1-p)\,\sigma\sqrt{h}$$

Report upside and downside separately (asymmetric away from 50 cents) by mapping $x \pm \sigma\sqrt{h}$ back through the logistic function.

### 5.5 Forecast scoring (volatility)

QLIKE loss with forecast $\hat{\sigma}^2$, realized variance $RV$, ratio $r = RV/\hat{\sigma}^2$:

$$L = r - \log r - 1$$

Zero when exact; penalizes under-prediction of risk more than over-prediction. Also compute the **Winkler interval score** to be comparable with Xi et al. Use **Diebold-Mariano tests** for significance of loss differences between models.

### 5.6 VPIN (informed flow)

Group trades into equal-volume buckets $\tau = 1..n$; $V^{buy}_\tau$, $V^{sell}_\tau$ from **ground-truth taker side**:

$$\text{VPIN} = \frac{1}{n}\sum_{\tau=1}^{n} \frac{|V^{buy}_\tau - V^{sell}_\tau|}{V_\tau}$$

Adapting to bounded binary contracts is an open research problem.

---

## 6. Level 0: Foundations

**Goal:** every member can look at a live Kalshi or Polymarket orderbook and explain it, and understands the venues well enough to build Level 1.

### 6.1 The four must-reads (only these before Level 1)

1. **Larry Harris, *Trading and Exchanges*:** chapters on orders, order-driven markets, and liquidity/spreads.
2. **Gould et al. (2013), "Limit Order Books," *Quantitative Finance*** (free on arXiv): Sections 1 to 3.
3. **Wolfers and Zitzewitz (2004), "Prediction Markets"** (free, about 20 pages).
4. **Kalshi and Polymarket documentation:** API docs and rulebooks (orders, matching, fees, tick size, settlement, available data).

Everything else is read in Level 2, when a direction needs it (see Section 12).

### 6.2 Done when

Every member can: (1) explain a live orderbook, (2) explain spread and depth, (3) explain what a 70-cent price means, (4) describe what data each venue's API provides.

---

## 7. Level 1: The Data Pipeline (IN PROGRESS, see Part I)

**Goal:** clean, validated, continuously updated data from both venues that the team can trust completely.

Similar plumbing to Oddpool, different purpose: this is our research foundation, not a product to sell. We do not need their coverage; we need trustworthy data.

### 7.1 Sources (verify all details against current docs)

**Kalshi**
- REST API (historically `https://api.elections.kalshi.com/trade-api/v2`): events, markets, market metadata, orderbook snapshots, trades, settlement results.
- WebSocket (historically `wss://api.elections.kalshi.com/trade-api/ws/v2`, requires API key auth): channels for orderbook snapshot + deltas (with sequence numbers), trades (with taker side), tickers.
- **Book representation caveat to verify:** Kalshi has historically returned resting **bids for YES and bids for NO**; the YES ask is implied as $1 - \text{best NO bid}$. Normalize carefully.
- Fees (verify): taker fee historically $\lceil 0.07 \times C \times P \times (1-P) \rceil$ in cents per order, some series charge makers. Store fee schedule versions.
- Combos/RFQ: **check early** what combo and RFQ data the API exposes. Direction B combo work depends on it.

**Polymarket**
- CLOB REST API (historically `https://clob.polymarket.com`): books, prices, trades per token.
- Market metadata API (historically Gamma API, `https://gamma-api.polymarket.com`): events, markets, categories, token IDs, resolution info.
- WebSocket market channel (historically `wss://ws-subscriptions-clob.polymarket.com/ws/market`): book snapshots, price changes, last trades.
- Each binary market has separate YES and NO outcome tokens with their own books; prices in $[0, 1]$; tick size can vary by market (store it).
- On-chain settlement on Polygon (CTF Exchange fill events): ground-truth trade direction and reconciliation source.
- Note: a US-regulated Polymarket venue also exists; confirm which venue(s) and APIs apply.

**Backfill**
- Polymarket-v1 public dataset (arXiv 2606.04217) for deep Polymarket history with true direction.
- Predexon free tier/credits for Kalshi history before our collector started (Kalshi tick history from January 2026).

### 7.2 What to capture

| Data | Fields (minimum) |
|---|---|
| Orderbook snapshots | venue, market_id, side, price, size, exchange_ts, recv_ts, seq |
| Orderbook deltas | venue, market_id, side, price, size_change or new_size, exchange_ts, recv_ts, seq |
| Trades | venue, market_id, trade_id, price, size, taker_side, exchange_ts, recv_ts |
| Markets | venue, market_id, event_id, title, category, rules text, resolution source, open/close/resolution times, tick size, fee schedule ref |
| Events | venue, event_id, title, category, list of market_ids, event type (single, multi-outcome, strike ladder, dated ladder) |
| Outcomes | venue, market_id, result (YES/NO/void), settlement value, settled_ts |
| Combos (if available) | combo_id, legs, quotes, fills, timestamps |
| Collector health | collector, status, gaps detected, reconnects, lag |

Always store **both** exchange timestamp and local receive timestamp (UTC, nanosecond or millisecond precision, documented).

### 7.3 Architecture

```
Collectors (one per venue, async, always running)
        ↓
RAW STORE: append-only, exactly as received (JSON lines or compressed), partitioned by venue/date/hour
        ↓
NORMALIZER: both venues → one shared schema (deterministic, versioned, re-runnable)
        ↓
CLEAN TABLES (Parquet, Hive-partitioned): books_snapshots, book_deltas, trades, markets, events, outcomes
        ↓
DERIVED TABLES: reconstructed books at any timestamp, top-of-book series, time bars, matched cross-venue events
        ↓
QUERY LAYER: DuckDB over Parquet
```

Key properties:
- Raw store is **never edited**. Fixing a normalizer bug = rebuild clean tables from raw.
- Normalizer has a **version number** stored with every derived row.
- Idempotent writes (re-running a day produces identical output).

### 7.4 Cross-venue event matching

- Table: `kalshi_market_id`, `polymarket_market_id`, `match_confidence`, `settlement_identical` (bool), `settlement_differences` (text), `matched_by`, `matched_ts`.
- Start manually for the top markets; automate later (text similarity on titles/rules, then human review).
- Never assume two "same" markets settle identically.

### 7.5 Validation: proving the data is real

Automated checks, run continuously and reported daily:

1. **Book reconstruction check:** rebuild the book from snapshot + deltas, periodically fetch a fresh REST snapshot, compare. Must match exactly (log and alert any mismatch).
2. **Sequence gap detection:** detect missing sequence numbers or timestamp gaps; on gap, mark the interval and resync from a fresh snapshot.
3. **Sanity:** no crossed books (best bid < best ask), prices within $(0,1)$ and on valid ticks, sizes positive.
4. **Trade-book consistency:** each trade price must have been available in the book at that time (within tolerance for latency).
5. **External reconciliation:** Polymarket trades vs on-chain fills; spot-check against Polymarket-v1 and Predexon samples.
6. **Clock checks:** consistent UTC across venues; monitor exchange_ts vs recv_ts lag.
7. **Completeness:** expected markets present; daily trade counts within plausible ranges.

### 7.6 Operations

- Runs 24/7 on a server (not a laptop). Containerize collectors.
- Automatic restart on crash; exponential backoff on reconnect; resubscribe and resnapshot after reconnect.
- Respect API rate limits (verify current limits).
- Alerts (e.g., Discord webhook) on collector down, gap detected, validation failure, disk usage.
- Daily data quality report (counts, gaps, mismatches, lag percentiles).
- Secrets (API keys) in environment variables or a secrets file excluded from Git.

### 7.7 Universe

Start with roughly the **100 most liquid contracts** across both venues (plus all contracts in their events). Expand once validation is stable. Store the universe definition and its history (which markets were tracked when).

### 7.8 Level 1 done criteria

- Two or more weeks of continuous collection with **no unrepaired gaps**.
- Reconstructed books match exchange snapshots.
- Any contract's full price, book, and trade history retrievable in one DuckDB query.
- Daily quality report running.
- Backfill loaded and reconciled.

---

## 8. Level 2: What the Data Lets Us Do (Directions A to F)

Directions stack; **A is the base** everything else uses.

```
Level 1: Data pipeline
        ↓
A: Measurement  ──→  B: Mispricing research
        ↓                     ↓
C: Strategies and backtesting
        ↓
D: Simulator          F: Derivatives
        ↓                     ↓
E: The product (packages everything above)
```

### 8.1 Direction A: Microstructure Measurement (first after Level 1)

Pattern for every feature: implement framework from the paper → compute across all markets → test forecasting/explanatory power → log forecasts to the track record.

| Feature | What it measures | Framework / source |
|---|---|---|
| Volatility | How much prices move | Realized logit vol; GARCH; Xi et al. structural and GARCH+DR-AS; Dalen logit jump-diffusion (arXiv 2510.15205); Stewart FBV (SSRN 7220678); Black-Scholes digital IV (strike ladders only) |
| Spreads | Cost of trading | Quoted, effective, realized spread (Hasbrouck) |
| Depth and resilience | Available size; refill speed after large trades | Depth by level; post-trade refill time |
| Price impact | Price move per dollar traded | Kyle's lambda; Amihud illiquidity |
| Order flow imbalance | Whether pressure predicts next move | Cont, Kukanov, Stoikov (2014) OFI |
| Informed flow | Whether traders seem informed | VPIN, PIN with **true** trade direction |
| Jumps vs drift | Sudden vs gradual moves | Jump tests (e.g., bipower variation); Dalen EM separation |
| Price discovery | Which venue moves first | Hasbrouck information share; lead-lag |
| Calibration | Prices vs outcomes | Brier score; calibration curves |
| Maker/taker profitability | Who makes money | Bürgi, Deng, Whelan approach |
| Near-resolution behavior | How all of the above changes in final hours | Everything above, conditioned on time left |
| Fee-adjusted break-even | Move needed to cover fees | Venue fee schedules |
| Maker concentration | Fragility of liquidity | Share of resting size by top participants (where identifiable) |

**Volatility benchmark sub-plan (the flagship of Direction A):**
1. Replicate Xi et al. on Kalshi (realized vol, structural model, GARCH+DR-AS). Numbers should roughly match; if not, resolve bug vs real discrepancy before continuing.
2. Add Dalen RN-JD, Stewart FBV (as far as published method allows; consider contacting the author), Black-Scholes digital IV.
3. Run on Kalshi **and** Polymarket.
4. Score with QLIKE and Winkler; Diebold-Mariano significance tests; break down by venue, category, horizon (1h, 1d, 1w), price level, time to resolution.
5. Paper-style write-up.

**Open research questions in Direction A:**
- Does true-direction VPIN predict **future volatility**?
- The calibration conflict (volume says thin = worse; spread says wide = better): explain it with controls.
- Cross-market belief correlations.

### 8.2 Direction B: Mispricing and Discrepancy Research

- **Combos/parlays:** quoted combo price vs product of leg probabilities; extract implied correlation by leg type (requires combo data access; check in Level 1).
- **Cross-venue gaps:** adjusted for fees and settlement-rule differences.
- **Logical consistency:** multi-outcome sums above 100%; dated ladders where later dates are priced below earlier ones.
- **Strike ladders vs options:** Bitcoin (Deribit/Binance options) and S&P (SPX options) implied probabilities vs contract prices.
- **Sports vs sharp sportsbook lines.**
- **Favorite-longshot bias** by category, after fees.
- **Scheduled releases** (CPI, jobs) vs economist consensus.

Note: finding a mispricing is research; profiting from it is much harder (fees, thin books, faster traders). Basic cross-venue arbitrage is crowded; combos, sports vs sharp books, and fee-adjusted bias are less so.

### 8.3 Direction C: Strategy Implementation and Backtesting

- Backtesting engine with realistic fills, **queue position**, fees, slippage (foundation for C).
- Market making (Avellaneda-Stoikov; Feil and Nendel, arXiv 2607.17991) using Direction A volatility and informed-flow measurements as inputs (vol sets spread width; informed flow sets when to widen or pull quotes; liquidity decides which markets to quote).
- Bias strategies if they survive fees; combo pricing; scheduled-release strategies.
- **Paper trading only** unless the lead explicitly approves otherwise. No club real-money trading.

### 8.4 Direction D: Trading Simulator and Practice Platform

- **Replay mode:** trade against real historical books (easy; market cannot react).
- **Reactive simulator:** simulated order flow (Poisson/Hawkes, queue-reactive) calibrated to our data, including convergence to 0 or 1 at resolution and news jumps (harder, more novel).
- Club competitions with leaderboard; guided learning scenarios.
- Coordinate with the club's Order Book Modeling team.

### 8.5 Direction E: Analytics Product

Packages A and B (and C/D outputs) for users. See the product map (Section 9). Differentiators: tested measurements and a graded track record, not dashboards.

### 8.6 Direction F: Derivatives and New Instruments

- Pricing options on event contracts (e.g., a "line lock": the right to buy a contract at a locked price over a window), which requires a tested volatility model.
- Term structures and implied distributions. Mostly theoretical; furthest from a product.

### 8.7 Staging (realistic for 3 to 5 people)

1. Level 0 → Level 1 → Direction A (core measurements + volatility benchmark).
2. One of B or C (combos is a strong pick if data exists).
3. Direction E (product) built from what A, B, C actually produced.
4. D and F later, or in collaboration with other club teams.

**Start the live forecast logger (track record) as soon as the first baseline model works.** History cannot be backfilled.

---

## 9. Product Map (target end state of Direction E)

Tools section is deferred. Keep the website lean; the research is the center of gravity, not React.

```
HOME
├── Hero                  (value proposition, Explore Markets / See the Research buttons, rotating live market card)
├── Live Market Ticker    (scrolling strip of clickable market cards)
├── Right Now Panels      (Most Volatile, Volatility Spikes, Thin Markets, Informed Flow)
├── Browse by Topic       (Fed & Economy, Elections, Geopolitics, Sports, Crypto, Culture)
├── Product Tour          (one scroll section each for Markets, Research, Data & API)
├── Featured Research     (headline benchmark finding with chart)
├── Trust Block           (open methodology, point-in-time history, changelog, independence)
├── Who It's For          (Traders, Market Makers, Researchers, Journalists)
├── Weekly Report Signup
└── Footer

MARKETS
├── All Markets           (sortable, filterable table: price, volume, volatility, liquidity grade, informed flow, days left)
├── Topics                (Fed & Economy, Elections, Geopolitics, Sports, Crypto, Culture)
├── Movers                (most volatile, biggest changes, new markets)
├── Watchlist
│
├── Contract Page         (opened by clicking any contract)
│   ├── Overview          (chart, live orderbook, trades, rules, other venue's price)
│   ├── Volatility
│   │   ├── Realized Volatility        (how much the price actually moved, in log-odds)
│   │   ├── Forward Belief Volatility  (expected movement, from the benchmark's best model for this category)
│   │   ├── Expected Move Range        (e.g. +7 / −4 points this week, showing upside vs. downside asymmetry)
│   │   ├── Volatility Surface         (by price level and time to resolution)
│   │   └── Track Record               (past forecasts vs. what actually happened)
│   ├── Liquidity         (cost to trade, cost to move the price, depth, grade, track record)
│   ├── Order Flow        (Informed Flow Meter, buy/sell pressure, large trades, track record)
│   ├── Moves & News      (jumps vs. drift, reaction speed, scheduled-release moves)
│   └── Related           (most correlated contracts)
│
└── Event Page            (opened by clicking any multi-outcome event)
    ├── Distribution      (probabilities across all outcomes)
    ├── Volatility        (how much the whole distribution may shift)
    ├── Implied Vol       (strike-ladder events only: implied volatility of the underlying,
    │                      plus sensitivity, e.g. points moved per $1,000 move in Bitcoin)
    ├── Term Structure    (curves for "by date X" ladders)
    ├── Contracts         (every outcome, linking to its Contract Page)
    └── Consistency       (outcomes not summing to 100%, dated contracts breaking logic)

RESEARCH
├── Volatility Benchmark
│   ├── Model Lineup      (realized logit vol, Xi et al. structural + GARCH+DR-AS, Dalen jump-diffusion,
│   │                      Stewart FBV, Black-Scholes digital IV for underlying-based contracts)
│   ├── Results           (winning model by category, horizon, price level, venue)
│   └── Live Scoreboard   (ongoing forecast accuracy, updated as contracts resolve)
├── Calibration           (how accurate markets have been historically)
├── Correlations          (interactive correlation matrix across markets)
├── Reports               (weekly report archive, deeper write-ups)
└── Methodology           (how every metric is calculated, glossary, changelog)

DATA & API
├── Downloads             (daily analytics files, CSV and Parquet; derived metrics only)
├── API                   (same metrics programmatically)
└── Access                (free for students and researchers)
```

Product principles:
- Every metric shows its **track record** (forecast vs realized).
- Plain-language labels for retail users (e.g., "Informed Flow Meter"); technical names live in Methodology.
- Raw orderbooks may be **displayed** for context but are never offered as downloads.

---

## 10. Track Record System (cross-cutting, high priority)

- Every forecast is written **at the moment it is made**: `forecast_id, model_id, model_version, market_id, made_ts, horizon, forecast_value(s), inputs_hash`.
- Append-only; never edited or deleted.
- When the horizon ends, a grader writes `realized_value, loss_qlike, winkler, graded_ts` to a separate table.
- Methodology changes are versioned, dated in a public changelog, and apply only going forward.
- Grade non-volatility metrics too: liquidity grade vs realized slippage on real large trades; informed-flow readings vs subsequent price moves; expected-move band hit rates.

---

## 11. Proposed Tech Stack and Repo Layout

### 11.1 Stack (proposed; confirm with lead)

- **Language:** Python 3.11+ for collectors, normalization, research, and API.
- **Async I/O:** `asyncio` + `websockets` / `aiohttp` (or `httpx`).
- **Storage:** raw JSONL (zstd-compressed) + Parquet (Hive-partitioned); **DuckDB** for queries; Polars and/or pandas for analysis.
- **Research:** NumPy, SciPy, statsmodels, `arch` (GARCH), scikit-learn; Jupyter for exploration (logic moves into `src/` once stable).
- **Testing:** pytest; property tests for book reconstruction.
- **Ops:** Docker, a small cloud VM, cron or a lightweight scheduler, Discord webhook alerts.
- **Frontend (later, lean):** React; API via FastAPI.
- **Version control:** Git/GitHub with PR reviews.

### 11.2 Suggested repo layout

> Superseded for now by the planned layout in Part I, E10, which keeps the flat scripts and grows a `pm/` package. Kept here as the long-term target.

```
pm-analytics/
├── PROJECT_HANDOFF.md        (this file)
├── README.md
├── pyproject.toml
├── .env.example
├── configs/
│   ├── universe.yaml         (tracked markets and selection rules)
│   └── venues.yaml           (endpoints, rate limits, fee schedule refs; verified dates)
├── src/
│   ├── collectors/
│   │   ├── kalshi_ws.py
│   │   ├── kalshi_rest.py
│   │   ├── polymarket_ws.py
│   │   └── polymarket_rest.py
│   ├── raw_store/            (append-only writers, partitioning)
│   ├── normalize/            (venue → shared schema, versioned)
│   ├── book/                 (order book reconstruction from snapshot + deltas)
│   ├── validate/             (all Section 7.5 checks + daily report)
│   ├── matching/             (cross-venue event matching)
│   ├── backfill/             (Polymarket-v1, Predexon loaders)
│   ├── metrics/              (Direction A features)
│   ├── models/               (volatility models)
│   ├── benchmark/            (evaluation loop, losses, DM tests)
│   ├── track_record/         (forecast logger, grader)
│   └── api/                  (later)
├── notebooks/
├── tests/
├── ops/                      (Dockerfiles, deploy scripts, alerting)
└── docs/
    ├── schema.md
    ├── methodology/
    └── changelog.md
```

### 11.3 First concrete tasks for Claude Code (Level 1)

> Historical. Items 4, 6, 7 and most of 8 are done for Polymarket. Current order of work is Part I, E1 and E10.

1. Scaffold the repo (layout above), `pyproject.toml`, linting, pytest.
2. Read current Kalshi and Polymarket API docs; write `docs/venue_notes.md` summarizing endpoints, auth, message formats, sequence semantics, book representation, fees, rate limits, combo/RFQ availability, with the date verified.
3. Define the shared schema in `docs/schema.md` and as typed models (e.g., dataclasses or pydantic).
4. Build the raw-store writer (append-only, partitioned, compressed).
5. Build the Kalshi WebSocket collector (snapshot + deltas + trades), with reconnect/resync.
6. Build the Polymarket WebSocket collector, same requirements.
7. Build book reconstruction with tests (snapshot + deltas → book; compare vs REST snapshot).
8. Build validation checks and the daily quality report.
9. Build backfill loaders (Polymarket-v1 first).
10. Containerize and deploy; add alerts.

---

## 12. Reading List for Level 2 (read only when a direction needs it)

- **Microstructure theory:** Kyle (1985); Glosten and Milgrom (1985); Hasbrouck, *Empirical Market Microstructure*; Bouchaud, Bonart, Donier, Gould, *Trades, Quotes and Prices*.
- **Order flow:** Cont, Kukanov, Stoikov (2014), "The Price Impact of Order Book Events"; Easley, López de Prado, O'Hara (2012), "Flow Toxicity and Liquidity in a High-Frequency World."
- **Prediction market volatility:** Xi, Moallemi, Pai, Wang (2026), arXiv 2607.08199; Dalen (2025), arXiv 2510.15205; Stewart (2026), SSRN 7220678; Archak and Ipeirotis (InTrade volatility).
- **Prediction market microstructure:** Qin and Yang, Polymarket-v1 (arXiv 2606.04217); Polymarket orderbook anatomy (arXiv 2604.24366); Bürgi, Deng, Whelan, "Makers and Takers"; Le, SSRN 6933527.
- **Prediction market basics:** Manski (2006); Snowberg and Wolfers (2010).
- **Forecast evaluation:** Gneiting and Raftery (2007), proper scoring rules; Patton (2011) on volatility loss functions; Diebold and Mariano (1995).
- **Statistics/time series:** Blitzstein and Hwang (Stat 110); Hyndman and Athanasopoulos, *FPP* (free); Tsay, *Analysis of Financial Time Series*.
- **Derivatives:** Hull, *Options, Futures, and Other Derivatives* (binary options, implied vol); Breeden and Litzenberger (1978).
- **Market making:** Avellaneda and Stoikov (2008); Feil and Nendel (2026), arXiv 2607.17991; Cartea, Jaimungal, Penalva.
- **Research hygiene:** López de Prado, *Advances in Financial Machine Learning* (backtest overfitting chapters); Bailey et al. (2014), "Pseudo-Mathematics and Financial Charlatanism."
- **Engineering:** McKinney, *Python for Data Analysis*; DuckDB docs.

---

## 13. Risks and Open Questions

| Risk / question | Mitigation |
|---|---|
| API details changed since research | Verify against official docs; record verification date in `configs/venues.yaml` |
| Collector downtime creates permanent holes | 24/7 server, auto-restart, alerts, backfill sources |
| Combo/RFQ data may not be exposed | Check in Level 1 before committing to combo research |
| Terms of use on publishing derived data or commercial use | Review Kalshi and Polymarket terms before any public release |
| Model implementations wrong | Test on simulated data with known parameters first; replicate Xi et al. before extending |
| FBV methodology not fully public | Implement what is published; contact the author |
| VPIN adaptation may fail validation | Publish honestly; do not ship an unvalidated meter |
| Look-ahead bias | Strict point-in-time joins; automated tests that fail on future data access |
| Scope creep (six directions) | Follow staging in Section 8.7 |
| Competitors expand (e.g., Belief Systems per-contract) | Start track record early; history cannot be backfilled |
| Literature moving fast | Re-check arXiv q-fin.TR and SSRN before each research phase |

---

## 14. Glossary

- **Orderbook (LOB):** list of resting buy (bid) and sell (ask) orders by price.
- **Spread:** best ask minus best bid.
- **Depth:** size available at or near the best prices.
- **Taker / maker:** taker crosses the spread (executes immediately); maker rests an order.
- **Snapshot / delta:** full book state / incremental change to it.
- **Belief volatility:** volatility of a contract's log-odds.
- **Forward belief volatility (FBV):** forward-looking term structure of expected probability movement (Stewart).
- **Jump-diffusion:** price process with continuous drift/diffusion plus sudden jumps.
- **VPIN / PIN:** measures of probability of informed trading.
- **OFI:** order flow imbalance.
- **Calibration / Brier score:** agreement between prices and realized outcome frequencies; squared-error score for probability forecasts.
- **QLIKE / Winkler / Diebold-Mariano:** volatility loss function / interval forecast score / test for significant forecast differences.
- **Point-in-time:** data or forecast exactly as known at that moment, never revised.
- **Combo / parlay:** multi-leg contract paying only if all legs resolve YES.
- **Strike ladder / dated ladder:** threshold contracts on an underlying / "by date X" contracts across dates.

---

*Part II last updated October 4, 2026. Part I last updated November 4, 2026. Update this file in the same PR as any change it describes.*
