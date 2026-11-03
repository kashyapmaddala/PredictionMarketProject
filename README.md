# PROJECT HANDOFF: Prediction Market Volatility and Analytics Platform

> **Read this entire file before writing any code.** It is the single source of truth for what this project is, what has been decided, what has been scrapped, and what comes next. If anything in the codebase conflicts with this file, ask the project lead before changing direction.

---

## 0. How to Use This File (instructions for Claude Code)

1. **Current phase:** Level 0 is in progress, and **Level 1 (Data Pipeline) is the next thing to build.** Do not start Level 2 work until Level 1's "done" criteria (Section 7.8) are met, unless the lead explicitly asks.
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
| **Status** | Project approved and presented to the club; team forming; Level 0 underway; Level 1 next |

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

## 7. Level 1: The Data Pipeline (NEXT TO BUILD)

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

Implementation note (November 3, 2026): the first offline Polymarket WebSocket
normalizer is available. See [the schema and run instructions](#15-running-the-normalizer-polymarket-ws-v1)
for supported messages, Parquet output, tests, and remaining scope. This is a
partial Level 1 implementation; Section 7.8's completion criteria still apply.

1. Scaffold the repo (layout above), `pyproject.toml`, linting, pytest.
2. Read current Kalshi and Polymarket API docs; record venue notes in this README summarizing endpoints, auth, message formats, sequence semantics, book representation, fees, rate limits, combo/RFQ availability, with the date verified.
3. Define the shared schema in this README and as typed models (e.g., dataclasses or pydantic).
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

*Last updated: October 4, 2026. Update this file whenever a decision changes, and record the change in the implementation changelog below.*


---

## 15. Running the Normalizer (polymarket-ws-v1)

This is the first Level 1 normalization increment, following README Sections
7.3 and 11.3. It reads the existing collector envelope without changing raw
files or the collector. Python 3.11+ is required.

### Run

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m pytest test_normalize.py
.venv/bin/python normalize.py data/raw/polymarket/2026-11-02 data/clean/2026-11-02-v1
```

Use closed files only, under a root containing only Polymarket WebSocket data.
Plain JSONL and gzip JSONL are supported. Do not point this command at the
Gamma metadata directory. The batch checks source hashes before and after
reading to catch concurrent changes, but this is not a live-file tailer.

Output is `<table>/venue=polymarket/date=YYYY-MM-DD/part-00000.parquet`.
The date uses receive time in UTC. A manifest contains source hashes, output
hashes, version and row counts. Publication uses a staging directory and rename.
Identical reruns are a no-op after verification. Changed inputs or output require
a new destination; no raw or existing derived data is overwritten. Query one
published dataset at a time to avoid counting overlapping rebuilds twice.

### Common fields

Every row carries `normalizer_version`, `venue`, `source_file` (relative to the
input root), `source_line` (one-based), `event_index` (zero-based within a frame),
and `recv_ts_ns` (integer UTC Unix nanoseconds). Market rows also carry
`exchange_ts_ns` (nullable, milliseconds converted to nanoseconds), `market_id`
(Polymarket condition ID, not Gamma numeric ID), and `asset_id` (outcome trading
identifier). YES and NO books must remain separate by asset ID.

Source location plus event index identifies an observation, not a unique
exchange trade. Delta rows additionally use `change_index`. Preserve capture
order when replaying. Exchange timestamps alone cannot order tied events.
No sequence number is synthesized; a hash is not a sequence number.

### Tables

- `books_snapshots`: one row per full asset snapshot, with `bids` and `asks`
  as lists of price/size structs and nullable `book_hash`. Empty books are
  retained because they reset previous state.
- `book_deltas`: one row per price change, with `side` (`bid` or `ask`), `price`,
  `new_size`, `change_index`, and nullable `book_hash`. New size is absolute,
  not additive; zero means remove the level.
- `trades`: last-trade observations with `price`, nullable `size`,
  `reported_side`, and nullable `transaction_hash`. No unique trade ID or
  verified taker side is invented. Do not use these rows for VPIN without
  separate direction verification and reconciliation.
- `collector_events`: non-message envelopes, preserving `kind` and JSON
  `payload`, including reconnect and subscription notes.
- `unsupported_events`: other exchange events, preserving their type and JSON
  payload. Tick-size and lifecycle events currently appear here. Monitor this
  table rather than assuming complete message coverage.

Prices and quantities are exact decimal strings in Parquet, avoiding implicit
binary floating-point conversion or an unverified fixed scale. Convert explicitly
to a suitable DECIMAL type for arithmetic. Parsing rejects negative, nonfinite,
and out-of-range prices. Endpoints 0 and 1 are preserved for later quality
checks, not clipped by normalization. Malformed known messages fail the batch
with source context; no partial dataset is published.

### Remaining scope

This normalization increment does not implement Gamma market/event/outcome normalization,
Kalshi, tick-aware validation, trade reconciliation, or daily
quality reports. It performs no metadata join, so no future metadata is attached
to older observations. Future metadata joins must use when records were known.
The initial reconstruction implementation is described in Section 16.

The collector currently records connection IDs on lifecycle notes but not on
exchange frames. A future collector change should attach a connection/session
identifier to every frame so reconstruction can reliably invalidate affected
books on reconnect. Existing captures cannot fully recover that information.

Parquet byte reproducibility is tested within one dependency environment.
Pin dependency versions for archived production runs; library upgrades can
change encoding even if logical rows remain equivalent.

### Venue verification notes

Checked November 3, 2026 against the official
[real-time data documentation](https://docs.polymarket.com/market-data/realtime-data).
The README's legacy market-channel documentation URL redirects to this page.

The API section still shows direct wire messages using `event_type`, `market`,
`asset_id`, and millisecond timestamps. Those are the messages our collector
stores. SDK payloads use different wrappers and are not accepted by this adapter.

This increment handles book snapshots, price-change arrays, and last-trade
messages. It retains condition and outcome identifiers separately. Unknown event
types remain visible in an auxiliary table. No exchange sequence or unique trade
identifier is assumed. Trade direction needs separate verification before research.

Collector follow-up: the current docs expose unsubscribe frames, while a comment
in listener.py says unsubscribe is unavailable. This normalization increment does
not alter collection behavior. Full authentication, limits, fee, metadata, Kalshi,
and combo/RFQ verification remains outstanding.

### Implementation changelog: November 3, 2026

- Added the first offline Polymarket normalization increment for existing raw
  WebSocket envelopes: snapshots, absolute-size updates, and trade observations.
- Added versioned Hive-partitioned Parquet output, source provenance, manifests,
  atomic batch publication, explicit unsupported events, and normalization tests.
- Documented scope and remaining Level 1 work in this section. Collection behavior
  and raw storage format remain unchanged.

- Consolidated normalization into root-level scripts and documentation into this
  README to retain the repository's existing flat layout. The Section 11.2 layout
  remains a future suggestion, not the current implementation.

## 16. Replaying Order Books

```sh
python reconstruct.py data/clean/2026-11-03-v1
python -m pytest -q test_normalize.py test_reconstruct.py
```

`reconstruct.py` verifies the normalized dataset's Parquet hashes against its
manifest, replays records, and prints final books and replay issues as JSON.
It does not modify the input dataset. Use one output of the normalizer from a
single collector. Records are sorted in memory, so this first implementation
is intended for development recordings, not multi-week archives.

Capture order is `(source_file, source_line, event_index, change_index)`.
Source filenames must be chronological, as in the existing day/hour layout.
Do not combine independently normalized datasets with overlapping relative source
paths. Neither exchange timestamps nor receive clocks override file capture
order. Duplicate positions are rejected.

Books are keyed by venue, market ID, and asset ID. Snapshots replace both sides;
updates set absolute sizes; zero deletes a level. Trades do not separately
subtract volume. Prices and quantities use Decimal arithmetic. Missing sides
produce null quotes, spreads, and midpoints. Locked or crossed books are flagged
and their spread and midpoint are withheld.

An update before a snapshot is reported and ignored. Reconnects, disconnects,
subscription changes, and unsupported exchange messages invalidate existing books
for that venue, because exchange frames currently lack connection IDs. Fresh
snapshots restore synchronization per asset. Invalid books expose no quotes or
levels, avoiding accidental use of stale data. A clean `stopped` event preserves
the last historical book and its original timestamps. Separate `collection_status`
and `collection_status_recv_ts_ns` fields describe the last recorded lifecycle
event, not current connectivity. Stopping never restores an already invalid book.
Updates after a stop require a new collection session and snapshot.

For step-by-step inspection in Python, use `Reconstructor.apply(table, row)`
on ordered records and inspect `engine.result()` after the desired event.
`synchronized` means a snapshot and uninterrupted supported replay are available;
it does not certify absence of exchange gaps or agreement with REST snapshots.
Snapshot reconciliation, sequence/gap validation, tick checks, scalable streaming,
and historical derived-table output remain future work.

### Reconstruction changelog: November 3, 2026

- Added root-level `reconstruct.py` and `test_reconstruct.py` for deterministic
  offline replay, lifecycle invalidation, quote summaries, and integration tests.

- Corrected clean-stop handling: retain historical quotes and depth while recording
  collection status separately; disconnects still require a fresh snapshot.

## 17. Validating a Normalized Dataset

```sh
python validate.py data/clean/2026-11-03-v1
python validate.py data/clean/2026-11-03-v1 --lag-warning-ms 10000
python -m pytest -q test_normalize.py test_reconstruct.py test_validate.py
```

The validator prints a JSON report and exits with code 1 on errors, or 0 for
warnings/informational findings only. Errors include manifest/hash/schema/count
mismatches, malformed records, duplicate capture positions, invalid levels, and
locked or crossed reconstructed books. Checks run after complete exchange events,
so temporary intermediate states within a multi-level update are not flagged.

Warnings include collection interruptions, unsupported messages, updates without
usable snapshots, missing exchange timestamps, receive-clock regressions, and
negative or unusually large exchange-to-receive timestamp differences. The default
positive difference threshold is 5,000 ms. These differences are not measurements
of pure network latency: snapshot age and clock skew also contribute. Reported
percentiles use the sorted sample at floor((n-1)*q); samples are normalized rows,
so multi-level messages can contribute more than one sample.

Empty book sides and clean collector stops are informational. The report includes
row counts, observed asset count, timestamp statistics, and source locations for
row-level findings. It also warns when a subscribed asset has no snapshot anywhere
in this dataset. That coverage check is dataset-wide, not a guarantee that each
subscription session received every required snapshot. Without subscription notes,
the expected universe is unknown.

Malformed records prevent replay instead of allowing misleading book results.
`replay_checked` indicates whether replay ran. A report with no errors establishes
only the checks implemented here. It does not certify gap-free collection, valid
ticks, trade/book consistency, correct settlement metadata, or agreement with
REST snapshots/on-chain fills. Raw-file hashes in the manifest are not rechecked
against raw storage by this command; derived Parquet hashes are checked.

Like reconstruction, this version loads records and findings into memory and is
intended for development recordings. Use one normalized dataset from a single
collector with chronological source filenames. Continuous reporting, bounded-memory
processing, and external reconciliation remain Level 1 work.

### Validation changelog: November 3, 2026

- Added root-level `validate.py` and `test_validate.py` for dataset integrity,
  replay consistency, timestamps, collection continuity, and coverage reports.
