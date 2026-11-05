# Prediction Market Volatility and Analytics Platform

The project plan: what we are building and why, who is building it, where it stands, and what comes next. For how to install, run and change the code, see [README.md](README.md).

_Last updated: November 5, 2026._

## Contents

- [The idea](#the-idea)
- [Team and club](#team-and-club)
- [Key decisions](#key-decisions)
- [What we dropped and why](#what-we-dropped-and-why)
- [Where the research stands](#where-the-research-stands)
- [Level 0: Foundations (done)](#level-0-foundations-done)
- [Level 1: The data pipeline](#level-1-the-data-pipeline)
- [Level 2: What the data lets us do](#level-2-what-the-data-lets-us-do)
- [Track record system](#track-record-system)
- [How the layers connect](#how-the-layers-connect)
- [Key concepts](#key-concepts)
- [Glossary](#glossary)
- [Reading list](#reading-list)
- [Risks and open questions](#risks-and-open-questions)
- [Meetings](#meetings)

## The idea

A prediction market price is the crowd's probability. A contract that pays \$1 if something happens and trades at 70 cents says "about a 70% chance". But the price alone does not tell you how much to trust it:

- Is it **jumpy**? How much is it likely to move (volatility)?
- Is it **real**? Is there money behind it, or would a small order move it (liquidity)?
- Is it being **pushed by people who know something** (informed flow)?

Kalshi and Polymarket now trade billions of dollars a month, and many tools already track their order books, large wallets and arbitrage. Very few measure these three things for every single contract, and nobody publicly tests which of the published methods actually work.

**Vision: build the measurement layer for prediction markets.** Concretely:

1. Collect our own order book and trade data and prove it is correct.
2. Measure volatility, liquidity, order flow and more for every contract.
3. Test which published methods actually forecast well, starting with volatility models.
4. Grade every forecast we make, publicly, against what actually happened.
5. Deliver it through a usable product (a website), built from whatever the research actually produces.

## Team and club

| | |
|---|---|
| Club | BoilerQuant, the quantitative finance club at Purdue University |
| Term | Fall 2026 |
| Lead | Dhruv Jhamb (applied math) |
| Team | 5 members plus the lead: Kashyap Maddala, Tarun Venkatesh, Ryan Challman, David Oprea, Peter Bossev. Mixed experience; the club includes freshmen new to the material |
| On the repo | Dhruv Jhamb, Kashyap Maddala (owns the GitHub repo), David Oprea |
| Venues | Polymarket first, then Kalshi |

Prerequisites given to members: Python, SQL, basic JavaScript and React; basic probability and statistics.

How we work: everyone should understand every part of the system, so there are no silos. Rough roles, adjusted to who is active: lead (methodology, models, benchmark design), research (models, liquidity, flow), data (pipeline, reliability), web (frontend and API, kept lean).

**Other BoilerQuant Fall 2026 projects that overlap** (chances to collaborate, not compete):

| Project | Overlap |
|---|---|
| Order Book Modeling (simulated order book with agents) | Direction D, the simulator |
| MacroCast (macro nowcasting with publicly graded predictions) | Shares the "grade forecasts publicly" idea, so that is not unique inside the club. Our angle: volatility for prediction markets, where nobody knows which model works |
| Options Pricing and Visualization Screener | Black-Scholes digital implied volatility and strike ladders |
| MD Feed Handler (Nasdaq ITCH to order book) | Same kind of problem as our Level 1 collectors |

## Key decisions

| Decision | What it means in practice |
|---|---|
| **The project runs in levels** | Level 0 foundations, Level 1 data pipeline, Level 2 research directions A to F. Level 2 waits until Level 1 is done |
| **Data integrity beats speed** | Raw data is saved exactly as received and never edited. Every table can be rebuilt from raw. We prove the data matches the exchange before using it |
| **No look-ahead, ever** | Anything computed for time $t$ uses only data timestamped at or before $t$. This is the most important research rule |
| **Raw data is infrastructure, not the product** | We collect it for our own research. We do not sell or redistribute raw order books |
| **Public outputs are derived analytics only** | Metrics, research and the track record, and only after checking Kalshi's and Polymarket's terms of use. Raw books may be shown for context, never offered as downloads |
| **Polymarket first, then Kalshi** | Polymarket's market data is public and needs no account. Kalshi needs written permission to store data and an API key |
| **Level 2 is broad, not volatility-only** | Volatility is one workstream inside Direction A. Every feature follows the same pattern: implement the method from the literature, measure it on all markets, test how well it forecasts or explains what happens next |
| **Start with 100 markets, then grow** | Top 100 by 24 hour volume, at most 5 per event (the first uncapped pick had 39 of 100 from one election map event). Grow to 500, then 1,000+ once validation passes. Quiet long-tail markets matter for later research |
| **Paper trading only** | No club real-money trading unless the lead explicitly approves |
| **The code is built like production software** | Tests, CI, code review, exact decimals, versioned outputs (Step H) |

## What we dropped and why

Do not build these unless the lead revives them.

| Idea | Why it was dropped |
|---|---|
| Selling raw order book data | Oddpool, Predexon and CryptoStruct already do it, and history cannot be backfilled |
| Cross-venue arbitrage scanner | Very crowded |
| Whale and wallet tracking | Very crowded |
| AI or LLM outcome forecasting, news-signal products | Crowded, and it is an edge claim we chose not to make |
| "Implied News Oracle" (LLM explains volatility spikes) | News attribution is taken; "this headline is underpriced" cannot be proven; too slow against professionals |
| Cross-venue volatility arbitrage | Broken premise: there are no options on these contracts, so volatility cannot be traded on its own |
| Greeks portfolio optimizer | An optimizer needs expected returns (edge) we do not have |
| Standalone "Greeks" on contract pages | Delta and gamma depend only on price; replaced by an expected move range |
| Market-wide dashboards section | Redundant with sortable market tables |
| LLM agent as market maker plus information trader | The two roles conflict; needs capital; crowded |
| Thematic benchmark indices | Belief Systems already does this |

Kept as possible later experiments: matching price jumps to **scheduled** events (CPI, FOMC, games); which venue leads price discovery; whether an outside fair-value estimate helps a market maker in thin markets; a risk visualizer (not optimizer) for portfolios of event contracts.

## Where the research stands

As researched in October 2026. This space moves monthly, so re-check before citing anything publicly.

**The model to beat.** Xi, Moallemi, Pai and Wang (Columbia), "Volatility in Prediction Markets: A Structural Approach" (arXiv 2607.08199), studied an hourly Kalshi panel (Aug 2021 to Apr 2026, about 880,000 out-of-sample forecasts). GARCH combined with a deadline-resolution and adverse-selection term (GARCH + DR-AS) was best or near-best across categories. Volatility peaks near 50 cents and rises as resolution approaches; economics contracts move smoothly, sports contracts jump.

**Already published, so not ours to claim:** calibration improves with volume and trader count; VPIN with true trade direction predicts worse accuracy and the Gibbs spread predicts better accuracy (Qin and Yang, Polymarket-v1); the tick rule and bulk volume classification are close to random for trade direction here, so **never build VPIN on inferred direction**; PIN and VPIN disagree in some Polymarket market types (Le); favorite-longshot bias on Kalshi (Bürgi, Deng and Whelan); Polymarket order book stylized facts (arXiv 2604.24366).

**Closest to us:** Belief Systems ("measurement layer for prediction markets"; thematic indices and Forward Belief Volatility, but not per contract at last check). Raw data vendors (Oddpool, Predexon, CryptoStruct) and retail signal tools (Alphascope, PrediEdge) cover other ground.

**What is still open (our contribution):**

1. A multi-model, cross-venue volatility benchmark on the same data, Kalshi and Polymarket.
2. Per-contract forward volatility, expected-move bands and liquidity grades for every contract.
3. A public, point-in-time, self-graded track record of every metric we publish.
4. Whether true-direction VPIN predicts **future volatility**, not just accuracy.
5. The calibration puzzle: thin markets (low volume) look less accurate, yet wider spreads predict **better** accuracy. Why?
6. Cross-market belief correlations; combo and parlay implied correlation (if the data is available).

## Level 0: Foundations (done)

**Goal:** every member can look at a live order book and explain it, and knows the venues well enough to build Level 1.

Four must-reads:

1. Larry Harris, *Trading and Exchanges*: chapters on orders, order-driven markets, liquidity and spreads.
2. Gould et al. (2013), "Limit Order Books", *Quantitative Finance*: sections 1 to 3.
3. Wolfers and Zitzewitz (2004), "Prediction Markets".
4. Kalshi and Polymarket API docs and rulebooks (orders, matching, fees, tick size, settlement, available data).

Done when every member can explain a live order book, spread and depth, what a 70 cent price means, and what data each venue's API provides. Meeting 2 (Oct 25) mapped the endpoints and ran a first listener on a live event. Level 0 is complete.

## Level 1: The data pipeline

**Goal:** clean, validated, continuously updated data from both venues that the team can trust completely. This is our research foundation, not a product.

### Polymarket

| # | Step | What it involves | Status |
|---|---|---|---|
| 1 | Raw store | Save every message, exactly as received, to hourly compressed files | Done |
| 2 | Pick markets | Top 100 by 24 hour volume, max 5 per event, market info saved, every pick kept | Done |
| 3 | Collector | Watch every picked market at once, reconnect with fresh books, re-pick daily, never quit | Done |
| 4 | Validation | Rebuild every book from raw, compare to Polymarket's own best prices and to 5 minute REST copies, find gaps and damage | Done |
| 5 | Clean tables | Raw into versioned Parquet tables, search with DuckDB; resync books from REST copies when they disagree | Done |
| H | Hardening | Shared `pipeline/` package, exact decimals, test suite, CI, logging, single config; ideas from David's branch; 172 tests, 96% coverage; fixed the Nov 5 mismatches and found the drops were the Mac sleeping | Done (Nov 5) |
| 6 | Server | Run 24/7 off a laptop (ask BoilerQuant or Purdue first, else rent one for roughly \$5 to \$15 a month with 100+ GB disk); auto-restart; hourly table builds and validation; daily quality report; alerts; daily raw backups to cheap cloud storage | **Next** |
| 7 | Outcomes and free history | Record how every market resolved; load free history (Polymarket-v1 dataset, price history API, on-chain fills) and reconcile it with our data | Not started |
| 8 | Scale | 100, then 500, then 1,000+ markets, measuring disk and load at each step | Not started |
| 9 | Two clean weeks | Two weeks of continuous collection with no unrepaired gaps | Not started |

**Results so far.** On Nov 3, our rebuilt books matched Polymarket's reported best prices on 644,154 of 644,154 live updates (100%) and matched 1,530 of 1,560 REST copies exactly at every level (98%). Most misses were one market where REST showed resting orders the live feed never sent; the pipeline now logs and resyncs those. On Nov 5: 677,466 of 677,478 updates (99.998%) and 618 of 620 REST copies. Step H found the 12 mismatches were our bug (an older full book overwrote a newer change) and fixed it. After hardening, all 63 hours (22 million messages) validate at 99.9997% of change batches and 99.97% of REST copies, and a 20 minute live run matched 100%. The "drops every 15 minutes" were the laptop sleeping with the lid closed (about 5.7 hours lost), which is why the server is next.

**Measured load (100 markets):** about 800 messages a second, about 0.26 GB an hour compressed, about 6 GB a day, about 90 GB per two weeks. This sets the size of the step 6 server.

Small open items: group repeated problems by market in the validation report.

### Kalshi

| # | Step | What it involves | Status |
|---|---|---|---|
| 0 | Permission and API key | Written permission from Kalshi to store their data (their terms require it), plus an account and API key. **Blocks everything below** | Not started (email not sent) |
| 1 | Raw store | Reuse the shared raw store, `raw/kalshi/` | Not started |
| 2 | Pick markets | Kalshi markets API, top by volume, cap per event | Not started |
| 3 | Collector | WebSocket with API key auth; use Kalshi's sequence numbers to detect gaps | Not started |
| 4 | Validation | Rebuild books and compare to the REST order book. Kalshi lists YES bids and NO bids; the YES ask is $1 - \text{best NO bid}$ | Not started |
| 5 | Clean tables | Same schema as Polymarket with a venue column; prices from cents to $[0, 1]$ | Not started |
| 6 | Server | Same server as Polymarket | Not started |
| 7 | Outcomes and history | Settlements; Kalshi's free trade history; Predexon's free tier for older tick data | Not started |
| 8 | Cross-venue matching | Pair the same events across venues and flag where settlement rules differ. Never assume two "same" markets settle the same way | Not started |
| 9 | Two clean weeks | As for Polymarket | Not started |

Steps 1 to 5 reuse the shared core built in Step H. Kalshi fees and combo/RFQ data availability must be checked against current docs when we start (combo data decides whether Direction B's combo work is possible).

### Level 1 is done when

- Both venues have run **two weeks** of continuous collection with **no unrepaired gaps**.
- Rebuilt books **match exchange snapshots**.
- Any contract's full price, book and trade history comes back in **one DuckDB query** (met for Polymarket in step 5).
- A **daily data quality report** runs automatically.
- **Free history is loaded and reconciled** with our own data.

## Level 2: What the data lets us do

Six directions. A is the base the others use.

```
Level 1: Data pipeline
        |
A: Measurement  ---->  B: Mispricing research
        |                       |
        v                       v
C: Strategies and backtesting
        |
D: Simulator            F: Derivatives
        |                       |
        v                       v
E: The product (packages everything above)
```

**A. Microstructure measurement (first after Level 1).** Measure, for every contract: volatility, spreads (quoted, effective, realized), depth and how fast it refills, price impact (Kyle's lambda, Amihud), order flow imbalance, informed flow (VPIN and PIN with true trade direction only), jumps versus drift, which venue moves first, calibration, who makes money (makers or takers), behavior near resolution, fee-adjusted break-even moves, and how concentrated the liquidity is. **Flagship: the volatility benchmark.** Replicate Xi et al. on Kalshi first (if our numbers differ, find out why before going on). Then add Dalen's logit jump-diffusion, Stewart's Forward Belief Volatility (as far as the published method allows) and Black-Scholes digital implied volatility for strike ladders. Run all of them on Kalshi and Polymarket, score with QLIKE and the Winkler score, test differences with Diebold-Mariano, and break results down by venue, category, horizon (1 hour, 1 day, 1 week), price level and time to resolution. Write it up paper-style.

**B. Mispricing research.** Is a price wrong? Combo and parlay prices against the product of their legs (implied correlation); cross-venue gaps after fees and rule differences; multi-outcome events summing above 100%; dated ladders where a later date is priced below an earlier one; strike ladders against options markets (Bitcoin, S&P); sports contracts against sharp sportsbook lines; favorite-longshot bias after fees; scheduled releases against economist consensus. Finding a mispricing is research; profiting from it is much harder.

**C. Strategies and backtesting.** Does any of it make money after costs? A backtester with realistic fills, queue position, fees and slippage. Market making that uses Direction A measurements (volatility sets spread width, informed flow sets when to widen or pull quotes, liquidity decides which markets to quote). Bias and scheduled-release strategies if they survive fees. Paper trading only.

**D. Trading simulator.** Replay mode (trade against real historical books) first, then a reactive simulator calibrated to our data, including convergence to 0 or 1 at resolution and news jumps. Club competitions with a leaderboard. Coordinate with the club's Order Book Modeling team.

**E. Analytics product.** A public website that packages A and B (and what C and D produce). Differentiator: tested measurements and a graded track record, not dashboards. Rough shape: Markets (sortable table of every contract; contract pages with volatility, expected move range, liquidity, order flow, moves and track record; event pages with the outcome distribution and consistency checks), Research (benchmark results, live scoreboard, calibration, methodology and changelog), and Data and API (derived metrics only). Plain-language labels for users, technical names in the methodology. Keep the website lean: the research is the center of gravity.

**F. Derivatives.** Pricing options on event contracts (for example a "line lock": the right to buy a contract at a locked price for a while), term structures and implied distributions. Needs a tested volatility model. Mostly theoretical and furthest from a product.

**Staging** (realistic for a team of 6):

1. Level 0, then Level 1, then Direction A (core measurements and the volatility benchmark).
2. One of B or C (combos is a strong pick if the data exists).
3. Direction E, built from what A, B and C actually produced.
4. D and F later, or together with other club teams.

Start the live forecast logger (the track record) as soon as the first baseline model works. History cannot be backfilled.

## Track record system

The most defensible new thing we can offer is an honest, public record of how good our forecasts were.

- Every forecast is written **at the moment it is made**: forecast ID, model and version, market, time made, horizon, forecast values, and a hash of the inputs.
- Forecasts are append-only. Never edited or deleted.
- When the horizon ends, a grader writes the realized value and the scores (QLIKE, Winkler) to a separate table.
- Method changes are versioned, dated in a public changelog, and only apply going forward.
- Non-volatility metrics are graded too: liquidity grades against real slippage on large trades, informed-flow readings against later price moves, expected-move bands against how often prices stayed inside them.

## How the layers connect

| Layer | What it holds | Built from | Rebuildable? |
|---|---|---|---|
| Raw | Every exchange message exactly as received | The exchange | No (back it up) |
| Clean | Typed, validated tables: book changes, full books, trades, top of book, markets, outcomes | Raw | Yes |
| Features | Per-contract measurements (volatility, spreads, depth, flow, and so on), point-in-time | Clean | Yes |
| Research and backtests | Benchmark runs, studies, strategy backtests, forecast logs and grades | Features and clean | Yes (except the forecast log, which is append-only by design) |
| Paper trading | Simulated orders and fills against live data | Features and live data | No (it is a record) |

**Internal versus public.** An internal dashboard holds what only the team should see: backtests, paper trading, risk, and pipeline health. The public product shows derived measurements, research write-ups and the graded track record. It never shows live trading signals and never offers raw data.

Research note: a catalog of trading strategy families and their prediction market versions (written Nov 3) is kept with the project's local notes, not in this repo. Its key idea: Direction A measures how trading works, B asks whether a price is wrong, and C tests whether either makes money after costs. Real strategies combine features in four roles: predictors, filters, cost and risk.

## Key concepts

**Contract and event.** A contract (market) is one yes/no outcome that pays \$1 if YES and \$0 if NO, with price $p \in (0, 1)$. An event groups related contracts (for example "Fed December decision: hold / cut 25 / cut 50"). A **strike ladder** is an event whose contracts are thresholds on a number (Bitcoin above \$X). A **dated ladder** is "happens by date X" for several dates. On Polymarket each outcome is its own token with its own order book.

**Log-odds.** Prices are stuck between 0 and 1, so volatility work happens in log-odds, which can take any value:

$$x = \log\left(\frac{p}{1-p}\right), \qquad p = \frac{1}{1+e^{-x}}$$

Clip prices to $[\epsilon, 1-\epsilon]$ first (with a documented $\epsilon$) so the transform never hits infinity.

**Realized volatility (the baseline).** Sample log-odds at regular intervals, take the changes $\Delta x_i = x_i - x_{i-1}$, and average their squares:

$$\hat{\sigma}^2 = \frac{1}{n}\sum_{i=1}^{n} (\Delta x_i)^2$$

**Expected move in probability points.** For belief volatility $\sigma$ over horizon $h$, small moves are about

$$\text{Expected move} \approx p(1-p)\,\sigma\sqrt{h}$$

Upside and downside differ away from 50 cents, so report both by mapping $x \pm \sigma\sqrt{h}$ back through the logistic function.

**Scoring volatility forecasts.** With forecast $\hat{\sigma}^2$, realized variance $RV$ and ratio $r = RV/\hat{\sigma}^2$, the QLIKE loss is

$$L = r - \log r - 1$$

It is zero for a perfect forecast and punishes under-predicting risk more than over-predicting it. We also compute the Winkler interval score (to compare with Xi et al.) and use Diebold-Mariano tests to check whether one model is really better than another.

**VPIN (informed flow).** Group trades into buckets of equal volume $\tau = 1, \dots, n$, split each bucket's volume into buys $V^{buy}_\tau$ and sells $V^{sell}_\tau$ using the **true** taker side, and average the imbalance:

$$\text{VPIN} = \frac{1}{n}\sum_{\tau=1}^{n} \frac{|V^{buy}_\tau - V^{sell}_\tau|}{V_\tau}$$

Adapting it to contracts that end at 0 or 1 is an open problem.

## Glossary

| Term | Meaning |
|---|---|
| Order book | Resting buy orders (bids) and sell orders (asks), by price |
| Spread | Best ask minus best bid |
| Depth | Size available at or near the best prices |
| Maker / taker | A maker's order rests in the book; a taker's order crosses the spread and trades immediately |
| Snapshot / delta | The full book / one change to it |
| Resync | Replacing our rebuilt book with an exchange copy when they disagree, then replaying later changes |
| Universe | The set of markets we are watching, and its history |
| Belief volatility | Volatility of a contract's log-odds |
| Forward Belief Volatility (FBV) | A forward-looking term structure of expected probability movement (Stewart) |
| Jump-diffusion | A price process with smooth movement plus sudden jumps |
| VPIN / PIN | Measures of the probability of informed trading |
| OFI | Order flow imbalance |
| Calibration / Brier score | Whether prices match how often things actually happen / a squared-error score for probability forecasts |
| QLIKE / Winkler / Diebold-Mariano | Volatility loss / interval forecast score / test for whether two forecasts really differ |
| Point-in-time | Data or a forecast exactly as known at that moment, never revised |
| Look-ahead | Accidentally using information from the future; banned |
| Combo / parlay | A multi-leg contract that pays only if every leg resolves YES |

## Reading list

Read only when a direction needs it.

- **Microstructure theory:** Kyle (1985); Glosten and Milgrom (1985); Hasbrouck, *Empirical Market Microstructure*; Bouchaud, Bonart, Donier and Gould, *Trades, Quotes and Prices*.
- **Order flow:** Cont, Kukanov and Stoikov (2014), "The Price Impact of Order Book Events"; Easley, López de Prado and O'Hara (2012), "Flow Toxicity and Liquidity in a High-Frequency World".
- **Prediction market volatility:** Xi, Moallemi, Pai and Wang (2026), arXiv 2607.08199; Dalen (2025), arXiv 2510.15205; Stewart (2026), SSRN 7220678; Archak and Ipeirotis (InTrade volatility).
- **Prediction market microstructure:** Qin and Yang, Polymarket-v1 (arXiv 2606.04217); Polymarket order book anatomy (arXiv 2604.24366); Bürgi, Deng and Whelan, "Makers and Takers"; Le, SSRN 6933527.
- **Prediction market basics:** Manski (2006); Snowberg and Wolfers (2010).
- **Forecast evaluation:** Gneiting and Raftery (2007); Patton (2011); Diebold and Mariano (1995).
- **Statistics and time series:** Blitzstein and Hwang (Stat 110); Hyndman and Athanasopoulos, *Forecasting: Principles and Practice*; Tsay, *Analysis of Financial Time Series*.
- **Derivatives:** Hull, *Options, Futures, and Other Derivatives*; Breeden and Litzenberger (1978).
- **Market making:** Avellaneda and Stoikov (2008); Feil and Nendel (2026), arXiv 2607.17991; Cartea, Jaimungal and Penalva.
- **Research hygiene:** López de Prado, *Advances in Financial Machine Learning* (backtest overfitting); Bailey et al. (2014), "Pseudo-Mathematics and Financial Charlatanism".
- **Engineering:** McKinney, *Python for Data Analysis*; DuckDB docs.

## Risks and open questions

| Risk | Mitigation |
|---|---|
| Collection only runs while a laptop is awake, so every hour off is a permanent hole | Step 6 server with auto-restart, alerts and backups; free history to fill old gaps |
| Raw data lives on one machine | Daily off-machine backups from step 6 on |
| Collection stops when the machine sleeps | Run on an always-on server (step 6). The collector detects sleeps, notes them and reconnects at once |
| Exchange-side inconsistencies (REST shows orders the feed never sent) | Log and resync; keep measuring |
| Disk growth at 500 and 1,000+ markets | Measure before each scale step |
| API details change | Check official docs before relying on any detail; record when it was checked |
| Kalshi permission takes time or is refused | Ask early; Polymarket work does not depend on it |
| Terms of use limit what we can publish | Check both venues' terms before any public release |
| Model implementations are wrong | Test on simulated data with known answers; replicate Xi et al. before extending |
| FBV method not fully public | Implement what is published; contact the author |
| VPIN adaptation fails validation | Publish honestly; never ship an unvalidated meter |
| Look-ahead bias | Strict point-in-time joins; tests that fail on future data |
| Scope creep across six directions | Follow the staging order |
| Competitors move into per-contract analytics | Start the track record early; history cannot be backfilled |

**Open questions**

- Where does the step 6 server live: BoilerQuant or Purdue, or a rented one?
- When does the Kalshi permission email go out, and who sends it?
- Does Kalshi's API expose combo and RFQ data?
- Which Polymarket venue and APIs apply to us (a US-regulated Polymarket venue also exists)?
- What exactly may we publish under each venue's terms?

## Meetings

| Meeting | Date | Notes |
|---|---|---|
| 1 | Oct 3, 2026 | First meeting |
| 2 | Oct 25, 2026 | Endpoints mapped; first listener ran on a live event |
| 3 | **Sunday, Nov 8, 2026** | Moved from Nov 1. Focus: the data pipeline |
