# Momentum Day Trading Engine (IBKR-Only)

---

## Project Directory Structure (IBKR-Only)

```
main.py
README.md
requirements.txt
config/
  __init__.py
  constants.py
  governance.py
  settings.py
data/
  __init__.py
  cache.py
  fetcher.py
  health.py
  pipeline.py
decision/
  __init__.py
  engine.py
events/
  __init__.py
  bus.py
  types.py
  sinks/
execution/
  __init__.py
  engine.py
  halt_machine.py
  integrity_gate.py
  lifecycle.py
  monitor.py
  order_manager.py
  orders.py
  reconciliation.py
  slippage.py
  startup_recovery.py
  supervisor.py
intelligence/
  __init__.py
  news_validator.py
logs/
  (log and report files)
news/
  __init__.py
  ingestor.py
  tracker.py
risk/
  __init__.py
  manager.py
  portfolio_controller.py
  regime.py
scanner/
  __init__.py
  demand.py
  filters.py
  movers.py
  rvol.py
  universe_manager.py
  universe.py
selection/
  __init__.py
  selector.py
signals/
  __init__.py
  setup.py
  structure.py
  vwap.py
tests/
  (comprehensive pytest suite)
tools/
  (reporting, replay, and utility scripts)
trade_log/
utils/
```

---
# Momentum Day Trading Engine (IBKR-Only)

An institutional-grade, event-driven momentum day trading engine for Interactive Brokers (IBKR). This engine is designed for deterministic, risk-managed, and auditable execution, with all credentials and sensitive information managed securely via `.env` files. No legacy or alternative broker code remains—this project is IBKR-exclusive and production-hardened.

---

## Requirements to Run

- **Interactive Brokers (IBKR) account** (live or paper trading enabled)
- **IBKR API access** (TWS or IB Gateway) with proper permissions
- **.env file** with IBKR credentials (see `.env.example`)
- **Python 3.8+**
- **All required Python packages** (`pip install -r requirements.txt`)
- (Optional) **Discord webhook URL** for trade notifications

Before running, ensure your IBKR account is fully set up for API trading and that you have tested connectivity with TWS or IB Gateway. Consult Interactive Brokers documentation for API setup and security best practices.

---

## Security & Credentials

- **All credentials and sensitive information must be managed via a `.env` file.**
- Never commit your real `.env` file to version control. Use `.env.example` as a template for safe sharing.
- The `.gitignore` is preconfigured to exclude `.env` and other sensitive files.
- All IBKR credentials (username, password, account ID) are loaded securely at runtime.
- Discord webhook URLs (if used for notifications) should also be stored in `.env`.
- **Warning:** Hardcoding credentials or leaking secrets in logs or code is a critical security risk. Always use environment variables and review your repository before sharing or publishing.

---

## Legal Disclaimer

This project is provided for informational and educational purposes only. It does not constitute financial advice, investment advice, or a recommendation to buy or sell any securities. Use of this software is at your own risk. The authors and contributors accept no liability for any loss, damage, or stress resulting from the use of this codebase. You are solely responsible for complying with all applicable laws, regulations, and brokerage terms.

By using this project, you acknowledge and agree that you assume full responsibility for any trading decisions and outcomes. Always consult with a qualified financial advisor before making investment decisions.

---

## How It Works: Dynamic, Real-Time IBKR-Driven Trade Selection

- The engine continuously ingests live 1-minute bars and quotes from IBKR, dynamically scanning the entire market for high-momentum opportunities.
- A dynamic universe scanner (with movers, gainers, and news catalysts) builds a ranked candidate list in real time, using hard filters and scoring functions.
- The decision engine applies strict, deterministic gates (liquidity, VWAP, regime, slippage, risk) to select the best trades, with no randomness.
- All trade selection, sizing, and execution is fully auditable, deterministic, and replayable.
- The engine is designed for institutional reliability, with full crash safety, audit trails, and risk controls.

---

## Architecture Overview

```
Market Data (1m bars)
• IBKR (Interactive Brokers)     ← sole authoritative feed (live/paper)
        ↓
Bar Pipeline (MTF resample)      ← 1m → 5m, 15m, 1h, 4h, 1D (in-memory)
        ↓
Dynamic Universe Scanner         ← Movers intake + hard filters
        ↓                           → DemandScore → Top 15
Momentum Setup Detection         ← VWAP Reclaim + PMH/HOD Break (5m + 1m)
        ↓
Deterministic Trade Selection    ← DemandScore → SQS → spread (no randomness)
        ↓
Decision Engine                  ← regime gate → slippage gate → risk gate
        ↓
Execution (1m trigger)           ← OrderManager (TTL/C-R/partial) → IBKR Broker Adapter
        ↓
Trade Lifecycle Machine          ← OPEN → PARTIAL1 → PARTIAL2 → TRAILING → CLOSED
        ↓
Risk Management + Logging        ← fixed-R stops, full audit trail
```

---

## Codebase Structure & Module Purposes

- **main.py**: Entry point. Handles CLI, environment, preflight safety checks, and launches the trading engine in live, paper, scan-only, or single-cycle mode. Enforces IBKR as the sole data and execution authority.

- **config/**
  - `settings.py`: Centralizes all tunable parameters (risk, execution, scanner, etc.).
  - `constants.py`: Stores fixed labels and status strings.
  - `governance.py`: Handles run manifests, config hashing, and evaluation reports for auditability and reproducibility.

- **data/**
  - `fetcher.py`: Adapters for fetching 1m bars and quotes from IBKR (and optionally yfinance for scan-only). Handles pacing, normalization, and error resilience.
  - `cache.py`: In-memory bar cache for fast, session-scoped access.
  - `pipeline.py`: Multi-timeframe (MTF) resampling and bar pipeline logic.

- **scanner/**
  - `dynamic_universe.py`: Dynamically discovers top gainers and movers from IBKR in real time.
  - `universe.py`: Orchestrates the candidate universe, merging movers and seed tickers.
  - `filters.py`: Applies hard filters (price, volume, rvol, spread, catalyst).
  - `demand.py`: Computes DemandScore and SetupQualityScore for ranking.
  - `rvol.py`: Calculates session-aware RVOL (RTH, PM, AH).
  - `movers.py`: Ingests and caches high-momentum tickers from IBKR.

- **signals/**
  - `vwap.py`: Computes intraday and anchored VWAP.
  - `structure.py`: Detects higher highs/lows, pivots, breakouts, ATR, and other price structure features.
  - `setup.py`: Implements the momentum setup detector (VWAP reclaim, PMH/HOD break, etc.).

- **selection/**
  - `selector.py`: Deterministically selects the best trade candidate(s) based on scores and tie-breakers.

- **decision/**
  - `engine.py`: Applies all entry gates (liquidity, VWAP, regime, slippage, risk) and produces the final trade decision.

- **risk/**
  - `manager.py`: Handles position sizing, fixed-R stops, cooldowns, and regime passthrough.
  - `regime.py`: Detects market regime (TREND/CHOP/RANGE) using ADX and SPY range.
  - `portfolio_controller.py`: Enforces portfolio-level risk and exposure constraints.

- **execution/**
  - `engine.py`: The main 1-minute event loop, orchestrating the full trading cycle.
  - `orders.py`: IBKR broker adapter and order state machine.
  - `order_manager.py`: Manages order lifecycle (PENDING→SUBMITTED→PARTIAL→FILLED/CANCELLED/STUCK/REPLACED).
  - `lifecycle.py`: Manages trade lifecycle (OPEN→PARTIAL1→PARTIAL2→TRAILING→CLOSED).
  - `slippage.py`: Monitors and enforces slippage policies.
  - `halt_machine.py`: Implements halt/resume logic for tickers.
  - `integrity_gate.py`: Prevents runaway rejection loops and enforces circuit breakers.
  - `reconciliation.py`: Reconciles broker state with internal state.
  - `startup_recovery.py`: Recovers open broker orders on restart.

- **events/**
  - `bus.py`, `types.py`, `sinks/`: Implements the domain event bus, event types, and event sinks (JSONL, CSV, console) for full auditability and replay.

- **news/**
  - `ingestor.py`, `tracker.py`: Ingests and tracks news catalysts, integrating them into the candidate universe.

- **tools/**
  - `replay_report.py`, `report_card.py`, etc.: Utilities for generating reports, diagnostics, and replaying historical sessions.

- **utils/**
  - `time_utils.py`, `math_utils.py`, `spread_policy.py`: Helper functions for time, math, and spread policy calculations.

- **tests/**
  - Comprehensive pytest suite covering all subsystems for reliability and determinism.

---


## Capital Allocation Policy

The engine computes entry quantity in a strict, deterministic sequence to ensure risk management and auditability:

1. **Risk-based base size:**
  - `qty_base = risk_based_qty(account, risk_per_trade_pct, R_distance)`
2. **Capital allocation caps:**
  - `qty_cap1 = floor((equity * max_notional_per_trade_pct) / entry_price)`
  - `qty_cap2 = floor(max_trade_notional / entry_price)` (if configured)
  - `qty_cap3 = floor((dollar_volume * liquidity_notional_cap_pct_of_dvol) / entry_price)`
  - Optional share-liquidity cap from recent volume
3. **Final deterministic size:**
  - `qty_final = max(1, min(qty_base, qty_cap1, qty_cap2, qty_cap3, ...))`
4. **Portfolio exposure fit:**
  - If adding `qty_final` breaches gross/net/leverage caps, qty is reduced deterministically to fit, otherwise blocked.

When any cap applies, structured events include the cap reason and cap values for full auditability and compliance.

---
## System Guarantees

- **Single-source configuration**: All tunable parameters are defined in `config/settings.py`. No subsystem hardcodes trading parameters.
- **Deterministic execution**: No randomness in candidate ranking or trade selection. Tie-breaking is alphabetical by ticker. All time-based logic accepts an injectable `now` parameter for deterministic replay.
- **Single slippage authority**: Slippage is applied exactly once by `OrderManager.submit()`. The IBKR broker adapter never modifies submitted prices.
- **State-machine driven trading**: Orders and trades follow strict state models. Every state change emits a structured domain event via `EventBus`.
- **Single authoritative audit stream**: All events are logged for replay and audit.
- **Crash safety**: On restart, open broker orders are reconstructed to prevent duplicates.
- **Feed authority guarantee**: IBKR is the sole source of market data and execution.
- **Capital allocation policy**: Position sizing is deterministic, with multiple caps to prevent over-sizing and concentration.

---

## Dynamic Trade Selection Workflow

1. **Market Data Ingestion**: Live 1m bars and quotes are fetched from IBKR.
2. **Dynamic Universe Scanning**: Movers, gainers, and news catalysts are merged and filtered in real time.
3. **Scoring & Ranking**: Each candidate is scored (DemandScore, SQS) and ranked deterministically.
4. **Setup Detection**: Momentum setups are detected using VWAP, PMH/HOD, and volume criteria.
5. **Decision Engine**: Strict gates (liquidity, VWAP, regime, slippage, risk) are applied. Only the best candidates pass.
6. **Order Execution**: Orders are submitted to IBKR, with full state tracking and slippage enforcement.
7. **Lifecycle Management**: Trades are managed through all exit stages, with stops, partials, and trailing logic.
8. **Audit & Logging**: Every action is logged for replay, audit, and compliance.

---

## Quick Start


1. **Install dependencies**
  ```bash
  pip install -r requirements.txt
  ```
2. **Configure IBKR credentials**
  - Create a `.env` file in the project root (see `.env.example` for required variables):
    ```
    IBKR_USERNAME=your_username
    IBKR_PASSWORD=your_password
    IBKR_ACCOUNT=your_account_id
    ```
  - Never commit your real `.env` file to version control.
3. **Run a universe scan (no trades, debug view)**
  ```bash
  python main.py --scan-only
  ```
4. **Run paper trading (default)**
  ```bash
  python main.py
  ```
5. **Run a single cycle (testing)**
  ```bash
  python main.py --once --debug
  ```
6. **Live trading with IBKR**
  ```bash
  python main.py --live
  # and set LIVE_TRADING_CONFIRM=I_UNDERSTAND_THIS_IS_REAL_MONEY
  ```
7. **Run the test suite**
  ```bash
  python -m pytest tests/ -x -q
  ```
8. **Replay report (daily signal diagnostics)**
  ```bash
  python tools/replay_report.py --date 2026-03-04
  python tools/replay_report.py --date 2026-03-04 --csv-out logs/replay_report_2026-03-04.csv
  ```
  Outputs grouped stats for submitted, filled, rejected, winner, and loser with mean, p50, p75, and p90 over demand_score, sqs, dollar_flow_z, and pressure_z.

---

## Configuration

All parameters live in `config/settings.py` under `MasterConfig`. Every subsystem reads from this single source — never hardcode values elsewhere.

---

## Scanner

| Parameter                | Default      | Description                       |
|--------------------------|--------------|-----------------------------------|
| min_price / max_price    | $2 – $250    | Price filter                      |
| min_dollar_volume        | $30 M        | Minimum daily dollar volume        |
| min_rvol                 | 2.0×         | Relative volume threshold          |
| top_n                    | 15           | Universe cap                      |
| max_news_tickers         | 5            | Max tickers sourced from news      |

---

## Risk

| Parameter             | Default   | Description                  |
|-----------------------|-----------|------------------------------|
| account_size          | $25,000   | Account equity for sizing    |
| risk_per_trade_pct    | 1 %       | Fixed R per trade            |
| daily_loss_cap_pct    | 3 %       | Session halt threshold       |
| max_trades_per_day    | 5         | Hard cap on daily entries    |

---

## Execution

| Parameter                   | Default   | Description                                 |
|-----------------------------|-----------|---------------------------------------------|
| paper_mode                  | True      | Paper = no real orders sent                 |
| order_type                  | "limit"   | "limit" or "market"                        |
| limit_slippage_pct          | 0.002     | Slippage applied once by OrderManager       |
| limit_order_ttl_seconds     | 30        | Age before TTL cancel-replace fires         |
| stuck_order_seconds         | 120       | Age before order flagged STUCK              |
| cancel_replace_reprice_step | 0.001     | Price nudge per C/R cycle (buys up, sells down) |
| cancel_replace_slippage_cap | 0.005     | Max total deviation from original limit     |

---

## Events / Observability

| Parameter                | Default | Description                                 |
|--------------------------|---------|---------------------------------------------|
| events.enabled           | True    | Master switch for EventBus sink registration |
| events.jsonl_enabled     | True    | Writes authoritative logs/events_YYYY-MM-DD.jsonl |
| events.csv_enabled       | False   | Enables logs/trades_YYYY-MM-DD.csv append-only sink |
| events.csv_orders_enabled| False   | Enables logs/orders_YYYY-MM-DD.csv append-only sink |
| events.console_enabled   | False   | Prints event lines in debug runs             |

---

## OrderManager

| Parameter                | Default | Description                                 |
|--------------------------|---------|---------------------------------------------|
| max_pending_orders       | 3       | Max concurrent live orders                  |
| cancel_replace_on_partial| True    | C/R remainder of partial fills after TTL    |

---

## Discovery vs Decision Architecture

The engine separates market discovery from trade decisions. These are two distinct layers with different jobs.

### Discovery Layer
The scanner continuously evaluates the market to identify symbols with genuine momentum. Discovery is intentionally broad and does not apply strict trading filters — its job is to surface opportunity, not decide trades.

**Inputs:**
- Session-aware RVOL (RTH / PM / AH variants)
- Dollar volume
- Gap %
- Intraday range expansion
- Volume spike Z-score
- Movers ingestion (MoversIngestor)
- News catalyst tickers (discovery only)

**Output:** a ranked candidate universe (Top N by DemandScore).

**News flow guarantee:**
- News → extract tickers → promote to candidate universe
- Candidate remains ineligible until tape confirmation on authoritative market bars (IBKR in live/paper)

### Decision Layer
The decision engine (`decision/engine.py`) evaluates only the highest-ranked discovery candidates and applies strict entry rules:
- Liquidity constraints (spread, dollar volume)
- VWAP structure validation (reclaim, extension reject)
- Regime adjustments (CHOP/RANGE tighten filters and reduce size)
- Slippage monitor gate (per-ticker block or size-reduce)
- Risk manager limits (daily cap, max open trades, cooldowns)

Only candidates passing all decision gates become entry orders. The discovery layer never sees rejection reasons — it always ranks everything it can find.

---

## DemandScore Formula

```
DemandScore =
  0.35 × RVOL
+ 0.25 × |Gap%|
+ 0.20 × IntradayRange%
+ 0.20 × VolumeSpikeZ
```
Ranked descending. Only the top `scanner.top_n` tickers are eligible for setups. Ties broken deterministically by ticker symbol (no randomness). Each component is capped to prevent outlier dominance.

---

## SetupQualityScore (SQS)

```
SQS =
  0.40 × NormalisedRVOL          (capped at 5×)
+ 0.30 × MomentumStrength        (price vs VWAP distance)
+ 0.30 × VolumeConfirmation      (1m vol vs rolling avg)
```
Used as a secondary sort after DemandScore to pick the single best entry when multiple setups qualify simultaneously.

---

## Trade Setup — Momentum v1

All conditions evaluated on the 5-minute chart (RTH bars only):

- Price above VWAP
- Higher highs + higher lows (≥ 2 of last 4 candles)
- Break of Premarket High (PMH) or Intraday High-of-Day
- 5m volume ≥ 1.5× rolling average
- Spread within `execution.max_spread_pct` (default 0.5 %)
- VWAP-extension reject: if price is already > `vwap_extension_reject_pct` above VWAP the setup is skipped (chasing filter)

---

## Trade Lifecycle

After entry fill the lifecycle machine (`execution/lifecycle.py`) manages every exit automatically:

| Stage      | Trigger                | Action                                      |
|------------|------------------------|---------------------------------------------|
| OPEN       | Entry order filled     | Initial stop placed at risk level           |
| PARTIAL1   | Price reaches +1R      | Stop moved to breakeven; sell 25% of position |
| PARTIAL2   | Price reaches +1.5R    | Sell another 50% of remaining shares        |
| TRAILING   | Price reaches +2R      | ATR-based trailing stop activated           |
| CLOSED     | Stop hit or EOD        | Full position exit, trade logged            |

All partial exits are routed back through `OrderManager.submit()` so every exit has a full order audit trail.

---

## Order Execution Flow

```
strategy limit_price
   → OrderManager.submit(broker, side, qty, limit_price)
      → apply slippage once   (limit ± limit_slippage_pct)
      → broker.buy/sell(limit_price=adjusted)   ← unified single param
        IBKR Broker  → LimitOrderRequest(limit_price) — no extra slippage
   → tick() polls fill status each cycle
      → PARTIAL  → optional cancel-replace remainder (cancel_replace_on_partial)
      → TTL hit  → cancel-replace once with repriced limit (_reprice)
      → 2× TTL   → final cancel
      → stuck_order_seconds → STUCK + integrity gate reject
No double-slippage: The IBKR adapter submits the price it receives without modification. _reprice() in OrderManager is the only place prices change after initial submission.
```

---

## Engine Cycle (1-Minute Loop)

Each engine tick (`execution/engine.py._tick()`) performs these steps in order:

 1. Fetch latest 1m bars for the active universe
 2. Update bar pipeline (resample to 5m / 15m / 1h / 4h / 1D)
 3. Scan universe — compute DemandScore, apply hard filters
 4. Select Top-N candidates (ranked by DemandScore → SQS)
 5. Evaluate setups (VWAP reclaim / PMH break / HOD break)
 6. Apply decision engine gates (regime / slippage / risk)
 7. Submit new entry orders via OrderManager
 8. Process lifecycle events for all open trades
 9. Submit partial-exit and full-exit orders via OrderManager
10. Update risk manager (P&L, daily cap, cooldowns)
11. Record fill quality in SlippageMonitor
12. Tick OrderManager state machine (poll fills, TTL, stuck checks)

Steps 8–12 occur even when no new entry is taken, ensuring open positions are always managed.

---

## Slippage Monitor

`execution/slippage.py` tracks execution quality per ticker across the session:

| Method                | Purpose                                                      |
|-----------------------|-------------------------------------------------------------|
| should_block(ticker, now) | Returns True during a fill-quality block window; auto-clears on expiry |
| size_multiplier(ticker)   | Returns 0.0 (blocked), reduced multiplier (degraded), or 1.0 (normal) |
| record_fill(ticker, slippage_pct, now) | Accumulates fill quality; sets block_until on threshold breach |

Called by the engine after every close fill.

---

## Halt / Resume Gate

`execution/halt_machine.py` arms at RTH open each day.

- On halt detection: cooldown timestamp recorded, entries blocked for that ticker
- Resume requires `halt_cooldown_seconds` to elapse
- Engine skips setup evaluation for halted tickers until gate clears

---

## Integrity Gate

`execution/integrity_gate.py` counts consecutive order rejects. If the count exceeds `max_consecutive_rejects`, the engine pauses new entries until a successful fill resets the counter. Prevents runaway rejection loops.

---

## Regime Handling

Regime is detected from SPY 5m bars (ADX + intraday range ratio).

| Regime | Effect on parameters                                 |
|--------|------------------------------------------------------|
| TREND  | Normal filters and sizing                            |
| CHOP   | RVOL threshold ×1.5, spread cap ×0.7, size ×0.5, faster time stop |
| RANGE  | Same adjustments as CHOP                             |

Regime never blocks trading outright — it shapes position size and filter stringency. Regime flows from `risk/regime.py` into `risk/manager.py` and the decision engine gate each cycle.

---

## Movers Integration

`scanner/movers.py` provides a `MoversIngestor` singleton:

- `fetch(now)` — throttled via `poll_interval_seconds`, calls IBKR screener
- `active_tickers(now)` — returns tickers whose TTL has not expired

Wired into `scanner/universe.py` → `scan()` merges movers with seed universe before scoring
`scanner/universe_manager.py` exposes `promote_from_movers(now)` called each cycle to bring in late-breaking high-momentum names.

---

## Session-Aware RVOL

`scanner/rvol.py` computes separate RVOL for each market session:

| Variant   | Active window (ET) |
|-----------|--------------------|
| rvol_rth  | 09:30 – 16:00      |
| rvol_pm   | 04:00 – 09:30      |
| rvol_ah   | 16:00 – 20:00      |

`calc_session_rvol(df_1m, now=None)` — `now=None` uses live ET clock; pass an explicit datetime for deterministic event replay. `best_rvol(result)` returns the active variant, falling back to 1.0 if no session is active.

---

## Startup Recovery

On restart `execution/startup_recovery.py` queries the broker for open orders and re-registers them into OrderManager via `recover_order()`. This prevents duplicate submissions for tickers that already have a live order resting in the book.

---

## Log Files

All logs are auto-created in `logs/`:

| File                  | Content                                                      |
|-----------------------|--------------------------------------------------------------|
| trades_YYYY-MM-DD.csv | Full trade audit: entry/exit prices, PnL, DemandScore, SQS, exit reason |
| scan_YYYY-MM-DD.csv   | Per-cycle scan results with all filter outcomes              |
| engine_YYYY-MM-DD.log | Structured engine log at DEBUG level                        |

---

## Data Freshness Rules

Trades are blocked when market data is considered stale. Rules are enforced before any setup evaluation:

| Condition                                 | Effect                                 |
|--------------------------------------------|----------------------------------------|
| No new 1m bar for > data_stale_seconds     | Ticker skipped — no setup evaluation    |
| Quote timestamp older than quote_stale_seconds | Ticker skipped                     |
| Ticker currently halted                    | Ticker skipped until halt gate clears   |

Higher-timeframe bars (5m, 15m, 1h, 4h, 1D) are never required. Missing higher-TF context only reduces position size — it never blocks a trade.

---

## Zero-Starvation Policy

The engine never refuses to trade due to missing higher-timeframe context.

- Missing higher-TF bars → degrade position size, do NOT block
- Block only on: stale quotes, halted ticker, missing recent 1m bars, integrity gate trip

---

## Test Suite

420+ passing pytest tests cover every subsystem:

| Module                        | Test file                        |
|-------------------------------|----------------------------------|
| Order Manager (TTL, C/R, partial, events) | test_order_manager.py |
| Trade Lifecycle (all 5 stages)            | test_lifecycle.py     |
| Slippage Monitor (block/reduce/clear)     | test_slippage_monitor.py |
| Halt / Resume Gate                        | test_halt_resume_gate.py |
| Integrity Gate                            | test_integrity_gate.py |
| Movers Intake                             | test_movers_intake.py |
| RVOL (session variants, determinism)      | test_rvol.py          |
| Universe Manager                          | test_universe_manager.py |
| Decision Engine                           | test_decision_engine.py |
| Risk Governors                            | test_risk_governors.py |
| Startup Recovery                          | test_startup_recovery.py |
| Event Replay Completeness                  | test_event_replay_completeness.py |
| Reconciliation                            | test_reconciliation.py |
| Settings hardening / governance            | test_settings_hardening.py, test_governance.py |
| Data health / feed authority               | test_data_health.py, test_feed_authority.py |
| RTH block / extended hours                 | test_rth_block.py, test_extended_hours.py |

---

## What Was Built and Changed

### Execution layer

| File                    | Changes                                                                 |
|-------------------------|------------------------------------------------------------------------|
| execution/orders.py     | Unified IBKR broker interface: buy(limit_price=) / sell(limit_price=). IBKR adapter submits price directly — no internal slippage multiply (eliminates double-slippage on C/R cycles). Added BrokerOrderStatus, limit sells via LimitOrderRequest. |
| execution/order_manager.py | submit() passes limit_price= to broker. _emit() uses filled_qty for PARTIAL/FILLED events (not order.qty). Phase 2 TTL block gates PARTIAL cancel-replace on cancel_replace_on_partial config flag. |
| execution/lifecycle.py  | Full 5-stage lifecycle state machine. shares_remaining correctly decremented on each partial exit. All exits emit LifecycleEvent.shares_to_sell back through OrderManager. |
| execution/slippage.py   | SlippageMonitor: should_block / size_multiplier / record_fill / reset_all. Per-ticker block windows auto-expire. |
| execution/halt_machine.py | Halt state machine with cooldown timestamp. Arms at RTH open each day. |
| execution/integrity_gate.py | Consecutive-reject counter with configurable circuit-breaker threshold. |
| execution/reconciliation.py | Broker order reconciliation on demand. |
| execution/startup_recovery.py | Cold-start recover_order() re-registers live broker orders on restart. |
| execution/engine.py     | Full end-to-end 1m loop wired: decision → entry submit → risk.open_trade → lifecycle.evaluate_all → exit submit → risk.close_trade → slippage.record_fill → order_manager.tick. Halt multipliers and regime passthrough added. |

### Scanner layer

| File                    | Changes                                                                 |
|-------------------------|------------------------------------------------------------------------|
| scanner/movers.py       | MoversIngestor with throttled fetch(now), TTL cache, active_tickers(now). |
| scanner/rvol.py         | calc_session_rvol(df_1m, now=None) with RTH/PM/AH variants. now param for deterministic replay. |
| scanner/universe.py     | Merges movers via movers_ingestor.active_tickers(now). Uses rth_bars. |
| scanner/universe_manager.py | Session state, promote_from_movers(), news ticker cap, deterministic tie-breaking. |
| scanner/demand.py       | now param, deterministic tie-breakers, component capping. |

### Signal layer

| File                    | Changes                                                                 |
|-------------------------|------------------------------------------------------------------------|
| signals/vwap.py         | VWAP computed over rth_bars only for session accuracy. |
| signals/structure.py    | rth_bars throughout, HOD excludes last bar, pivot confirmation, RTH vol baseline. |
| signals/setup.py        | rth_bars, VWAP-extension reject filter, spread policy from settings. |

### Data layer

| File                    | Changes                                                                 |
|-------------------------|------------------------------------------------------------------------|
| data/pipeline.py        | Timezone enforcement on all frames, rth_bars output, premarket day-bound correctness. |

### Config

| File                    | Changes                                                                 |
|-------------------------|------------------------------------------------------------------------|
| config/settings.py      | New fields: cancel_replace_on_partial, cancel_replace_reprice_step, cancel_replace_slippage_cap, limit_order_ttl_seconds, stuck_order_seconds, vwap_extension_reject_pct, max_news_tickers, regime multipliers. |
| config/constants.py     | Added order status constants: ORDER_PARTIAL, ORDER_STUCK, ORDER_REPLACED. |
| config/governance.py    | Live-trade runtime guard rules. |

---

## Roadmap

**Phase 1 — MVP (complete)**
- 1m pipeline with MTF resample and RTH enforcement
- Dynamic universe scanner with movers integration
- Session-aware RVOL (RTH / PM / AH)
- VWAP Reclaim + PMH/HOD setup with VWAP-extension reject
- Deterministic selection (DemandScore + SQS, no randomness)
- Fixed-R risk management with regime passthrough
- OrderManager state machine (TTL cancel-replace, partial handling, audit trail)
- Trade lifecycle machine (OPEN → PARTIAL1 → PARTIAL2 → TRAILING → CLOSED)
- Slippage monitor (per-ticker block / size-reduce)
- Halt / resume gate with cooldown
- Integrity gate (consecutive-reject circuit breaker)
- Startup recovery (cold-start order reconciliation)
- IBKR-only execution (unified limit_price interface, no double-slippage)
- 420-test suite

**Phase 2 — In Progress**
- Domain Event Bus (events/ package) — all state changes publish structured events
- Portfolio-level risk controller (risk/portfolio_controller.py) — cross-position rules
- Market State Supervisor (execution/supervisor.py) — NORMAL/CAUTION/DEFENSIVE/HALT_ENTRIES
- JSONL event sink — every domain event logged to logs/events_YYYY-MM-DD.jsonl

**Phase 3 — Next**
- Live 1m WebSocket bar feed (IBKR)
- Adaptive DemandScore weights (ML / reinforcement)
- Backtest harness with full event replay
- Streamlit dashboard (live P&L, scan results, regime indicator)

---

## Test Suite

420+ passing pytest tests cover every subsystem:
- Order Manager, Trade Lifecycle, Slippage Monitor, Halt/Resume, Integrity Gate, Movers Intake, RVOL, Universe Manager, Decision Engine, Risk Governors, Startup Recovery, Event Replay, Reconciliation, Settings Hardening, Data Health, Extended Hours, and more.

---

## Log Files

All logs are auto-created in `logs/`:
- `trades_YYYY-MM-DD.csv`: Full trade audit (entry/exit prices, PnL, scores, exit reason)
- `scan_YYYY-MM-DD.csv`: Per-cycle scan results with all filter outcomes
- `engine_YYYY-MM-DD.log`: Structured engine log at DEBUG level

---

## Data Freshness & Zero-Starvation Policy

- Trades are blocked if market data is stale (no new 1m bar, old quote, or halted ticker).
- Higher-timeframe bars are never required; missing context only reduces position size, never blocks a trade.
- The engine never refuses to trade due to missing higher-TF context—only on stale quotes, halts, or integrity gate trips.

---

## Full Auditability & Replay

- Every state change, order, and trade is logged as a structured event.
- The entire trading session can be replayed deterministically for compliance, debugging, and research.

---

For further details, see the codebase and in-line documentation in each module.

