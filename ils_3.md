# Institutional Liquidity Strategy (ILS) 3.0
## Single Source of Technical Truth

---

## 0. Document Purpose & Governance

This document is the **authoritative technical specification** for the ILS 3.0 trading system.

It serves as:
- The **single source of truth** for strategy logic
- The reference for **all development, testing, and deployment**
- The translation layer between **trading theory and engineering execution**

Rules:
- No feature or logic exists unless documented here
- Changes require explicit versioning (v1 freeze, v2 enhancements, etc.)
- Human overrides must follow documented protocols only

---

## 1. Strategic Objective

Primary Objective:
- Build a **robust, anti-fragile, semi-automated trading system** based on institutional liquidity mechanics

Secondary Objectives:
- Capital preservation first
- Scalable risk-adjusted returns
- HITL amplification without emotional discretion

Target Performance Envelope (not guarantees):
- Fully automated: 30–60% annually
- HITL disciplined: 70–120% annually
- Max drawdown target: <20%

---

## 2. Asset Universe (v1)

### Included Instruments

Forex Majors:
- EURUSD
- GBPUSD
- USDJPY
- USDCHF
- AUDUSD
- USDCAD

Forex Crosses (limited):
- GBPJPY
- EURJPY

Commodities:
- XAUUSD (Gold)
- XAGUSD (Silver)
- CL (Crude Oil – optional, v1.1)

Indices (optional v1.2):
- NAS100
- SPX500

---

## 3. Timeframe Architecture

### Analysis Stack

Higher Timeframes (Bias & Regime):
- Weekly
- Daily
- H4

Execution Timeframes:
- M15 (primary)
- M5 (secondary)
- M1 (scalping only post-v2)

Rules:
- HTF defines bias
- LTF defines entry only
- No counter-bias execution without HTF liquidity sweep

---

## 4. Market Regime Filters

### Volatility Metrics

- ATR(14)
- ATR(100)

### Trend / Chop Metrics

- ADX(14)
- CHOP(14)

Regime Logic:

- CHOP > 61.8 AND ADX < 20 → NO TRADING
- CHOP < 38.2 → Trending regime
- Transition zone → Standard scoring

---

## 5. Adaptive Fractal Structure Engine

### Purpose

Define market structure dynamically based on asset volatility.

### Adaptive Lookback Formula

L_adaptive = round(L_base × (1 + α × (ATR_long / ATR_short − 1)))

Parameters by Asset Class:

| Asset | L_base | α |
|-----|------|----|
| Forex Majors | 5 | 0.5 |
| Forex Crosses | 7 | 1.0 |
| Gold | 9 | 1.5 |
| Indices | 7 | 1.2 |

### Structure Types

- External Structure: HTF bias
- Internal Structure: LTF entry triggers

Rule:
- Internal MSS valid only with external alignment OR HTF liquidity sweep

---

## 6. Liquidity Mechanics

### Swing Identification

Swing High:
- High[i] > max(High[i−L:i−1]) AND High[i] > max(High[i+1:i+L])

Swing Low:
- Low[i] < min(Low[i−L:i−1]) AND Low[i] < min(Low[i+1:i+L])

---

### Liquidity Sweep Detection

Bearish Sweep:
- High_current > SwingHigh
- Close_current ≤ SwingHigh + 0.2 × ATR14

Bullish Sweep:
- Low_current < SwingLow
- Close_current ≥ SwingLow − 0.2 × ATR14

Sweep Types:
- Wick Sweep
- Close & Immediate Reversal

---

## 7. Displacement Validation

Displacement Candle Conditions:

- Body / Range ≥ 0.6
- Range ≥ 1.5 × ATR14
- Directional close:
  - Bearish: Close in bottom 30%
  - Bullish: Close in top 30%

Optional (futures only):
- Volume ≥ 125% of 20-period MA

---

## 8. Market Structure Shift (MSS)

Valid MSS requires:
1. Prior Liquidity Sweep
2. Valid Displacement
3. Break of Internal Structure
4. Regime filter pass

Without sweep → continuation structure (lower score)

---

## 9. Fair Value Gap (FVG) Logic

### Detection

Bullish FVG:
- Low[i] > High[i−2]
- Candle i−1 bullish

Bearish FVG:
- High[i] < Low[i−2]
- Candle i−1 bearish

### Size Filter

- Gap ≥ 0.5 × ATR14

### Validity

- Invalidated if candle close crosses distal boundary

---

## 10. Order Block (OB) Logic (Secondary Only)

Definition:
- Last opposing candle before displacement breaking structure

Validation:
- Must produce valid FVG
- Must break adaptive swing
- Optional volume confirmation

OB alone is NEVER an entry trigger

---

## 11. Confluence Scoring Engine

### Score Components (Max 100)

HTF Alignment:
- Bias aligned: +25
- HTF POI: +15

Displacement:
- Strong displacement: +10
- Clean FVG: +10

Liquidity:
- HTF sweep: +15
- Inducement: +10

Context:
- Killzone timing: +10
- CHOP < 50: +5

---

### Tier Thresholds

- Tier 1: 85–100
- Tier 2: 65–84
- Below 65: Ignore

---

## 12. Risk & Position Sizing

### Fixed Fractional Model

Base Risk Unit (R):
- Default: 1.0%

Tier Scaling:
- Tier 1: 1.0R
- Tier 1 + Human Escalation: 1.5–2.0R
- Tier 2: 0.5R

### Drawdown Circuit Breaker

- At −5R drawdown → R reduced by 50%
- Restored only at equity high watermark

---

## 13. Trade Management

Stop Loss:
- Beyond swept swing ± 1.0–1.5 × ATR14

Take Profit:
- TP1: Internal liquidity
- TP2: ≥3R or HTF structure

Breakeven:
- At +1R or after TP1

---

## 14. Temporal Filters (UTC)

Asia: 00:00–06:00 (Observe only)
London Killzone: 07:00–10:00
NY Killzone: 12:00–15:00
London Close: 15:00–17:00

---

## 15. Human-in-the-Loop Protocol

HITL Responsibilities:
- News veto
- HTF visual validation
- Risk escalation approval

No discretionary entry creation allowed

---

## 16. Market Data Processing & Construction Rules (v1)

This section defines how raw market data is ingested, normalized, aggregated, and validated before entering the signal engine. All downstream logic assumes these rules.

---

### 16.1 Supported Raw Data Sources

Primary (Local Backtesting):
- Dukascopy tick data (CSV / binary)

Secondary (Live / Paper):
- Interactive Brokers (IBKR) TWS / Gateway

---

### 16.2 Tick Data Canonical Schema

All tick data must be normalized to the following schema:

- timestamp_utc (datetime, nanosecond precision preferred)
- bid_price (float)
- ask_price (float)
- bid_size (optional)
- ask_size (optional)
- source (enum: DUKASCOPY | IB)

Rules:
- All timestamps converted to UTC
- No local timezone logic allowed
- No mid-price stored permanently (computed only)

---

### 16.3 Tick Cleansing & Validation Rules

Before aggregation:

- Drop ticks where bid <= 0 OR ask <= 0
- Drop ticks where ask < bid
- Remove duplicate timestamps (keep last)
- Forward-fill gaps <= 1 second only
- Gaps > 1 second flagged as data hole

Data quality flags must be persisted per session.

---

### 16.4 Bar Construction Rules (Critical)

Bars are constructed from **bid/ask ticks**, not mid-price candles.

Reference Price:
- Open: mid = (bid + ask) / 2
- High: max(mid)
- Low: min(mid)
- Close: last(mid)

Volume Proxy:
- Tick count per bar (v1 default)
- Optional: sum(bid_size + ask_size)

Supported Bar Types:
- Time bars only (no range/volume bars in v1)

Bar Durations:
- 1s (internal)
- 1m
- 5m
- 15m
- 1h
- 4h
- 1d

---

### 16.5 Session & Day Boundary Handling

- Forex trading day defined as 17:00 New York close (22:00 UTC standard)
- All daily bars aligned to this boundary
- Session labels assigned per bar:
  - Asia
  - London
  - New York

DST handling:
- ALL logic uses UTC only
- Session windows are fixed UTC ranges

---

### 16.6 Spread & Slippage Modeling (Backtest)

Spread Model:
- Historical bid/ask used directly when available
- If only mid available, synthetic spread applied:
  - Forex majors: 0.8–1.2 pips dynamic
  - Gold: 15–30 cents dynamic

Slippage Model:
- Market orders:
  - Slippage = max(0.1 × ATR_1m, min_tick)
- Limit orders:
  - Filled only if price trades through limit

Stops:
- Executed at worst available bid/ask

---

### 16.7 Multi-Timeframe Alignment

Rules:
- All HTF bars derived from SAME tick stream
- No mixing broker feeds across TFs
- HTF indicators computed AFTER aggregation

Alignment Rule:
- LTF signals cannot reference incomplete HTF bars

---

### 16.8 Data Output Contract

Each finalized bar must expose:

- OHLC
- Spread_avg
- Tick_count
- Session_label
- ATR14, ATR100
- ADX14, CHOP14

No strategy logic allowed in data layer.

---

## 17. Backtesting Architecture

---

### 17.1 Design Principles

- Deterministic
- Replayable
- No lookahead bias
- Identical logic for backtest and live

---

### 17.2 Event-Driven Engine

Core Components:

1. Data Replayer
   - Feeds ticks sequentially
   - Controls simulated clock

2. Bar Aggregator
   - Emits bars when completed

3. Strategy Engine
   - Reads completed bars only
   - Generates signals

4. Risk Engine
   - Validates risk & sizing

5. Execution Simulator
   - Applies spread & slippage

6. Portfolio Tracker
   - Equity
   - Drawdown
   - Exposure

---

### 17.3 Execution Model

Order Types:
- Market
- Limit (FVG entries)
- Stop

Fill Logic:
- Market: immediate at bid/ask
- Limit: requires price trade-through
- Partial fills NOT simulated (v1)

---

### 17.4 Human-in-the-Loop Simulation

HITL modes:

1. Auto-Approve (baseline expectancy)
2. Rule-Based Filter (news, HTF POI)
3. Manual Replay (research only)

Telegram simulation:
- Signals logged with decision timestamps

---

### 17.5 Metrics & Reporting

Per Trade:
- R-multiple
- Slippage
- Spread cost

Per Period:
- Win rate
- Expectancy
- Profit factor
- Max DD

Monte Carlo:
- Trade reshuffling
- Worst-case DD estimation

---

### 17.6 Validation Workflow

1. Historical Backtest (3–5 years)
2. Walk-forward (year by year)
3. Parameter perturbation
4. Paper trading (IB)
5. Capital deployment

---

## 17. Backtesting & Validation

Metrics:
- Profit Factor > 1.5
- Max DD < 20%
- Sharpe > 1.0

Testing:
- Walk-forward
- Parameter perturbation

---

## 18. Versioning

- v1.0: Frozen logic (this document)
- v1.x: Execution + infra only
- v2.0: ML / optimization (future)

---

## End of Technical Specification

