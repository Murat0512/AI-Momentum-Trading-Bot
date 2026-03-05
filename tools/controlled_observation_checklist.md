# Controlled Observation Runbook (Phase 1)

Purpose: validate system behavior end-to-end before any strategy tuning.

## Scope for this phase

Focus only on:
1. Scanner quality
2. Trade attempt quality
3. Execution safety
4. Exit behavior

Ignore PnL for now.

---

## Session setup (before market open)

- Run in paper mode only.
- Enable debug logs.
- Ensure event sinks are on:
  - `events.enabled = True`
  - `events.jsonl_enabled = True`
  - `events.csv_enabled = True`
  - `events.csv_orders_enabled = True`
- Start command:
  - `python main.py --debug`

---

## Observation checklist

## 1) Scanner finds real momentum names

Validate:
- Top-N ranked tickers look like real movers (gaps, breakouts, news runners).
- Ranked list is not random/illiquid noise.
- Liquidity and spread filters appear effective.

Record:
- Top 5–10 ranked names at open.
- Whether each name is a real mover (`yes/no`).

## 2) Trade attempts are reasonable

Validate each attempted entry:
- Breakout structure is real (PMH/HOD/VWAP reclaim continuation).
- Spread is acceptable.
- Volume context supports the move.

Red flag:
- Entries on random candles without structure/volume confirmation.

## 3) Execution system is safe and professional

Validate:
- Order sizing matches risk rules.
- Partial fills handled correctly.
- Cancel/replace behavior is sane.
- No duplicate orders for same trade intent.
- Lifecycle transitions are coherent.

Red flag:
- Duplicate submissions, stuck state loops, or missing transitions.

## 4) Exit logic behaves by design

Validate:
- Partial profit triggers fire correctly.
- Trailing stop activates as designed.
- Breakeven protection is applied when expected.
- Close events are complete and consistent.

Red flag:
- Position remains open after close condition, or close event missing.

---

## Health checks (must pass)

- [ ] No crashes
- [ ] Event logs written
- [ ] Orders submitted correctly
- [ ] Risk caps enforced
- [ ] No runaway trade loops

If all pass, system is technically healthy.

---

## Post-session review flow

1. Fill `tools/first_run_notes_template.csv`.
2. Generate replay summary:
   - `python tools/replay_report.py --date YYYY-MM-DD --csv-out logs/replay_report_YYYY-MM-DD.csv`
3. Compare:
   - submitted vs filled vs rejected signal distributions
   - winner vs loser signal distributions
4. Do not tune rules yet; collect multiple sessions first.
