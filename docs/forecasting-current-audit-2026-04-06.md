# Forecasting Audit - 2026-04-06

## Scope

This audit reviews the current forecasting implementation under `src/metro_bike_share_forecasting` after the earlier refactor work.

The goal is to separate:

- what is already structurally strong
- what is analytically improved from the original version
- what is still weak or misleading
- what must be corrected next before this can honestly be presented as a portfolio-grade adaptive forecasting system

## Current Repo State

The codebase now has the right high-level shape:

- CSV ingestion in `src/metro_bike_share_forecasting/ingestion`
- reusable cleaning in `src/metro_bike_share_forecasting/cleaning`
- aggregation, feature engineering, and regime logic in `src/metro_bike_share_forecasting/features`
- diagnostics in `src/metro_bike_share_forecasting/diagnostics`
- forecasting models in `src/metro_bike_share_forecasting/forecasting`
- splitting, backtesting, scoring, and metrics in `src/metro_bike_share_forecasting/evaluation`
- model selection in `src/metro_bike_share_forecasting/selection`
- PostgreSQL schema and repository plumbing in `src/metro_bike_share_forecasting/database` and `sql/forecasting/001_create_forecasting_schema.sql`
- orchestration in `src/metro_bike_share_forecasting/orchestration/pipeline.py`
- reporting and dashboard layers in `src/metro_bike_share_forecasting/reporting.py` and `src/metro_bike_share_forecasting/dashboard.py`

This structure should be preserved. The main remaining problems are not repo layout. They are evaluation depth, calibration, station execution, coherence, and artifact truthfulness.

## Data Reality In The Repo

From the current processed data in `data/processed/cleaned_trip_data.csv.gz`:

- cleaned rows: `1,899,765`
- local timestamp range: `2019-01-01 00:07:00` to `2024-12-31 23:47:00`
- distinct start stations: `381`

Implication:

- system-level forecasting is well supported
- station-level forecasting is not optional; it is feasible and central to the project story

## What Is Already Good

### 1. Base-project reuse is real

The cleaner in `src/metro_bike_share_forecasting/cleaning/cleaner.py` explicitly reuses legacy logic documented in `src/metro_bike_share_forecasting/cleaning/legacy_rules.py`.

That is good portfolio signal because the project is not pretending the earlier SQL work does not exist.

### 2. Strict split logic now exists

`src/metro_bike_share_forecasting/evaluation/splitting.py` does enforce:

- train
- validation
- test

with time-based slicing only.

This is a real improvement over the earlier version.

### 3. Rolling backtests now use recent folds instead of the earliest folds

`generate_rolling_folds()` in `src/metro_bike_share_forecasting/evaluation/backtesting.py` now keeps the last `max_folds` candidate windows.

For the current daily defaults, the backtest windows now concentrate on mid-2024 instead of early 2020.

That is directionally correct for adaptiveness.

### 4. Metrics are no longer obviously collapsed at the code level

`rmse()` and `mae()` are implemented separately in `src/metro_bike_share_forecasting/evaluation/metrics.py`, and `score_prediction_frame()` now scores the full holdout subset instead of single-observation horizon slices.

This addresses one of the biggest earlier analytical failures.

### 5. The pipeline now has the right conceptual pieces

The pipeline in `src/metro_bike_share_forecasting/orchestration/pipeline.py` now attempts to produce:

- strict split metadata
- rolling backtests
- validation summaries
- test summaries
- champion selection
- station profiles
- reconciliation outputs

This is the right direction.

## What Is Still Wrong Or Incomplete

## P0 - The saved repo artifacts are stale and do not demonstrate the current engine

The latest saved summary in the repo is:

- `outputs/reports/forecasting_20260405T182313Z_624f1cc4_summary.json`

That run still shows:

- frequencies: `['daily']`
- `station_level_top_n = 0`
- champion count: `1`
- no validation summary artifact
- no test summary artifact
- no station profile artifact
- no reconciliation artifact

Implication:

- the code may have improved, but the repo-visible outputs do not prove it
- the dashboard is still largely showing an outdated daily-only run
- station forecasting is not actually demonstrated in committed artifacts

This must be fixed by rerunning the pipeline with real station modeling enabled and saving the new artifacts into the repo.

## P0 - Production forecast metadata is misleading

In `src/metro_bike_share_forecasting/orchestration/pipeline.py`, the future production-style forecasts are fit on:

- train + validation + test

But `_prepare_forecast_frame()` writes:

- `training_window_start`
- `training_window_end`

from `temporal_split.train_frame` only.

So the stored forecast metadata claims the model was trained only through the end of the train window even though the actual future forecast model was fit through the end of the full history.

Why this matters:

- model registry metadata is inaccurate
- dashboard interpretation is misleading
- the project story about locked test vs final retrain becomes harder to explain clearly

This should be corrected by storing both:

- development windows used for selection
- full refit window used for final production forecasts

## P0 - Backtesting is still too narrow in regime coverage

The current backtest logic uses the latest folds inside the training span. For daily data, the current default folds are:

- `2024-06-26` to `2024-07-23`
- `2024-07-03` to `2024-07-30`
- `2024-07-10` to `2024-08-06`
- `2024-07-17` to `2024-08-13`
- `2024-07-24` to `2024-08-20`
- `2024-07-31` to `2024-08-27`
- `2024-08-07` to `2024-09-03`
- `2024-08-14` to `2024-09-10`

This is better than the old earliest-fold behavior, but it still means:

- the rolling backtest is almost entirely post-pandemic
- the model selection logic is not stress-tested across pandemic shock and recovery
- regime-aware fold coverage is not yet implemented

The current system is adaptive to recent conditions, but it is not yet regime-comparative in the way the project story promises.

Needed correction:

- keep recent folds for production relevance
- add regime-stratified evaluation summaries so pre-pandemic, shock, recovery, and post-pandemic performance are all visible
- consider a second evaluation panel or fold-bank that explicitly samples different structural periods

## P0 - Station-level forecasting is still only partially implemented

Station support exists in the code, but the implementation is still incomplete relative to the stated goal.

What exists:

- station aggregates are preserved
- station profiles are computed
- top-N direct modeling eligibility exists
- remaining stations receive share-allocation in reconciliation

What is still missing:

- station-level forecasting is not demonstrated in the saved artifacts
- there is no station-level locked-test summary committed in the repo
- there is no macro vs weighted station evaluation layer
- there is no station-tier-specific evaluation summary
- there is no clear report of which stations are hardest to forecast
- there is no station-level risk ranking for the next horizon

The code preserves the station dimension, but the project still does not yet prove that station forecasting works end to end.

## P0 - Coherence exists only as future allocation, not as evaluated forecasting evidence

The current reconciliation logic in `_build_reconciled_station_outputs()` does this:

- take the direct total forecast
- keep direct top-station forecasts
- scale direct station forecasts down if they exceed the total
- allocate residual demand to unmodeled stations by recent share

This is a useful first step, but it is not yet enough.

What is missing:

- no holdout evaluation comparing direct total vs summed stations vs reconciled result
- no empirical evidence that the reconciliation step improves anything
- no stored error diagnostics for coherence on historical windows
- no station-level backtest for the reconciled outputs

Right now coherence is a forward allocation strategy, not yet a validated forecasting layer.

## P0 - Intervals are still uncalibrated

The code now computes `50`, `80`, and `95` intervals and scores all of them in `src/metro_bike_share_forecasting/evaluation/scoring.py`.

That is good.

But the system still does not include:

- interval recalibration
- coverage diagnostics by horizon bucket
- coverage diagnostics by regime
- coverage diagnostics by station tier
- selection logic that explicitly penalizes consistent undercoverage enough to quarantine bad uncertainty models

So the system is producing interval metrics, but not yet solving poor calibration.

For this project story, badly undercovered intervals are a first-class analytical failure, not a minor detail.

## P1 - The ensemble logic is still too simple

The ensemble currently:

- computes inverse-error weights from validation summary
- applies them uniformly across the full horizon
- applies them uniformly across all regimes
- applies them uniformly across all station tiers

This is a reasonable first baseline, but it is not yet the dynamic ensemble story the project wants.

Needed correction:

- weights by frequency and horizon bucket at minimum
- optionally different weights for total vs station
- optionally different weights for high-volume vs sparse stations

## P1 - Champion selection is still too coarse

`select_champion_model()` still chooses one winner per frequency for the current segment loop.

That means the project still does not surface:

- best short-horizon model vs best long-horizon model
- best post-pandemic model vs best recovery model
- best total-level model vs best station-level strategy

The system has the data needed for this direction, but not the final selection logic yet.

## P1 - Count GLM is still fragile and should be treated as provisional until validated on current artifacts

The GLM design uses:

- many dummy variables
- lag features
- rolling features
- regime interactions
- recursive forecasting

That is promising, but also potentially brittle. At the moment:

- there is no fresh repo-visible validation/test evidence proving the GLM is stable
- the stale committed artifacts are not enough to trust it

The right approach is:

- rerun with current engine
- inspect locked validation and test outputs
- quarantine the GLM from champion tables if it shows pathological bias or interval failure

## P1 - Diagnostics are stronger than before, but still not fully tied into model governance

The diagnostics module now produces more meaningful insights, but the downstream governance layer still does not act strongly enough on them.

Examples of missing links:

- non-stationarity is not converted into any explicit model quarantine or recency strategy decision
- poor interval coverage does not trigger calibration or down-ranking logic beyond the basic composite penalty
- station sparsity does not yet feed a dedicated pooled or fallback evaluation framework

The diagnostics are becoming useful, but they are not yet fully decision-driving.

## P1 - Dashboard structure is ahead of the artifacts

The dashboard now has better tabs and some stronger explanatory text, but there are still two practical issues:

1. The committed outputs feeding it are stale
2. There is still no dedicated hierarchy/coherence page or dedicated map page

Right now:

- the station map lives inside the segment explorer
- coherence is surfaced in overview-style summaries rather than as its own analytical page

That is acceptable for now, but not the final target.

## Leakage And Split Audit

## What looks safe

- no random train/test splitting was found
- rolling features are computed on shifted history in `build_feature_store()`
- lag features are grouped by `segment_type` and `segment_id`
- forecasting models rebuild features from the provided history subset instead of using precomputed full-data features directly

So the biggest classic leakage failures do not appear to be present in the current code.

## What still needs attention

- feature generation is duplicated across models, which increases the risk of silent divergence
- production forecast metadata currently blurs the distinction between development windows and final refit windows
- reconciliation is only applied to future outputs, not evaluated historically

## Most Important Corrections To Implement Next

1. Produce a fresh full run with current engine settings and commit real artifacts that demonstrate:
   - validation results
   - locked final test results
   - station profiles
   - station forecasts
   - reconciliation outputs

2. Fix forecast/model metadata so future production forecasts clearly distinguish:
   - selection windows
   - final full-refit training window

3. Add regime-aware evaluation summaries:
   - by pandemic phase
   - by horizon bucket
   - by station tier

4. Promote station forecasting from optional support to demonstrated output:
   - enable top-N direct station modeling
   - store and surface station-level validation/test summaries
   - report weighted and macro station metrics

5. Evaluate coherence on holdout data, not only future forecasts:
   - direct total
   - sum of direct station forecasts
   - reconciled station-sum

6. Add interval calibration diagnostics and, if feasible, a practical recalibration layer.

7. Make champion and ensemble logic more specific:
   - by horizon bucket
   - by total vs station
   - later by regime or station tier

8. Only after the above, finish the dashboard with:
   - a dedicated coherence page
   - a dedicated map page
   - stronger station-risk storytelling

## Bottom Line

The current codebase is no longer a toy. The structure is good, the split logic is real, and the station hierarchy is beginning to exist.

But it is still not yet portfolio-complete because:

- the repo-visible artifacts are stale
- station forecasting is not yet proven end to end
- coherence is not historically evaluated
- intervals are not calibrated
- selection and ensemble logic are still too coarse
- production metadata is partially misleading

The next phase should focus on analytical proof, not UI polish.
