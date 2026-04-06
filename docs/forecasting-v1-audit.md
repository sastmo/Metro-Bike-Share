# Forecasting V1 Technical Audit

## Scope

This audit reviews the current Python forecasting system under `src/metro_bike_share_forecasting` before major refactoring.

The goal is to identify:

- what is already worth preserving
- what is analytically incorrect
- what is structurally incomplete relative to the project story
- what must be fixed first in v2

## Current Repo Shape

The repo already has a useful production-style skeleton:

- ingestion and cleaning modules in `src/metro_bike_share_forecasting/ingestion` and `src/metro_bike_share_forecasting/cleaning`
- feature and diagnostics modules in `src/metro_bike_share_forecasting/features` and `src/metro_bike_share_forecasting/diagnostics`
- forecasting, evaluation, selection, and monitoring modules under `src/metro_bike_share_forecasting`
- PostgreSQL schema and repository code in `src/metro_bike_share_forecasting/database` and `sql/forecasting/001_create_forecasting_schema.sql`
- pipeline orchestration in `src/metro_bike_share_forecasting/orchestration/pipeline.py`
- a usable reporting/dashboard layer in `src/metro_bike_share_forecasting/dashboard.py`

This structure is strong enough to keep. The main problems are analytical correctness and missing modeling depth, not top-level organization.

## Observed Data Reality

From the current processed artifacts:

- cleaned trip rows: `1,899,765`
- cleaned local timestamp range: `2019-01-01 00:07:00` to `2024-12-31 23:47:00`
- daily aggregate system-total rows: `2,192`
- station-level daily start-station series: `381` distinct stations in cleaned data, `382` station segments in the current daily aggregate artifact
- station concentration is real:
  - top 5 stations explain about `14.55%` of station-start volume
  - top 10 explain about `22.25%`
  - top 20 explain about `33.20%`
  - top 50 explain about `53.31%`

Implication: station-level forecasting is both feasible and important. The current engine preserves the station dimension, but it does not yet operationalize it well.

## What V1 Already Does Well

### 1. Ingestion and cleaning are real, not toy placeholders

- CSV ingestion reads all files as strings first and keeps source lineage in [`src/metro_bike_share_forecasting/ingestion/csv_loader.py`](../src/metro_bike_share_forecasting/ingestion/csv_loader.py).
- The cleaner reuses the old SQL project’s cleaning intent explicitly in [`src/metro_bike_share_forecasting/cleaning/legacy_rules.py`](../src/metro_bike_share_forecasting/cleaning/legacy_rules.py).
- The fallback cleaner handles:
  - timestamp parsing
  - timezone localization
  - impossible durations and timestamp order
  - invalid geo rows
  - testing rows
  - plan-duration sentinel rows
  - canonical typing and source lineage

### 2. Aggregation preserves total and station views

[`src/metro_bike_share_forecasting/features/aggregation.py`](../src/metro_bike_share_forecasting/features/aggregation.py) creates:

- `system_total`
- `start_station`

at hourly, daily, weekly, monthly, and quarterly frequencies, with completed missing periods.

### 3. Pandemic logic is actually used by models

This is not just plot decoration.

- Regime definitions are created in [`src/metro_bike_share_forecasting/features/regime.py`](../src/metro_bike_share_forecasting/features/regime.py).
- Regime and interaction features are built in [`src/metro_bike_share_forecasting/features/engineering.py`](../src/metro_bike_share_forecasting/features/engineering.py).
- `count_glm` uses regime dummies and interactions.
- `sarimax_fourier` uses pandemic regime flags as exogenous drivers.

### 4. The project already has real persistence and artifacts

The pipeline writes:

- cleaned data
- aggregated series
- feature stores
- backtest outputs
- champion selections
- forecast outputs
- forecast intervals
- monitoring outputs

to local artifacts and optionally to PostgreSQL.

## Critical V1 Problems

These are the issues that must be corrected before the project can honestly claim rigorous adaptive forecasting.

### P0. There is no strict train/validation/test framework

The current project uses rolling-origin backtests, but it does **not** maintain:

- a locked validation window
- a locked final test window
- a separation between tuning/model selection data and final evaluation data

Evidence:

- fold generation is defined in [`src/metro_bike_share_forecasting/evaluation/backtesting.py:31`](../src/metro_bike_share_forecasting/evaluation/backtesting.py#L31)
- pipeline model selection runs directly from rolling backtest output in [`src/metro_bike_share_forecasting/orchestration/pipeline.py:214`](../src/metro_bike_share_forecasting/orchestration/pipeline.py#L214) through [`src/metro_bike_share_forecasting/orchestration/pipeline.py:253`](../src/metro_bike_share_forecasting/orchestration/pipeline.py#L253)
- final forecasts are then refit on all available history in [`src/metro_bike_share_forecasting/orchestration/pipeline.py:269`](../src/metro_bike_share_forecasting/orchestration/pipeline.py#L269) through [`src/metro_bike_share_forecasting/orchestration/pipeline.py:317`](../src/metro_bike_share_forecasting/orchestration/pipeline.py#L317)

Why this matters:

- there is no unbiased final test estimate
- champion selection and ensemble weighting are based on the same windows used to compare models
- interval calibration also has no protected evaluation stage

### P0. Backtest folds are anchored to the oldest windows, not the most relevant recent windows

`generate_rolling_folds()` starts from the earliest possible split and stops after `max_folds` in [`src/metro_bike_share_forecasting/evaluation/backtesting.py:37`](../src/metro_bike_share_forecasting/evaluation/backtesting.py#L37).

For daily data with the current defaults:

- initial window = `365`
- horizon = `28`
- step = `7`
- max folds = `8`

the default daily folds only cover test periods from `2020-01-01` to `2020-03-17`, despite the data running through `2024-12-31`.

That means:

- v1 model selection is based on very old windows
- post-pandemic behavior is not being used properly for default selection
- the system is not actually adaptive in the way the project story claims

### P0. The current backtest metric construction is analytically wrong

Metrics are computed inside a loop over `horizon_step` in [`src/metro_bike_share_forecasting/evaluation/backtesting.py:92`](../src/metro_bike_share_forecasting/evaluation/backtesting.py#L92).

For a single series and a single fold, each `horizon_step` group often contains only one observation.

That causes:

- `MAE` and `RMSE` to collapse to the same number
- horizon-step metrics to become single-point errors rather than meaningful distributions
- summary metrics to be averages of single-observation errors

This is already visible in the current artifact:

- [`outputs/reports/forecasting_20260405T182313Z_624f1cc4_backtest_summary.csv`](../outputs/reports/forecasting_20260405T182313Z_624f1cc4_backtest_summary.csv)

In that file, every model has `mae == rmse`, which is a major red flag.

### P0. Ensemble MASE is implemented incorrectly

In [`src/metro_bike_share_forecasting/forecasting/ensemble/weighted_ensemble.py:100`](../src/metro_bike_share_forecasting/forecasting/ensemble/weighted_ensemble.py#L100), ensemble MASE uses:

- `actual` as the evaluation series
- `actual` again as the training series scale input

That is not valid MASE scaling.

This makes ensemble MASE unusable for fair comparison.

### P0. Station-level forecasting is preserved in the warehouse, but not truly implemented as a first-class forecasting layer

The station dimension survives aggregation, but the training loop only models:

- the configured primary segment by default
- optional top-N stations only if `STATION_LEVEL_TOP_N > 0`

Evidence:

- segment selection is controlled in [`src/metro_bike_share_forecasting/orchestration/pipeline.py:482`](../src/metro_bike_share_forecasting/orchestration/pipeline.py#L482)

Problems:

- station forecasting is off by default
- there is no station tiering
- there is no sparse-station strategy
- there is no macro vs weighted station evaluation
- there is no answer yet to “which stations are hardest to forecast?”

### P0. There is no hierarchical coherence or reconciliation

The current system does not implement:

- bottom-up totals from station forecasts
- top-down allocation
- reconciliation
- consistency diagnostics between direct total forecasts and station-sum forecasts

This is a major gap now that total and station stories both matter.

### P1. Baseline coverage is incomplete

The current model factories only build:

- `seasonal_naive`
- `count_glm`
- `sarimax_fourier`

in [`src/metro_bike_share_forecasting/orchestration/pipeline.py:475`](../src/metro_bike_share_forecasting/orchestration/pipeline.py#L475).

Missing required baselines:

- plain naive
- recent rolling average baseline
- hour-of-week seasonal naive for hourly data

Also, `infer_season_length()` uses `24` for hourly in [`src/metro_bike_share_forecasting/utils/time.py:18`](../src/metro_bike_share_forecasting/utils/time.py#L18), which means hourly seasonal naive is day-seasonal, not hour-of-week seasonal as required for operations.

### P1. Interval evaluation is incomplete and calibration is absent

The models emit `50`, `80`, and `95` intervals, but the evaluation layer only computes:

- `pinball_80`
- `coverage_80`
- `width_80`

Evidence:

- [`src/metro_bike_share_forecasting/evaluation/backtesting.py:107`](../src/metro_bike_share_forecasting/evaluation/backtesting.py#L107)
- [`src/metro_bike_share_forecasting/selection/champion.py:14`](../src/metro_bike_share_forecasting/selection/champion.py#L14)

What is missing:

- coverage evaluation for `50` and `95`
- calibration by horizon
- calibration by regime
- calibration by station tier
- any practical recalibration layer

### P1. Champion selection is too coarse

Current champion selection groups only by:

- `model_name`
- `frequency`

in [`src/metro_bike_share_forecasting/selection/champion.py:8`](../src/metro_bike_share_forecasting/selection/champion.py#L8).

It does not differentiate by:

- horizon bucket
- regime
- total vs station business level
- station tier

It also uses a single raw composite score that mixes scale-dependent metrics directly.

### P1. The feature store and persistence schema are not rich enough for v2 governance

Current tables do not fully store:

- validation windows
- final test windows
- forecast level (`total` vs `station`) as an explicit governed field
- reconciliation outputs
- calibration summaries
- split metadata
- station-tier metadata

Evidence:

- [`src/metro_bike_share_forecasting/database/schema.py`](../src/metro_bike_share_forecasting/database/schema.py)
- [`sql/forecasting/001_create_forecasting_schema.sql`](../sql/forecasting/001_create_forecasting_schema.sql)

### P1. Monitoring is too thin

Current drift monitoring only tracks:

- recent mean vs historical mean
- recent zero share

in [`src/metro_bike_share_forecasting/monitoring/monitoring.py`](../src/metro_bike_share_forecasting/monitoring/monitoring.py).

Missing:

- live forecast error tracking
- interval degradation
- station mix drift
- regime-specific degradation
- champion drift

## Medium-Priority Gaps

### Cleaning and data quality gaps

The cleaner is good, but still incomplete for a portfolio-grade operational system:

- deduplication is only by `trip_id` in [`src/metro_bike_share_forecasting/cleaning/cleaner.py:118`](../src/metro_bike_share_forecasting/cleaning/cleaner.py#L118)
- file-level `duplicates_removed` is not populated in raw ingestion logs in [`src/metro_bike_share_forecasting/validation/quality.py:35`](../src/metro_bike_share_forecasting/validation/quality.py#L35)
- schema drift detection is exact column-order comparison only in [`src/metro_bike_share_forecasting/ingestion/csv_loader.py:52`](../src/metro_bike_share_forecasting/ingestion/csv_loader.py#L52)
- raw station reference files are not yet integrated into modeling

### Diagnostics are useful, but not yet decision-complete

Current diagnostics provide:

- missing periods
- zero share
- basic outlier counts
- ACF/PACF
- STL
- periodogram
- weekday/monthly profiles

Missing diagnostics needed for the project story:

- duplicate profile
- station contribution concentration over time
- share-of-total drift by station
- regime-wise error analysis
- station sparsity profile
- demand concentration risk for fleet logic

### Dashboard is better than before, but it still mirrors the engine’s gaps

The current dashboard does a reasonable job separating:

- held-out evaluation
- final future forecasts
- diagnostics
- station history exploration

But it still cannot answer several key project questions because the engine does not yet produce the right artifacts:

- total vs station-sum consistency
- calibration by interval level
- regime-wise scorecards
- top-station error contribution
- sparse-station behavior
- coherent forecasting diagnostics

## Leakage and Method Audit

### What looks safe in v1

- no random train/test split was found
- lag features use `shift()` and rolling features are based on shifted series in [`src/metro_bike_share_forecasting/features/engineering.py:84`](../src/metro_bike_share_forecasting/features/engineering.py#L84)
- GLM and SARIMAX rebuild features from the fold-specific training frame inside the model, which avoids a large class of leakage from precomputed full-history features

### What still needs tightening

- no split object exists to enforce strict train/validation/test contracts across the project
- regime breakpoint refinement currently uses the full daily system series before backtesting in [`src/metro_bike_share_forecasting/features/regime.py:45`](../src/metro_bike_share_forecasting/features/regime.py#L45)
- diagnostics are run on full series before evaluation and are not isolated from model-governance decisions

## What Must Be Preserved in V2

Do not throw these parts away:

- repo structure
- legacy SQL reuse story
- CSV lineage and cleaning foundation
- total + station aggregation backbone
- pandemic-aware feature concept
- PostgreSQL-first system-of-record approach
- champion-challenger framing
- local artifact generation and lightweight dashboard entrypoint

These are the right foundations. The refactor should deepen them, not restart from scratch.

## V2 Fix Order

### Phase 1. Correct analytical validity

1. Implement strict split objects:
   - train
   - validation
   - locked final test
2. Redesign rolling-origin backtesting so folds are:
   - recent or explicitly distributed through history
   - separate from final validation/test
3. Fix metric computation:
   - aggregate over meaningful holdout groups
   - correct MASE
   - add `50/80/95` coverage and width
   - add calibrated interval evaluation

### Phase 2. Make station forecasting first-class

1. Add station tiering:
   - high-volume
   - medium-volume
   - sparse
2. Add station-level evaluation:
   - macro average
   - weighted average
   - top-station scorecards
   - sparse-station scorecards
3. Store station-level forecasts and backtests as governed artifacts, not optional side paths

### Phase 3. Add coherent forecasting

1. Train direct total models
2. Train station models
3. Create bottom-up totals from station forecasts
4. Add reconciliation / consistency diagnostics
5. Compare:
   - direct total
   - bottom-up total
   - reconciled total

### Phase 4. Improve governance and reporting

1. Expand schema for:
   - split metadata
   - calibration summaries
   - reconciliation outputs
   - station tier metadata
2. Improve champion selection by:
   - frequency
   - horizon bucket
   - level
   - station tier
3. Refresh the dashboard around analytical questions, not generic views

## Bottom Line

V1 is a strong scaffold with real data engineering, cleaning reuse, persistence, regime-aware features, and a usable dashboard.

But analytically it is not yet portfolio-grade because:

- it lacks a locked validation/test design
- its default backtests are biased toward early history
- its metric layer is flawed
- station forecasting is not yet first-class
- coherence is missing
- interval evaluation is incomplete

The right move is **not** to rebuild the repo from zero.
The right move is to keep the structure and aggressively refactor the forecasting engine around split integrity, metric correctness, station modeling, and coherent forecasting.
