# Forecasting V2 Implementation Report

## What Was Wrong In V1

V1 had a strong structure but important analytical problems:

- no strict train / validation / final test separation
- rolling folds defaulted to very old windows instead of recent windows
- backtest metric construction could collapse `MAE` and `RMSE` into the same value
- ensemble `MASE` scaling was wrong
- interval evaluation only covered `80%`
- station forecasting was optional and shallow
- there was no coherent total-versus-station forecasting layer

See the full audit in [forecasting-v1-audit.md](/Users/morteza/Metro-Bike-Share/docs/forecasting-v1-audit.md).

## What V2 Implements

The current refactor adds:

- strict temporal splits via [`splitting.py`](/Users/morteza/Metro-Bike-Share/src/metro_bike_share_forecasting/evaluation/splitting.py)
- recent rolling-origin backtests via [`backtesting.py`](/Users/morteza/Metro-Bike-Share/src/metro_bike_share_forecasting/evaluation/backtesting.py)
- corrected metric scoring with `50/80/95` interval evaluation via [`scoring.py`](/Users/morteza/Metro-Bike-Share/src/metro_bike_share_forecasting/evaluation/scoring.py)
- safer `MASE` scaling in [`metrics.py`](/Users/morteza/Metro-Bike-Share/src/metro_bike_share_forecasting/evaluation/metrics.py)
- a stronger baseline stack:
  - `naive`
  - `seasonal_naive`
  - `rolling_mean`
- validation-based champion selection and final test evaluation
- station profiling and direct-model eligibility tracking
- coherent station outputs through direct-station modeling plus share-allocation / reconciliation
- expanded PostgreSQL schema for split metadata, evaluation predictions, station modeling registry, and reconciliation outputs

## Current V2 Evidence

Verified locally:

- `PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_*.py'`
- `PYTHONPATH=src python3 -m metro_bike_share_forecasting.cli inspect-base-logic`
- synthetic end-to-end smoke run with:
  - strict splits
  - validation / test artifacts
  - station-level direct modeling
  - reconciliation outputs

Smoke-run artifacts were written under `/tmp/metro_bike_share_smoke/...` for fast verification without waiting on the full historical corpus.

## What Still Needs The Next Pass

V2 is a meaningful engine upgrade, but it is not the end state yet.

Still recommended next:

- run and review the full historical corpus with the new engine
- add station-tier evaluation summaries:
  - macro average
  - weighted average
  - sparse-station scorecards
- add richer calibration summaries by regime and horizon
- compare direct total vs bottom-up total vs reconciled total explicitly
- deepen the dashboard around:
  - final test outcomes
  - calibration
  - regime-specific degradation
  - top-station error contribution
