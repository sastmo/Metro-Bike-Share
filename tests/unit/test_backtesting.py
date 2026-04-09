from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from metro_bike_share_forecasting.evaluation.backtesting import BacktestConfig, describe_rolling_folds, run_backtest
from metro_bike_share_forecasting.evaluation.splitting import build_temporal_split
from metro_bike_share_forecasting.forecasting.baselines.seasonal_naive import SeasonalNaiveForecaster


class _FailingForecaster:
    def fit(self, history: pd.DataFrame):
        raise ValueError("synthetic failure")

    def forecast(self, history: pd.DataFrame, horizon: int) -> pd.DataFrame:
        raise AssertionError("forecast should not be called")


class BacktestingTests(unittest.TestCase):
    def test_rolling_backtest_runs_for_baseline_model(self) -> None:
        frame = pd.DataFrame(
            {
                "bucket_start": pd.date_range("2023-01-01", periods=40, freq="D"),
                "trip_count": [10 + (idx % 7) for idx in range(40)],
                "segment_type": "system_total",
                "segment_id": "all",
                "pandemic_phase": "post_pandemic",
            }
        )
        predictions, metrics = run_backtest(
            frame,
            {"seasonal_naive": lambda: SeasonalNaiveForecaster("daily")},
            BacktestConfig(frequency="daily", horizon=7, initial_window=21, step=7, max_folds=2),
        )
        self.assertFalse(predictions.empty)
        self.assertFalse(metrics.empty)
        self.assertEqual(set(metrics["model_name"]), {"seasonal_naive"})

    def test_recent_rolling_folds_use_latest_windows(self) -> None:
        frame = pd.DataFrame(
            {
                "bucket_start": pd.date_range("2023-01-01", periods=100, freq="D"),
                "trip_count": [10 + (idx % 7) for idx in range(100)],
            }
        )
        folds = describe_rolling_folds(
            frame,
            BacktestConfig(frequency="daily", horizon=7, initial_window=21, step=7, max_folds=2),
        )
        self.assertEqual(len(folds), 2)
        self.assertGreaterEqual(pd.to_datetime(folds.iloc[0]["test_window_start"]), pd.Timestamp("2023-03-25"))
        self.assertEqual(pd.to_datetime(folds.iloc[1]["test_window_end"]).date().isoformat(), "2023-04-08")

    def test_temporal_split_reserves_validation_and_test_windows(self) -> None:
        frame = pd.DataFrame(
            {
                "bucket_start": pd.date_range("2023-01-01", periods=220, freq="D"),
                "trip_count": range(220),
            }
        )
        split = build_temporal_split(
            frame,
            frequency="daily",
            validation_window=28,
            test_window=28,
            minimum_train_window=100,
        )
        self.assertEqual(len(split.validation_frame), 28)
        self.assertEqual(len(split.test_frame), 28)
        self.assertEqual(len(split.train_frame), 164)
        self.assertLess(split.train_frame["bucket_start"].max(), split.validation_frame["bucket_start"].min())
        self.assertLess(split.validation_frame["bucket_start"].max(), split.test_frame["bucket_start"].min())

    def test_run_backtest_skips_failing_models_without_aborting(self) -> None:
        frame = pd.DataFrame(
            {
                "bucket_start": pd.date_range("2023-01-01", periods=40, freq="D"),
                "trip_count": [10 + (idx % 7) for idx in range(40)],
                "segment_type": "system_total",
                "segment_id": "all",
                "pandemic_phase": "post_pandemic",
            }
        )
        predictions, metrics = run_backtest(
            frame,
            {
                "seasonal_naive": lambda: SeasonalNaiveForecaster("daily"),
                "broken_model": lambda: _FailingForecaster(),
            },
            BacktestConfig(frequency="daily", horizon=7, initial_window=21, step=7, max_folds=2),
        )
        self.assertFalse(predictions.empty)
        self.assertFalse(metrics.empty)
        self.assertEqual(set(metrics["model_name"]), {"seasonal_naive"})


if __name__ == "__main__":
    unittest.main()
