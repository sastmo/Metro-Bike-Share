from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from metro_bike_share_forecasting.config.settings import get_settings
from metro_bike_share_forecasting.evaluation.splitting import build_temporal_split
from metro_bike_share_forecasting.orchestration.pipeline import ForecastingPipeline
from metro_bike_share_forecasting.utils.logging import setup_logging


class PipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.pipeline = ForecastingPipeline(get_settings(), setup_logging("ERROR"))

    def test_prepare_forecast_frame_uses_full_refit_window_and_selection_windows(self) -> None:
        history = pd.DataFrame(
            {
                "bucket_start": pd.date_range("2024-01-01", periods=30, freq="D"),
                "trip_count": [100 + idx for idx in range(30)],
            }
        )
        split = build_temporal_split(
            history,
            frequency="daily",
            validation_window=7,
            test_window=7,
            minimum_train_window=14,
        )
        forecast_frame = pd.DataFrame(
            {
                "target_timestamp": pd.date_range("2024-01-31", periods=3, freq="D"),
                "prediction": [120.0, 121.0, 122.0],
                "lower_50": [110.0, 111.0, 112.0],
                "upper_50": [130.0, 131.0, 132.0],
                "lower_80": [105.0, 106.0, 107.0],
                "upper_80": [135.0, 136.0, 137.0],
                "lower_95": [100.0, 101.0, 102.0],
                "upper_95": [140.0, 141.0, 142.0],
            }
        )

        prepared = self.pipeline._prepare_forecast_frame(
            forecast_frame=forecast_frame,
            pipeline_run_id="run_1",
            frequency="daily",
            segment_type="system_total",
            segment_id="all",
            model_name="seasonal_naive",
            generated_at=pd.Timestamp("2026-04-06T00:00:00Z"),
            temporal_split=split,
        )
        outputs = prepared["outputs"]

        self.assertEqual(pd.to_datetime(outputs.iloc[0]["training_window_start"]), split.train_frame["bucket_start"].min())
        self.assertEqual(pd.to_datetime(outputs.iloc[0]["training_window_end"]), split.test_frame["bucket_start"].max())
        self.assertEqual(
            pd.to_datetime(outputs.iloc[0]["selection_train_window_start"]),
            split.train_frame["bucket_start"].min(),
        )
        self.assertEqual(
            pd.to_datetime(outputs.iloc[0]["selection_train_window_end"]),
            split.train_frame["bucket_start"].max(),
        )
        self.assertEqual(pd.to_datetime(outputs.iloc[0]["validation_window_start"]), split.validation_frame["bucket_start"].min())
        self.assertEqual(pd.to_datetime(outputs.iloc[0]["test_window_end"]), split.test_frame["bucket_start"].max())

    def test_build_evaluation_summaries_creates_station_and_regime_views(self) -> None:
        evaluation_metrics = pd.DataFrame(
            [
                {
                    "window_role": "validation",
                    "frequency": "daily",
                    "segment_type": "system_total",
                    "segment_id": "all",
                    "model_name": "seasonal_naive",
                    "metric_scope": "overall",
                    "horizon_bucket": "all",
                    "evaluation_regime": "all",
                    "mae": 10.0,
                    "rmse": 12.0,
                    "mape": 0.10,
                    "smape": 0.11,
                    "mase": 0.80,
                    "bias": -2.0,
                    "pinball_50": 5.0,
                    "pinball_80": 7.0,
                    "pinball_95": 9.0,
                    "coverage_50": 0.48,
                    "coverage_80": 0.75,
                    "coverage_95": 0.90,
                    "width_50": 20.0,
                    "width_80": 30.0,
                    "width_95": 40.0,
                },
                {
                    "window_role": "validation",
                    "frequency": "daily",
                    "segment_type": "start_station",
                    "segment_id": "101",
                    "model_name": "seasonal_naive",
                    "metric_scope": "overall",
                    "horizon_bucket": "all",
                    "evaluation_regime": "all",
                    "mae": 4.0,
                    "rmse": 5.0,
                    "mape": 0.09,
                    "smape": 0.10,
                    "mase": 0.70,
                    "bias": -1.0,
                    "pinball_50": 2.0,
                    "pinball_80": 3.0,
                    "pinball_95": 4.0,
                    "coverage_50": 0.50,
                    "coverage_80": 0.78,
                    "coverage_95": 0.93,
                    "width_50": 9.0,
                    "width_80": 12.0,
                    "width_95": 16.0,
                },
                {
                    "window_role": "validation",
                    "frequency": "daily",
                    "segment_type": "start_station",
                    "segment_id": "102",
                    "model_name": "seasonal_naive",
                    "metric_scope": "overall",
                    "horizon_bucket": "all",
                    "evaluation_regime": "all",
                    "mae": 8.0,
                    "rmse": 9.0,
                    "mape": 0.14,
                    "smape": 0.15,
                    "mase": 1.10,
                    "bias": 2.0,
                    "pinball_50": 3.0,
                    "pinball_80": 4.0,
                    "pinball_95": 5.0,
                    "coverage_50": 0.44,
                    "coverage_80": 0.70,
                    "coverage_95": 0.88,
                    "width_50": 11.0,
                    "width_80": 15.0,
                    "width_95": 19.0,
                },
                {
                    "window_role": "validation",
                    "frequency": "daily",
                    "segment_type": "start_station",
                    "segment_id": "101",
                    "model_name": "seasonal_naive",
                    "metric_scope": "regime",
                    "horizon_bucket": "all",
                    "evaluation_regime": "post_pandemic",
                    "mae": 5.0,
                    "rmse": 6.0,
                    "mape": 0.10,
                    "smape": 0.11,
                    "mase": 0.75,
                    "bias": 0.5,
                    "pinball_50": 2.5,
                    "pinball_80": 3.5,
                    "pinball_95": 4.5,
                    "coverage_50": 0.51,
                    "coverage_80": 0.81,
                    "coverage_95": 0.94,
                    "width_50": 10.0,
                    "width_80": 13.0,
                    "width_95": 17.0,
                },
                {
                    "window_role": "validation",
                    "frequency": "daily",
                    "segment_type": "start_station",
                    "segment_id": "101",
                    "model_name": "seasonal_naive",
                    "metric_scope": "horizon_bucket",
                    "horizon_bucket": "short",
                    "evaluation_regime": "all",
                    "mae": 3.5,
                    "rmse": 4.5,
                    "mape": 0.08,
                    "smape": 0.09,
                    "mase": 0.60,
                    "bias": 0.2,
                    "pinball_50": 1.5,
                    "pinball_80": 2.2,
                    "pinball_95": 3.0,
                    "coverage_50": 0.52,
                    "coverage_80": 0.82,
                    "coverage_95": 0.95,
                    "width_50": 8.0,
                    "width_80": 11.0,
                    "width_95": 14.0,
                },
            ]
        )
        station_profiles = pd.DataFrame(
            [
                {
                    "frequency": "daily",
                    "station_id": "101",
                    "total_trip_count": 1000.0,
                    "recent_share": 0.60,
                    "volume_tier": "high_volume",
                },
                {
                    "frequency": "daily",
                    "station_id": "102",
                    "total_trip_count": 500.0,
                    "recent_share": 0.40,
                    "volume_tier": "medium_volume",
                },
            ]
        )

        summaries = self.pipeline._build_evaluation_summaries(evaluation_metrics, station_profiles)

        self.assertIn("segment_evaluation_summary", summaries)
        self.assertIn("station_tier_evaluation_summary", summaries)
        self.assertIn("regime_evaluation_summary", summaries)
        self.assertIn("horizon_evaluation_summary", summaries)

        segment_summary = summaries["segment_evaluation_summary"]
        self.assertIn("macro_mae", segment_summary.columns)
        self.assertIn("weighted_mae", segment_summary.columns)

        station_tier_summary = summaries["station_tier_evaluation_summary"]
        self.assertEqual(set(station_tier_summary["volume_tier"]), {"high_volume", "medium_volume"})

        regime_summary = summaries["regime_evaluation_summary"]
        self.assertEqual(regime_summary.iloc[0]["evaluation_regime"], "post_pandemic")

        horizon_summary = summaries["horizon_evaluation_summary"]
        self.assertEqual(horizon_summary.iloc[0]["horizon_bucket"], "short")


if __name__ == "__main__":
    unittest.main()
