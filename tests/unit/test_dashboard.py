from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from metro_bike_share_forecasting.dashboard import _build_diagnostics_frame, _build_interval_view


class DashboardTests(unittest.TestCase):
    def test_build_diagnostics_frame_handles_existing_frequency_column(self) -> None:
        summary = {
            "diagnostics_summary": {
                "daily": {
                    "frequency": "daily",
                    "row_count": 10,
                    "mean_trip_count": 100.0,
                }
            }
        }
        frame = _build_diagnostics_frame(summary)
        self.assertIn("diagnostics_key", frame.columns)
        self.assertIn("segment_type", frame.columns)
        self.assertIn("segment_id", frame.columns)
        self.assertEqual(frame.iloc[0]["frequency"], "daily")
        self.assertEqual(frame.iloc[0]["segment_type"], "system_total")
        self.assertEqual(frame.iloc[0]["segment_id"], "all")

    def test_build_interval_view_merges_interval_levels(self) -> None:
        outputs = pd.DataFrame(
            [
                {
                    "forecast_id": "f1",
                    "frequency": "daily",
                    "model_name": "seasonal_naive",
                    "segment_type": "system_total",
                    "segment_id": "all",
                    "target_timestamp": pd.Timestamp("2025-01-01"),
                    "prediction": 100.0,
                    "horizon": 1,
                }
            ]
        )
        intervals = pd.DataFrame(
            [
                {"forecast_id": "f1", "interval_level": 80, "lower_bound": 90.0, "upper_bound": 110.0},
                {"forecast_id": "f1", "interval_level": 95, "lower_bound": 80.0, "upper_bound": 120.0},
            ]
        )
        merged = _build_interval_view(outputs, intervals, "daily", "seasonal_naive", "system_total", "all")
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged.iloc[0]["lower_bound_80"], 90.0)
        self.assertEqual(merged.iloc[0]["upper_bound_95"], 120.0)


if __name__ == "__main__":
    unittest.main()
