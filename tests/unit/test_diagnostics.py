from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from metro_bike_share_forecasting.diagnostics.time_series import (
    TimeSeriesDiagnosticsConfig,
    run_diagnostics,
    run_time_series_diagnostics,
)
from metro_bike_share_forecasting.features.regime import RegimeDefinition


class DiagnosticsTests(unittest.TestCase):
    def test_run_time_series_diagnostics_writes_outputs_and_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir) / "daily_diagnostics"
            timestamps = pd.date_range("2021-01-01", periods=180, freq="D")
            index = np.arange(len(timestamps))
            values = 120 + 0.25 * index + 18 * np.sin(2 * np.pi * index / 7) + 3 * np.cos(2 * np.pi * index / 30)
            frame = pd.DataFrame({"timestamp": timestamps, "value": values})

            summary = run_time_series_diagnostics(
                frame,
                TimeSeriesDiagnosticsConfig(
                    output_dir=output_dir,
                    series_name="daily_test_series",
                    frequency="daily",
                ),
            )

            self.assertEqual(summary["frequency"], "daily")
            self.assertTrue(summary["recommended_model_families"])
            self.assertTrue(summary["recommendations"])
            self.assertIsNotNone(summary["trend_strength"])
            self.assertIsNotNone(summary["seasonal_strength"])
            self.assertLess(summary["seasonal_naive_mae"], summary["naive_mae"])

            expected_files = [
                "series.png",
                "acf.png",
                "pacf.png",
                "stl.png",
                "periodogram.png",
                "distribution.png",
                "rolling_stats.png",
                "outliers.png",
                "seasonal_profile.png",
                "weekday_profile.csv",
                "monthly_profile.csv",
                "diagnostics_summary.json",
                "diagnostics_summary.csv",
                "diagnostics_report.md",
            ]
            for filename in expected_files:
                self.assertTrue((output_dir / filename).exists(), filename)

    def test_run_time_series_diagnostics_flags_multiple_seasonalities(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir) / "hourly_diagnostics"
            timestamps = pd.date_range("2021-01-01", periods=24 * 28, freq="h")
            index = np.arange(len(timestamps))
            values = (
                70
                + 12 * np.sin(2 * np.pi * index / 24)
                + 7 * np.sin(2 * np.pi * index / 168)
                + 0.5 * np.cos(2 * np.pi * index / 12)
            )
            frame = pd.DataFrame({"timestamp": timestamps, "value": values})

            summary = run_time_series_diagnostics(
                frame,
                TimeSeriesDiagnosticsConfig(
                    output_dir=output_dir,
                    series_name="hourly_multi_seasonal",
                    frequency="hourly",
                ),
            )

            self.assertTrue(summary["multiple_seasonalities_detected"])
            self.assertIn("TBATS / multi-seasonal state space", summary["recommended_model_families"])
            self.assertIn("Fourier-based regression or dynamic harmonic regression", summary["recommended_model_families"])

    def test_run_diagnostics_wrapper_handles_composite_frequency_key(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_root = Path(temp_dir)
            frame = pd.DataFrame(
                {
                    "bucket_start": pd.date_range("2022-01-01", periods=90, freq="D"),
                    "trip_count": 200 + 10 * np.sin(2 * np.pi * np.arange(90) / 7),
                }
            )
            regime_definition = RegimeDefinition(
                pandemic_shock_start=pd.Timestamp("2020-03-15"),
                recovery_start=pd.Timestamp("2021-06-15"),
                post_pandemic_start=pd.Timestamp("2022-06-15"),
                detected_breakpoints=[],
                detection_method="known_dates_only",
            )

            summary = run_diagnostics(
                frame=frame,
                frequency="daily__system_total__all",
                output_root=output_root,
                regime_definition=regime_definition,
            )

            target_dir = output_root / "daily__system_total__all"
            self.assertEqual(summary["frequency"], "daily")
            self.assertEqual(summary["series_key"], "daily__system_total__all")
            self.assertTrue((target_dir / "diagnostics_summary.json").exists())
            self.assertTrue((target_dir / "diagnostics_report.md").exists())


if __name__ == "__main__":
    unittest.main()
