from __future__ import annotations

import os
import json
import sys
import tempfile
import unittest
import gzip
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from metro_bike_share_forecasting.reporting import (
    build_dashboard_context,
    find_latest_summary,
    list_diagnostic_images,
    load_station_coordinates,
    load_csv_artifact,
)


class ReportingTests(unittest.TestCase):
    def test_reporting_helpers_find_latest_summary_and_images(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            reports_dir = project_root / "outputs" / "reports"
            figures_dir = project_root / "outputs" / "figures" / "daily"
            processed_dir = project_root / "data" / "processed"
            forecasts_dir = project_root / "outputs" / "forecasts"
            reports_dir.mkdir(parents=True)
            figures_dir.mkdir(parents=True)
            processed_dir.mkdir(parents=True)
            forecasts_dir.mkdir(parents=True)

            older = reports_dir / "forecasting_20260101_summary.json"
            latest = reports_dir / "forecasting_20260102_summary.json"
            older.write_text(json.dumps({"pipeline_run_id": "older"}))
            latest.write_text(json.dumps({"pipeline_run_id": "latest"}))
            os.utime(older, (1, 1))
            os.utime(latest, (2, 2))
            (figures_dir / "series.png").write_bytes(b"png")
            (processed_dir / "cleaned_trip_data.csv.gz").write_bytes(b"csv")
            (forecasts_dir / "forecast_outputs.csv").write_text("a,b\n1,2\n")

            self.assertEqual(find_latest_summary(reports_dir), latest)
            self.assertIn("daily", list_diagnostic_images(project_root / "outputs" / "figures"))

            context = build_dashboard_context(project_root)
            self.assertEqual(context["summary"]["pipeline_run_id"], "latest")
            self.assertEqual(len(context["processed_artifacts"]), 1)
            self.assertEqual(len(context["forecast_artifacts"]), 1)

    def test_load_csv_artifact_ignores_missing_parse_date_columns(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reports_dir = root / "outputs" / "reports"
            forecasts_dir = root / "outputs" / "forecasts"
            reports_dir.mkdir(parents=True)
            forecasts_dir.mkdir(parents=True)

            summary = {
                "artifact_paths": {
                    "forecast_outputs": str(forecasts_dir / "forecast.csv"),
                }
            }
            (forecasts_dir / "forecast.csv").write_text(
                "forecast_id,target_timestamp,prediction\n"
                "f1,2025-01-01,100\n"
            )

            frame = load_csv_artifact(
                summary,
                "forecast_outputs",
                forecasts_dir,
                "*_forecast_outputs.csv",
                parse_dates=[
                    "target_timestamp",
                    "selection_train_window_start",
                    "selection_train_window_end",
                    "validation_window_start",
                    "validation_window_end",
                    "test_window_start",
                    "test_window_end",
                ],
            )
            self.assertEqual(len(frame), 1)
            self.assertIn("target_timestamp", frame.columns)

    def test_load_station_coordinates_builds_station_point_frame(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            processed_dir = Path(temp_dir)
            with gzip.open(processed_dir / "cleaned_trip_data.csv.gz", "wt") as handle:
                handle.write(
                    "start_station,start_lat,start_lon\n"
                    "3001,34.05,-118.25\n"
                    "3001,34.05,-118.25\n"
                    "3002,34.06,-118.24\n"
                )
            frame = load_station_coordinates(processed_dir)
            self.assertEqual(len(frame), 2)
            self.assertIn("station_id", frame.columns)
            self.assertIn("latitude", frame.columns)
            self.assertIn("longitude", frame.columns)


if __name__ == "__main__":
    unittest.main()
