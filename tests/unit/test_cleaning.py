from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from metro_bike_share_forecasting.cleaning.cleaner import clean_trip_data
from metro_bike_share_forecasting.config.settings import get_settings


class CleaningTests(unittest.TestCase):
    def test_clean_trip_data_applies_legacy_style_filters(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "trip_id": "1",
                    "duration": "5",
                    "start_time": "2019-01-01 00:00:00",
                    "end_time": "2019-01-01 00:05:00",
                    "start_station": "3005",
                    "start_lat": "34.0",
                    "start_lon": "-118.2",
                    "end_station": "3006",
                    "end_lat": "34.1",
                    "end_lon": "-118.3",
                    "bike_id": "100",
                    "plan_duration": "30",
                    "trip_route_category": "one way",
                    "passholder_type": "monthly pass",
                    "bike_type": "electric",
                    "source_file": "sample.csv",
                    "source_row_number": 2,
                },
                {
                    "trip_id": "1",
                    "duration": "6",
                    "start_time": "2019-01-01 00:00:00",
                    "end_time": "2019-01-01 00:06:00",
                    "start_station": "3005",
                    "start_lat": "34.0",
                    "start_lon": "-118.2",
                    "end_station": "3006",
                    "end_lat": "34.1",
                    "end_lon": "-118.3",
                    "bike_id": "100",
                    "plan_duration": "30",
                    "trip_route_category": "one way",
                    "passholder_type": "monthly pass",
                    "bike_type": "electric",
                    "source_file": "sample.csv",
                    "source_row_number": 3,
                },
                {
                    "trip_id": "2",
                    "duration": "7",
                    "start_time": "2019-01-01 00:10:00",
                    "end_time": "2019-01-01 00:17:00",
                    "start_station": "3005",
                    "start_lat": "0",
                    "start_lon": "0",
                    "end_station": "3006",
                    "end_lat": "34.1",
                    "end_lon": "-118.3",
                    "bike_id": "100",
                    "plan_duration": "30",
                    "trip_route_category": "one way",
                    "passholder_type": "testing",
                    "bike_type": "electric",
                    "source_file": "sample.csv",
                    "source_row_number": 4,
                },
            ]
        )
        result = clean_trip_data(raw, get_settings())
        self.assertEqual(len(result.cleaned_data), 1)
        self.assertEqual(result.quality_summary["duplicates_removed"], 1)
        self.assertEqual(result.cleaned_data["passholder_type"].iloc[0], "Monthly Pass")


if __name__ == "__main__":
    unittest.main()

