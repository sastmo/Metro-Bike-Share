from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from metro_bike_share_forecasting.config.settings import get_settings
from metro_bike_share_forecasting.features.regime import add_regime_features, derive_regime_definition


class RegimeTests(unittest.TestCase):
    def test_regime_labels_cover_known_phases(self) -> None:
        dates = pd.date_range("2019-01-01", periods=1600, freq="D")
        frame = pd.DataFrame({"bucket_start": dates, "trip_count": 100})
        definition = derive_regime_definition(frame, get_settings())
        labeled = add_regime_features(frame.iloc[[0, 500, 950, 1400]].copy(), "bucket_start", definition)
        self.assertIn("pre_pandemic", set(labeled["pandemic_phase"]))
        self.assertIn("pandemic_shock", set(labeled["pandemic_phase"]))
        self.assertIn("recovery", set(labeled["pandemic_phase"]))
        self.assertIn("post_pandemic", set(labeled["pandemic_phase"]))


if __name__ == "__main__":
    unittest.main()

