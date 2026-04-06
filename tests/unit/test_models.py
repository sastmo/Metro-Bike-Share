from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from metro_bike_share_forecasting.config.settings import get_settings
from metro_bike_share_forecasting.features.engineering import build_feature_store
from metro_bike_share_forecasting.features.regime import add_regime_features, derive_regime_definition
from metro_bike_share_forecasting.forecasting.classical.glm_count import CountGLMForecaster


class ModelTests(unittest.TestCase):
    def test_count_glm_fit_and_forecast_produce_numeric_predictions(self) -> None:
        frame = pd.DataFrame(
            {
                "bucket_start": pd.date_range("2022-01-01", periods=70, freq="D"),
                "trip_count": [120 + (idx % 7) * 8 + (idx // 7) for idx in range(70)],
                "segment_type": "system_total",
                "segment_id": "all",
                "distinct_bikes": [40 + (idx % 5) for idx in range(70)],
                "avg_duration_minutes": [18 + (idx % 9) for idx in range(70)],
                "is_observed": True,
                "missing_period_flag": False,
            }
        )
        settings = get_settings()
        regime = derive_regime_definition(frame[["bucket_start", "trip_count"]].copy(), settings)
        feature_frame = build_feature_store(frame, "daily", regime, settings.holiday_country)
        feature_frame = add_regime_features(feature_frame, "bucket_start", regime)

        model = CountGLMForecaster("daily", regime, settings.holiday_country).fit(feature_frame)
        forecast = model.forecast(feature_frame, 5)

        self.assertEqual(len(forecast), 5)
        self.assertTrue(pd.api.types.is_numeric_dtype(forecast["prediction"]))
        self.assertTrue((forecast["prediction"] >= 0).all())


if __name__ == "__main__":
    unittest.main()
