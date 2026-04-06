from __future__ import annotations

import numpy as np
import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX

from metro_bike_share_forecasting.features.engineering import build_feature_store
from metro_bike_share_forecasting.forecasting.base import BaseForecaster
from metro_bike_share_forecasting.utils.time import build_future_index


class SarimaxFourierForecaster(BaseForecaster):
    def __init__(self, frequency: str, regime_definition, holiday_country: str) -> None:
        self.frequency = frequency
        self.regime_definition = regime_definition
        self.holiday_country = holiday_country
        self.name = "sarimax_fourier"
        self.result = None
        self.exog_columns: list[str] = []

    def _build_exog(self, frame: pd.DataFrame) -> pd.DataFrame:
        feature_frame = build_feature_store(frame, self.frequency, self.regime_definition, self.holiday_country)
        exog_columns = [
            column
            for column in feature_frame.columns
            if column.startswith("fourier_")
            or column in {"is_weekend", "is_holiday", "is_lockdown", "is_reopening", "is_post_pandemic"}
        ]
        exog = feature_frame[exog_columns].replace([np.inf, -np.inf], np.nan).fillna(0.0).astype(float)
        return exog

    def fit(self, history: pd.DataFrame) -> "SarimaxFourierForecaster":
        history = history.sort_values("bucket_start").reset_index(drop=True)
        exog = self._build_exog(history)
        self.exog_columns = exog.columns.tolist()
        model = SARIMAX(
            history["trip_count"].astype(float),
            order=(1, 0, 1),
            exog=exog,
            trend="c",
            enforce_stationarity=False,
            enforce_invertibility=False,
        )
        self.result = model.fit(disp=False)
        return self

    def forecast(self, history: pd.DataFrame, horizon: int) -> pd.DataFrame:
        if self.result is None:
            raise RuntimeError("Model must be fitted before forecasting.")
        history = history.sort_values("bucket_start").reset_index(drop=True)
        future_index = build_future_index(history["bucket_start"].iloc[-1], self.frequency, horizon)
        future_frame = pd.DataFrame(
            {
                "bucket_start": future_index,
                "trip_count": np.nan,
                "segment_type": history["segment_type"].iloc[-1],
                "segment_id": history["segment_id"].iloc[-1],
            }
        )
        exog_future = self._build_exog(pd.concat([history, future_frame], ignore_index=True)).tail(horizon)
        forecast_mean = self.result.get_forecast(steps=horizon, exog=exog_future)
        frame = pd.DataFrame(
            {
                "target_timestamp": future_index,
                "prediction": np.maximum(forecast_mean.predicted_mean, 0.0),
                "model_name": self.name,
            }
        )
        for level in (0.50, 0.80, 0.95):
            conf_int = forecast_mean.conf_int(alpha=1 - level)
            frame[f"lower_{int(level * 100)}"] = np.maximum(conf_int.iloc[:, 0].to_numpy(), 0.0)
            frame[f"upper_{int(level * 100)}"] = np.maximum(conf_int.iloc[:, 1].to_numpy(), 0.0)
        return frame
