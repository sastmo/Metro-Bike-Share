from __future__ import annotations

import numpy as np
import pandas as pd

from metro_bike_share_forecasting.forecasting.base import BaseForecaster
from metro_bike_share_forecasting.utils.time import build_future_index, infer_season_length


def _empirical_interval(predictions: np.ndarray, residuals: np.ndarray, level: float) -> tuple[np.ndarray, np.ndarray]:
    alpha = (1 - level) / 2
    lower_offset = np.quantile(residuals, alpha) if len(residuals) else 0.0
    upper_offset = np.quantile(residuals, 1 - alpha) if len(residuals) else 0.0
    return np.maximum(predictions + lower_offset, 0.0), np.maximum(predictions + upper_offset, 0.0)


class SeasonalNaiveForecaster(BaseForecaster):
    def __init__(self, frequency: str) -> None:
        self.frequency = frequency
        self.name = "seasonal_naive"
        self.season_length = infer_season_length(frequency)
        self.residuals: np.ndarray = np.array([])

    def fit(self, history: pd.DataFrame) -> "SeasonalNaiveForecaster":
        values = history["trip_count"].to_numpy(dtype=float)
        if len(values) > self.season_length:
            self.residuals = values[self.season_length:] - values[:-self.season_length]
        elif len(values) > 1:
            self.residuals = np.diff(values)
        else:
            self.residuals = np.array([0.0])
        return self

    def forecast(self, history: pd.DataFrame, horizon: int) -> pd.DataFrame:
        history = history.sort_values("bucket_start").reset_index(drop=True)
        values = history["trip_count"].astype(float).tolist()
        future_index = build_future_index(history["bucket_start"].iloc[-1], self.frequency, horizon)
        predictions: list[float] = []
        season_length = min(self.season_length, len(values)) if values else 1

        for _ in range(horizon):
            if len(values) >= season_length:
                prediction = float(values[-season_length])
            else:
                prediction = float(values[-1])
            predictions.append(max(prediction, 0.0))
            values.append(prediction)

        predictions_array = np.array(predictions)
        frame = pd.DataFrame(
            {
                "target_timestamp": future_index,
                "prediction": predictions_array,
                "model_name": self.name,
            }
        )
        for level in (0.50, 0.80, 0.95):
            lower, upper = _empirical_interval(predictions_array, self.residuals, level)
            frame[f"lower_{int(level * 100)}"] = lower
            frame[f"upper_{int(level * 100)}"] = upper
        return frame


class NaiveForecaster(BaseForecaster):
    def __init__(self, frequency: str) -> None:
        self.frequency = frequency
        self.name = "naive"
        self.residuals: np.ndarray = np.array([])

    def fit(self, history: pd.DataFrame) -> "NaiveForecaster":
        values = history["trip_count"].to_numpy(dtype=float)
        self.residuals = np.diff(values) if len(values) > 1 else np.array([0.0])
        return self

    def forecast(self, history: pd.DataFrame, horizon: int) -> pd.DataFrame:
        history = history.sort_values("bucket_start").reset_index(drop=True)
        last_value = float(history["trip_count"].iloc[-1])
        future_index = build_future_index(history["bucket_start"].iloc[-1], self.frequency, horizon)
        predictions_array = np.repeat(max(last_value, 0.0), horizon)
        frame = pd.DataFrame(
            {
                "target_timestamp": future_index,
                "prediction": predictions_array,
                "model_name": self.name,
            }
        )
        for level in (0.50, 0.80, 0.95):
            lower, upper = _empirical_interval(predictions_array, self.residuals, level)
            frame[f"lower_{int(level * 100)}"] = lower
            frame[f"upper_{int(level * 100)}"] = upper
        return frame


class RollingMeanForecaster(BaseForecaster):
    def __init__(self, frequency: str, window: int | None = None) -> None:
        self.frequency = frequency
        self.name = "rolling_mean"
        self.window = window or max(3, infer_season_length(frequency))
        self.residuals: np.ndarray = np.array([])

    def fit(self, history: pd.DataFrame) -> "RollingMeanForecaster":
        values = history["trip_count"].to_numpy(dtype=float)
        if len(values) > 1:
            baseline = pd.Series(values).shift(1).rolling(window=min(self.window, len(values)), min_periods=1).mean()
            self.residuals = (pd.Series(values) - baseline).dropna().to_numpy(dtype=float)
        else:
            self.residuals = np.array([0.0])
        return self

    def forecast(self, history: pd.DataFrame, horizon: int) -> pd.DataFrame:
        history = history.sort_values("bucket_start").reset_index(drop=True)
        values = history["trip_count"].astype(float).tolist()
        future_index = build_future_index(history["bucket_start"].iloc[-1], self.frequency, horizon)
        predictions: list[float] = []

        for _ in range(horizon):
            window_values = values[-min(self.window, len(values)) :]
            prediction = float(np.mean(window_values)) if window_values else 0.0
            prediction = max(prediction, 0.0)
            predictions.append(prediction)
            values.append(prediction)

        predictions_array = np.array(predictions)
        frame = pd.DataFrame(
            {
                "target_timestamp": future_index,
                "prediction": predictions_array,
                "model_name": self.name,
            }
        )
        for level in (0.50, 0.80, 0.95):
            lower, upper = _empirical_interval(predictions_array, self.residuals, level)
            frame[f"lower_{int(level * 100)}"] = lower
            frame[f"upper_{int(level * 100)}"] = upper
        return frame
