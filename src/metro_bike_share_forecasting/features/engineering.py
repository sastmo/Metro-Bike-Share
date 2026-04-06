from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

try:
    import holidays
except ModuleNotFoundError:  # pragma: no cover - optional until dependencies are installed
    holidays = None

from metro_bike_share_forecasting.features.regime import RegimeDefinition, add_regime_features
from metro_bike_share_forecasting.utils.time import infer_season_length


FOURIER_CONFIG = {
    "hourly": [(24, 3), (24 * 7, 2)],
    "daily": [(7, 3), (365.25, 3)],
    "weekly": [(52, 2)],
    "monthly": [(12, 2)],
    "quarterly": [(4, 1)],
}


LAG_CONFIG = {
    "hourly": [1, 24, 24 * 7],
    "daily": [1, 7, 28],
    "weekly": [1, 4, 12],
    "monthly": [1, 3, 12],
    "quarterly": [1, 2, 4],
}


ROLLING_CONFIG = {
    "hourly": [24, 24 * 7],
    "daily": [7, 28],
    "weekly": [4, 12],
    "monthly": [3, 12],
    "quarterly": [2, 4],
}


def add_calendar_features(frame: pd.DataFrame, timestamp_col: str, holiday_country: str) -> pd.DataFrame:
    enriched = frame.copy()
    timestamp = pd.to_datetime(enriched[timestamp_col])
    holiday_calendar = holidays.country_holidays(holiday_country) if holidays else set()

    enriched["year"] = timestamp.dt.year
    enriched["quarter"] = timestamp.dt.quarter
    enriched["month"] = timestamp.dt.month
    enriched["week_of_year"] = timestamp.dt.isocalendar().week.astype(int)
    enriched["day_of_week"] = timestamp.dt.dayofweek
    enriched["day_of_month"] = timestamp.dt.day
    enriched["hour_of_day"] = timestamp.dt.hour
    enriched["is_weekend"] = timestamp.dt.dayofweek.isin([5, 6]).astype(int)
    enriched["is_month_start"] = timestamp.dt.is_month_start.astype(int)
    enriched["is_month_end"] = timestamp.dt.is_month_end.astype(int)
    enriched["is_quarter_start"] = timestamp.dt.is_quarter_start.astype(int)
    enriched["is_quarter_end"] = timestamp.dt.is_quarter_end.astype(int)
    enriched["is_holiday"] = timestamp.dt.date.map(lambda value: int(value in holiday_calendar))
    return enriched


def add_fourier_terms(frame: pd.DataFrame, timestamp_col: str, frequency: str) -> pd.DataFrame:
    enriched = frame.copy()
    group_cols = ["segment_type", "segment_id"] if {"segment_type", "segment_id"}.issubset(enriched.columns) else []
    group_obj = enriched.groupby(group_cols, dropna=False) if group_cols else [(None, enriched)]
    frames = []

    for _, group in group_obj:
        group = group.sort_values(timestamp_col).copy()
        time_index = np.arange(len(group), dtype=float)
        for period, order in FOURIER_CONFIG[frequency]:
            for k in range(1, order + 1):
                group[f"fourier_sin_{int(period)}_{k}"] = np.sin(2 * np.pi * k * time_index / period)
                group[f"fourier_cos_{int(period)}_{k}"] = np.cos(2 * np.pi * k * time_index / period)
        frames.append(group)

    return pd.concat(frames, ignore_index=True)


def add_lag_and_rolling_features(frame: pd.DataFrame, target_col: str, frequency: str) -> pd.DataFrame:
    enriched = frame.copy()
    group_cols = ["segment_type", "segment_id"] if {"segment_type", "segment_id"}.issubset(enriched.columns) else []
    group_obj = enriched.groupby(group_cols, dropna=False) if group_cols else [(None, enriched)]

    frames = []
    for _, group in group_obj:
        group = group.sort_values("bucket_start").copy()
        for lag in LAG_CONFIG[frequency]:
            group[f"lag_{lag}"] = group[target_col].shift(lag)
        for window in ROLLING_CONFIG[frequency]:
            shifted = group[target_col].shift(1)
            group[f"rolling_mean_{window}"] = shifted.rolling(window=window, min_periods=1).mean()
            group[f"rolling_std_{window}"] = shifted.rolling(window=window, min_periods=1).std().fillna(0.0)
            group[f"rolling_min_{window}"] = shifted.rolling(window=window, min_periods=1).min()
            group[f"rolling_max_{window}"] = shifted.rolling(window=window, min_periods=1).max()
        frames.append(group)

    return pd.concat(frames, ignore_index=True)


def add_regime_interactions(frame: pd.DataFrame) -> pd.DataFrame:
    enriched = frame.copy()
    enriched["hour_x_phase"] = enriched["hour_of_day"].astype(str) + "_" + enriched["pandemic_phase"]
    enriched["dow_x_phase"] = enriched["day_of_week"].astype(str) + "_" + enriched["pandemic_phase"]
    enriched["month_x_phase"] = enriched["month"].astype(str) + "_" + enriched["pandemic_phase"]
    enriched["weekend_x_phase"] = enriched["is_weekend"].astype(str) + "_" + enriched["pandemic_phase"]
    return enriched


def build_feature_store(
    aggregate_frame: pd.DataFrame,
    frequency: str,
    regime_definition: RegimeDefinition,
    holiday_country: str,
) -> pd.DataFrame:
    feature_frame = add_calendar_features(aggregate_frame, "bucket_start", holiday_country)
    feature_frame = add_regime_features(feature_frame, "bucket_start", regime_definition)
    feature_frame = add_regime_interactions(feature_frame)
    feature_frame = add_fourier_terms(feature_frame, "bucket_start", frequency)
    feature_frame = add_lag_and_rolling_features(feature_frame, "trip_count", frequency)
    feature_frame["season_length"] = infer_season_length(frequency)
    if {"segment_type", "segment_id"}.issubset(feature_frame.columns):
        weighted_frames = []
        for _, group in feature_frame.groupby(["segment_type", "segment_id"], dropna=False):
            group = group.sort_values("bucket_start").copy()
            group["recency_weight"] = np.linspace(0.3, 1.0, num=len(group))
            weighted_frames.append(group)
        feature_frame = pd.concat(weighted_frames, ignore_index=True)
    else:
        feature_frame["recency_weight"] = np.linspace(0.3, 1.0, num=len(feature_frame))
    return feature_frame


def to_feature_payload(frame: pd.DataFrame, keep_columns: Iterable[str]) -> pd.DataFrame:
    keep_columns = list(keep_columns)
    payload_columns = [column for column in frame.columns if column not in keep_columns]
    result = frame[keep_columns].copy()
    result["feature_payload"] = frame[payload_columns].replace({pd.NA: None}).to_dict(orient="records")
    return result
