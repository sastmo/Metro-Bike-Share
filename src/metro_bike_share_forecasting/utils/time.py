from __future__ import annotations

from datetime import timedelta

import pandas as pd
from pandas.tseries.frequencies import to_offset


FREQUENCY_TO_PANDAS_ALIAS = {
    "hourly": "h",
    "daily": "D",
    "weekly": "W-MON",
    "monthly": "MS",
    "quarterly": "QS",
}


def infer_season_length(frequency: str) -> int:
    mapping = {
        "hourly": 24 * 7,
        "daily": 7,
        "weekly": 52,
        "monthly": 12,
        "quarterly": 4,
    }
    return mapping[frequency]


def build_future_index(last_timestamp: pd.Timestamp, frequency: str, horizon: int) -> pd.DatetimeIndex:
    alias = FREQUENCY_TO_PANDAS_ALIAS[frequency]
    start = last_timestamp + to_offset(alias)
    return pd.date_range(start=start, periods=horizon, freq=alias)


def bucket_end_from_start(bucket_start: pd.Series, frequency: str) -> pd.Series:
    alias = FREQUENCY_TO_PANDAS_ALIAS[frequency]
    offset = to_offset(alias)
    return bucket_start + offset


def timedelta_to_minutes(delta: pd.Series) -> pd.Series:
    return delta / timedelta(minutes=1)
