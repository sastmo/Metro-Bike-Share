from __future__ import annotations

import numpy as np
import pandas as pd


def mae(actual: pd.Series, predicted: pd.Series) -> float:
    return float(np.mean(np.abs(actual - predicted)))


def rmse(actual: pd.Series, predicted: pd.Series) -> float:
    return float(np.sqrt(np.mean(np.square(actual - predicted))))


def smape(actual: pd.Series, predicted: pd.Series) -> float:
    denominator = np.abs(actual) + np.abs(predicted)
    safe = np.where(denominator == 0, 1.0, denominator)
    return float(np.mean(2 * np.abs(actual - predicted) / safe))


def mape(actual: pd.Series, predicted: pd.Series) -> float:
    safe = np.where(actual == 0, 1.0, actual)
    return float(np.mean(np.abs((actual - predicted) / safe)))


def mase(actual: pd.Series, predicted: pd.Series, training_series: pd.Series, season_length: int) -> float:
    training_array = pd.Series(training_series).dropna().to_numpy(dtype=float)
    if len(training_array) <= max(season_length, 1):
        if len(training_array) > 1:
            naive_errors = np.abs(np.diff(training_array))
        else:
            naive_errors = np.array([1.0])
    else:
        naive_errors = np.abs(training_array[season_length:] - training_array[:-season_length])
    scale = np.mean(naive_errors) if len(naive_errors) else 1.0
    scale = scale if scale else 1.0
    return float(np.mean(np.abs(actual - predicted)) / scale)


def pinball_loss(actual: pd.Series, lower: pd.Series, upper: pd.Series, level: float) -> float:
    alpha = (1 - level) / 2
    lower_loss = np.maximum(alpha * (actual - lower), (alpha - 1) * (actual - lower))
    upper_alpha = 1 - alpha
    upper_loss = np.maximum(upper_alpha * (actual - upper), (upper_alpha - 1) * (actual - upper))
    return float(np.mean(lower_loss + upper_loss))


def interval_coverage(actual: pd.Series, lower: pd.Series, upper: pd.Series) -> float:
    covered = (actual >= lower) & (actual <= upper)
    return float(np.mean(covered))


def interval_width(lower: pd.Series, upper: pd.Series) -> float:
    return float(np.mean(upper - lower))


def bias(actual: pd.Series, predicted: pd.Series) -> float:
    return float(np.mean(predicted - actual))
