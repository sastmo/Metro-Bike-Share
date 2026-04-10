from __future__ import annotations

import json
import math
import os
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
from scipy.signal import periodogram
from scipy.stats import kurtosis, skew

os.environ.setdefault("MPLCONFIGDIR", "/tmp/mplconfig")

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.seasonal import STL
from statsmodels.tsa.stattools import adfuller, kpss

try:
    from statsmodels.tsa.seasonal import MSTL
except ImportError:  # pragma: no cover - depends on statsmodels version
    MSTL = None

from metro_bike_share_forecasting.features.regime import RegimeDefinition


FREQUENCY_DEFAULTS: dict[str, dict[str, Any]] = {
    "hourly": {
        "expected_frequency": "h",
        "candidate_periods": (24, 168),
        "primary_period": 24,
        "rolling_window": 24,
        "max_acf_lags": 168,
    },
    "daily": {
        "expected_frequency": "D",
        "candidate_periods": (7, 30, 365),
        "primary_period": 7,
        "rolling_window": 28,
        "max_acf_lags": 56,
    },
    "weekly": {
        "expected_frequency": "W-MON",
        "candidate_periods": (4, 13, 52),
        "primary_period": 13,
        "rolling_window": 13,
        "max_acf_lags": 52,
    },
    "monthly": {
        "expected_frequency": "MS",
        "candidate_periods": (12,),
        "primary_period": 12,
        "rolling_window": 12,
        "max_acf_lags": 24,
    },
    "quarterly": {
        "expected_frequency": "QS",
        "candidate_periods": (4,),
        "primary_period": 4,
        "rolling_window": 8,
        "max_acf_lags": 16,
    },
}


@dataclass(frozen=True)
class DiagnosticEvent:
    label: str
    timestamp: pd.Timestamp
    color: str = "#d9534f"


@dataclass
class TimeSeriesDiagnosticsConfig:
    output_dir: Path
    series_name: str
    frequency: str
    timestamp_col: str = "timestamp"
    value_col: str = "value"
    expected_frequency: str | None = None
    candidate_periods: tuple[int, ...] = ()
    primary_period: int | None = None
    max_acf_lags: int | None = None
    rolling_window: int | None = None
    outlier_threshold: float = 5.0
    events: tuple[DiagnosticEvent, ...] = field(default_factory=tuple)


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (float, int)) and not math.isnan(float(value)):
        return float(value)
    if isinstance(value, np.floating) and np.isfinite(value):
        return float(value)
    if isinstance(value, np.integer):
        return float(value)
    return None


def _serialize(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _serialize(inner) for key, inner in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    return value


def _base_frequency_label(frequency: str) -> str:
    return str(frequency).split("__", 1)[0].strip().lower()


def _config_with_defaults(config: TimeSeriesDiagnosticsConfig) -> TimeSeriesDiagnosticsConfig:
    defaults = FREQUENCY_DEFAULTS.get(_base_frequency_label(config.frequency), {})
    return TimeSeriesDiagnosticsConfig(
        output_dir=config.output_dir,
        series_name=config.series_name,
        frequency=config.frequency,
        timestamp_col=config.timestamp_col,
        value_col=config.value_col,
        expected_frequency=config.expected_frequency or defaults.get("expected_frequency"),
        candidate_periods=config.candidate_periods or tuple(defaults.get("candidate_periods", ())),
        primary_period=config.primary_period or defaults.get("primary_period"),
        max_acf_lags=config.max_acf_lags or defaults.get("max_acf_lags"),
        rolling_window=config.rolling_window or defaults.get("rolling_window"),
        outlier_threshold=config.outlier_threshold,
        events=config.events,
    )


def _prepare_series_frame(frame: pd.DataFrame, config: TimeSeriesDiagnosticsConfig) -> tuple[pd.DataFrame, dict[str, Any]]:
    working = frame[[config.timestamp_col, config.value_col]].copy()
    working[config.timestamp_col] = pd.to_datetime(working[config.timestamp_col], errors="coerce")
    working[config.value_col] = pd.to_numeric(working[config.value_col], errors="coerce")
    working = working.dropna(subset=[config.timestamp_col]).sort_values(config.timestamp_col)

    dropped_missing_values = int(working[config.value_col].isna().sum())
    grouped = (
        working.groupby(config.timestamp_col, as_index=False)[config.value_col]
        .sum(min_count=1)
        .rename(columns={config.timestamp_col: "timestamp", config.value_col: "observed_value"})
    )
    duplicate_timestamps = max(len(working) - len(grouped), 0)

    inferred_frequency = pd.infer_freq(grouped["timestamp"]) if len(grouped) >= 3 else None
    expected_frequency = config.expected_frequency or inferred_frequency
    if expected_frequency:
        full_index = pd.date_range(grouped["timestamp"].min(), grouped["timestamp"].max(), freq=expected_frequency)
        prepared = pd.DataFrame({"timestamp": full_index}).merge(grouped, on="timestamp", how="left")
    else:
        prepared = grouped.rename(columns={"observed_value": "observed_value"}).copy()
        prepared["timestamp"] = pd.to_datetime(prepared["timestamp"])

    prepared["missing_period_flag"] = prepared["observed_value"].isna().astype(int)
    prepared["value"] = prepared["observed_value"]
    prepared["value_filled"] = prepared["value"].interpolate(method="linear", limit_direction="both")
    prepared["value_filled"] = prepared["value_filled"].ffill().bfill().fillna(0.0)
    prepared["timestamp"] = pd.to_datetime(prepared["timestamp"])

    metadata = {
        "row_count": int(len(grouped)),
        "duplicate_timestamps": int(duplicate_timestamps),
        "missing_periods": int(prepared["missing_period_flag"].sum()),
        "missing_values": int(dropped_missing_values),
        "inferred_frequency": inferred_frequency,
        "expected_frequency": expected_frequency,
        "timestamp_start": prepared["timestamp"].min(),
        "timestamp_end": prepared["timestamp"].max(),
    }
    return prepared, metadata


def _choose_primary_period(candidate_periods: Iterable[int], series_length: int, fallback: int | None) -> int | None:
    for period in candidate_periods:
        if period >= 2 and series_length >= period * 2:
            return int(period)
    if fallback and fallback >= 2 and series_length >= fallback * 2:
        return int(fallback)
    return None


def _distribution_summary(values: pd.Series) -> dict[str, Any]:
    numeric = pd.to_numeric(values, errors="coerce").dropna().astype(float)
    if numeric.empty:
        return {
            "mean": None,
            "median": None,
            "std": None,
            "min": None,
            "max": None,
            "zero_share": None,
            "skewness": None,
            "kurtosis": None,
            "is_count_like": False,
        }
    return {
        "mean": float(numeric.mean()),
        "median": float(numeric.median()),
        "std": float(numeric.std(ddof=0)),
        "min": float(numeric.min()),
        "max": float(numeric.max()),
        "zero_share": float((numeric == 0).mean()),
        "skewness": float(skew(numeric, bias=False)) if len(numeric) > 2 else None,
        "kurtosis": float(kurtosis(numeric, fisher=True, bias=False)) if len(numeric) > 3 else None,
        "is_count_like": bool(np.allclose(numeric, np.round(numeric)) and (numeric >= 0).all()),
    }


def _stationarity_summary(values: pd.Series) -> dict[str, Any]:
    numeric = pd.to_numeric(values, errors="coerce").dropna().astype(float)
    if len(numeric) < 12:
        return {"adf_pvalue": None, "kpss_pvalue": None, "stationarity_assessment": "insufficient_history"}

    adf_pvalue: float | None
    kpss_pvalue: float | None
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            adf_pvalue = float(adfuller(numeric, autolag="AIC")[1])
    except Exception:
        adf_pvalue = None

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            kpss_pvalue = float(kpss(numeric, regression="c", nlags="auto")[1])
    except Exception:
        kpss_pvalue = None

    if adf_pvalue is not None and kpss_pvalue is not None:
        if adf_pvalue <= 0.05 and kpss_pvalue > 0.05:
            assessment = "likely_stationary"
        elif adf_pvalue > 0.05 and kpss_pvalue <= 0.05:
            assessment = "likely_nonstationary"
        else:
            assessment = "mixed_signal"
    elif adf_pvalue is not None:
        assessment = "likely_stationary" if adf_pvalue <= 0.05 else "likely_nonstationary"
    else:
        assessment = "unknown"

    return {
        "adf_pvalue": adf_pvalue,
        "kpss_pvalue": kpss_pvalue,
        "stationarity_assessment": assessment,
    }


def _autocorrelation_summary(values: pd.Series, primary_period: int | None) -> dict[str, Any]:
    numeric = pd.to_numeric(values, errors="coerce").dropna().astype(float)
    lag1 = float(numeric.autocorr(lag=1)) if len(numeric) > 2 else None
    seasonal_lag = None
    if primary_period and len(numeric) > primary_period + 2:
        seasonal_lag = float(numeric.autocorr(lag=primary_period))
    return {
        "lag1_autocorrelation": lag1,
        "seasonal_lag_autocorrelation": seasonal_lag,
    }


def _dominant_periods(values: pd.Series, max_candidates: int = 5) -> list[dict[str, float]]:
    numeric = pd.to_numeric(values, errors="coerce").dropna().astype(float).to_numpy()
    if len(numeric) < 16:
        return []

    frequencies, power = periodogram(numeric, detrend="linear", scaling="spectrum")
    candidates: list[dict[str, float]] = []
    for frequency, magnitude in zip(frequencies, power):
        if frequency <= 0:
            continue
        period = 1.0 / frequency
        if not np.isfinite(period) or period < 2 or period > len(numeric) / 2:
            continue
        candidates.append({"period": float(period), "power": float(magnitude)})

    if not candidates:
        return []

    ranked = sorted(candidates, key=lambda item: item["power"], reverse=True)
    selected: list[dict[str, float]] = []
    for candidate in ranked:
        if all(abs(candidate["period"] - kept["period"]) > max(1.5, kept["period"] * 0.08) for kept in selected):
            selected.append({"period": round(candidate["period"], 2), "power": candidate["power"]})
        if len(selected) >= max_candidates:
            break
    return selected


def _decomposition_summary(
    values: pd.Series,
    candidate_periods: tuple[int, ...],
    primary_period: int | None,
) -> tuple[dict[str, Any], Any]:
    numeric = pd.to_numeric(values, errors="coerce").dropna().astype(float)
    if len(numeric) < 16:
        return {
            "decomposition_method": "none",
            "trend_strength": None,
            "seasonal_strength": None,
            "seasonality_strengths": {},
        }, None

    valid_periods = [period for period in candidate_periods if period >= 2 and len(numeric) >= period * 2]
    decomposition = None
    method = "none"
    trend_strength = None
    seasonal_strength = None
    seasonal_strengths: dict[str, float] = {}

    try:
        if MSTL is not None and len(valid_periods) >= 2:
            decomposition = MSTL(numeric, periods=valid_periods[:3]).fit()
            method = "mstl"
            seasonal_component = decomposition.seasonal.sum(axis=1)
            remainder = decomposition.resid
            trend = decomposition.trend
            seasonal_strength = _strength_from_components(seasonal_component, remainder)
            trend_strength = _strength_from_components(trend, remainder)
            for index, period in enumerate(valid_periods[:3]):
                seasonal_strengths[str(period)] = _strength_from_components(
                    decomposition.seasonal.iloc[:, index],
                    remainder,
                )
        elif primary_period and len(numeric) >= primary_period * 2:
            decomposition = STL(numeric, period=primary_period, robust=True).fit()
            method = "stl"
            seasonal_strength = _strength_from_components(decomposition.seasonal, decomposition.resid)
            trend_strength = _strength_from_components(decomposition.trend, decomposition.resid)
            seasonal_strengths[str(primary_period)] = seasonal_strength
    except Exception:
        decomposition = None
        method = "failed"

    return {
        "decomposition_method": method,
        "trend_strength": trend_strength,
        "seasonal_strength": seasonal_strength,
        "seasonality_strengths": seasonal_strengths,
    }, decomposition


def _strength_from_components(component: Iterable[float], remainder: Iterable[float]) -> float | None:
    component_values = np.asarray(component, dtype=float)
    remainder_values = np.asarray(remainder, dtype=float)
    if component_values.size == 0 or remainder_values.size == 0:
        return None
    numerator = np.var(remainder_values)
    denominator = np.var(component_values + remainder_values)
    if denominator <= 0:
        return None
    return float(max(0.0, 1.0 - numerator / denominator))


def _rolling_outlier_summary(values: pd.Series, window: int, threshold: float) -> tuple[pd.DataFrame, dict[str, Any]]:
    numeric = pd.to_numeric(values, errors="coerce").astype(float)
    adaptive_window = max(int(window), 5)
    rolling_median = numeric.rolling(adaptive_window, center=True, min_periods=max(3, adaptive_window // 2)).median()
    rolling_median = rolling_median.bfill().ffill()
    residual = numeric - rolling_median
    mad = residual.abs().rolling(adaptive_window, center=True, min_periods=max(3, adaptive_window // 2)).median()
    mad = mad.replace(0, np.nan).bfill().ffill()
    robust_score = 0.6745 * residual / mad
    outlier_flag = robust_score.abs() > threshold

    detail = pd.DataFrame(
        {
            "rolling_median": rolling_median,
            "residual": residual,
            "robust_score": robust_score.fillna(0.0),
            "outlier_flag": outlier_flag.fillna(False),
        }
    )
    return detail, {
        "outlier_count": int(outlier_flag.fillna(False).sum()),
        "outlier_share": float(outlier_flag.fillna(False).mean()),
    }


def _detect_level_shifts(values: pd.Series, timestamps: pd.Series) -> list[dict[str, Any]]:
    try:
        import ruptures as rpt
    except ImportError:
        return []

    numeric = np.asarray(pd.to_numeric(values, errors="coerce").fillna(0.0), dtype=float)
    if len(numeric) < 40:
        return []

    signal = np.log1p(np.clip(numeric, 0, None)).reshape(-1, 1)
    try:
        algo = rpt.Pelt(model="rbf").fit(signal)
        penalty = max(np.log(len(signal)) * np.var(signal), 1.0)
        breakpoints = algo.predict(pen=penalty)
    except Exception:
        return []

    detected: list[dict[str, Any]] = []
    for index in breakpoints[:-1]:
        if 0 < index <= len(timestamps):
            detected.append(
                {
                    "label": "detected_level_shift",
                    "timestamp": pd.Timestamp(timestamps.iloc[index - 1]),
                }
            )
    return detected


def _baseline_error_summary(values: pd.Series, primary_period: int | None) -> dict[str, Any]:
    numeric = pd.to_numeric(values, errors="coerce").dropna().astype(float)
    naive_mae = None
    seasonal_naive_mae = None
    if len(numeric) >= 2:
        naive_mae = float((numeric.iloc[1:].to_numpy() - numeric.iloc[:-1].to_numpy()).astype(float).__abs__().mean())
    if primary_period and len(numeric) > primary_period:
        seasonal_naive_mae = float(
            (
                numeric.iloc[primary_period:].to_numpy() - numeric.iloc[:-primary_period].to_numpy()
            ).astype(float).__abs__().mean()
        )
    return {
        "naive_mae": naive_mae,
        "seasonal_naive_mae": seasonal_naive_mae,
    }


def _profile_tables(prepared: pd.DataFrame, frequency: str) -> dict[str, pd.DataFrame]:
    timestamp = prepared["timestamp"]
    profiles: dict[str, pd.DataFrame] = {}

    weekday_profile = (
        prepared.assign(weekday=timestamp.dt.day_name())
        .groupby("weekday", as_index=False)["value_filled"]
        .mean()
        .rename(columns={"value_filled": "average_value"})
    )
    weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    weekday_profile["weekday"] = pd.Categorical(weekday_profile["weekday"], categories=weekday_order, ordered=True)
    profiles["weekday_profile"] = weekday_profile.sort_values("weekday").reset_index(drop=True)

    monthly_profile = (
        prepared.assign(month=timestamp.dt.month_name())
        .groupby("month", as_index=False)["value_filled"]
        .mean()
        .rename(columns={"value_filled": "average_value"})
    )
    month_order = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]
    monthly_profile["month"] = pd.Categorical(monthly_profile["month"], categories=month_order, ordered=True)
    profiles["monthly_profile"] = monthly_profile.sort_values("month").reset_index(drop=True)

    if _base_frequency_label(frequency) == "hourly":
        intraday_profile = (
            prepared.assign(
                hour=timestamp.dt.hour,
                weekend=np.where(timestamp.dt.dayofweek >= 5, "weekend", "weekday"),
            )
            .groupby(["weekend", "hour"], as_index=False)["value_filled"]
            .mean()
            .rename(columns={"value_filled": "average_value"})
        )
        profiles["intraday_profile"] = intraday_profile

    return profiles


def _multiple_seasonality_flag(dominant_periods: list[dict[str, float]], candidate_periods: tuple[int, ...]) -> bool:
    if len(dominant_periods) < 2:
        return False
    periods = [item["period"] for item in dominant_periods]
    matched_candidates = 0
    for candidate in candidate_periods:
        if any(abs(period - candidate) <= max(1.0, candidate * 0.2) for period in periods):
            matched_candidates += 1
    return matched_candidates >= 2 or len(periods) >= 3


def _build_model_guidance(summary: dict[str, Any]) -> tuple[list[str], list[str], dict[str, float]]:
    scores = {
        "ETS / exponential smoothing": 0.0,
        "ARIMA / SARIMA": 0.0,
        "Fourier-based regression or dynamic harmonic regression": 0.0,
        "TBATS / multi-seasonal state space": 0.0,
        "ML lag-feature models": 0.0,
        "Probabilistic / count-aware methods": 0.0,
    }

    trend_strength = summary.get("trend_strength") or 0.0
    seasonal_strength = summary.get("seasonal_strength") or 0.0
    lag1 = summary.get("lag1_autocorrelation") or 0.0
    seasonal_lag = summary.get("seasonal_lag_autocorrelation") or 0.0
    level_shift_count = summary.get("level_shift_count") or 0
    multiple_seasonalities = bool(summary.get("multiple_seasonalities_detected"))
    is_count_like = bool(summary.get("is_count_like"))
    outlier_share = summary.get("outlier_share") or 0.0
    stationarity = summary.get("stationarity_assessment")
    zero_share = summary.get("zero_share") or 0.0
    seasonal_naive_mae = summary.get("seasonal_naive_mae")
    naive_mae = summary.get("naive_mae")

    if trend_strength >= 0.45 and seasonal_strength >= 0.35 and not multiple_seasonalities:
        scores["ETS / exponential smoothing"] += 3.0
    if lag1 >= 0.45 or stationarity in {"mixed_signal", "likely_stationary"}:
        scores["ARIMA / SARIMA"] += 2.0
    if seasonal_lag >= 0.35:
        scores["ARIMA / SARIMA"] += 1.5
    if multiple_seasonalities:
        scores["Fourier-based regression or dynamic harmonic regression"] += 3.0
        scores["TBATS / multi-seasonal state space"] += 3.0
    elif seasonal_strength >= 0.35:
        scores["Fourier-based regression or dynamic harmonic regression"] += 2.0
    if level_shift_count >= 1 or stationarity == "likely_nonstationary":
        scores["ML lag-feature models"] += 2.0
        scores["Fourier-based regression or dynamic harmonic regression"] += 1.0
    if is_count_like:
        scores["Probabilistic / count-aware methods"] += 2.0
    if zero_share >= 0.15:
        scores["Probabilistic / count-aware methods"] += 1.0
    if outlier_share >= 0.02:
        scores["ML lag-feature models"] += 1.0
        scores["Probabilistic / count-aware methods"] += 1.0
    if seasonal_naive_mae is not None and naive_mae is not None and seasonal_naive_mae < naive_mae:
        scores["ARIMA / SARIMA"] += 1.0
        scores["ETS / exponential smoothing"] += 1.0

    ranked_methods = [name for name, _score in sorted(scores.items(), key=lambda item: item[1], reverse=True) if _score > 0]

    recommendations: list[str] = []
    if multiple_seasonalities:
        recommendations.append(
            "Multiple recurring cycles are visible, so use Fourier terms or a multi-seasonal model instead of relying on a single seasonal lag."
        )
    if level_shift_count >= 1:
        recommendations.append(
            "Detected level shifts suggest adding intervention or regime features and preferring rolling retraining over one static fit."
        )
    if stationarity == "likely_nonstationary":
        recommendations.append(
            "The series looks nonstationary, so trend-aware or differenced models are safer than assuming a fixed mean level."
        )
    if seasonal_lag >= 0.35:
        recommendations.append(
            "Strong seasonal autocorrelation means seasonal-naive and seasonal-lag features should be part of the baseline stack."
        )
    if is_count_like:
        recommendations.append(
            "Because the target behaves like non-negative counts, probabilistic count models can give more defensible uncertainty than Gaussian assumptions."
        )
    if outlier_share >= 0.02:
        recommendations.append(
            "Frequent anomalies mean forecast evaluation should track interval coverage and consider robust loss or calibrated residual intervals."
        )
    if not recommendations:
        recommendations.append(
            "Start with a seasonal baseline and one interpretable model, then expand only if the backtests show a clear gain."
        )

    return ranked_methods, recommendations, scores


def _build_insights(summary: dict[str, Any]) -> list[str]:
    insights: list[str] = []
    trend_strength = summary.get("trend_strength")
    seasonal_strength = summary.get("seasonal_strength")
    lag1 = summary.get("lag1_autocorrelation")
    seasonal_lag = summary.get("seasonal_lag_autocorrelation")
    dominant_periods = summary.get("dominant_periods", [])
    level_shift_count = summary.get("level_shift_count", 0)
    stationarity = summary.get("stationarity_assessment")
    missing_periods = summary.get("missing_periods", 0)
    outlier_share = summary.get("outlier_share") or 0.0

    if trend_strength is not None:
        if trend_strength >= 0.6:
            insights.append("Trend strength is high, so the level is moving over time and a static-mean assumption would be too weak.")
        elif trend_strength >= 0.3:
            insights.append("Trend strength is moderate, which supports rolling retraining or explicit trend components.")
    if seasonal_strength is not None:
        if seasonal_strength >= 0.6:
            insights.append("Seasonality is strong and should be encoded explicitly with seasonal baselines, seasonal lags, or harmonic terms.")
        elif seasonal_strength >= 0.3:
            insights.append("Seasonality is present but not dominant, so season-aware features are still useful.")
    if lag1 is not None and lag1 >= 0.65:
        insights.append("Lag-1 autocorrelation is very high, which means recent history carries strong predictive signal.")
    if seasonal_lag is not None and seasonal_lag >= 0.35:
        insights.append("Seasonal-lag autocorrelation is meaningful, so a seasonal naive benchmark is a real bar to beat.")
    if dominant_periods:
        if isinstance(dominant_periods[0], dict):
            formatted = ", ".join(str(item["period"]) for item in dominant_periods[:3])
        else:
            formatted = ", ".join(str(item) for item in dominant_periods[:3])
        insights.append(f"Frequency-domain peaks point to repeating cycles near {formatted} periods.")
    if level_shift_count:
        insights.append("Detected level shifts imply regime-aware modeling and evaluation matter here.")
    if stationarity == "likely_nonstationary":
        insights.append("Stationarity tests suggest the series is not stable around one fixed mean, so differencing or trend/regime features are important.")
    elif stationarity == "mixed_signal":
        insights.append("Stationarity tests are mixed, which is a common sign of trend plus seasonality rather than a clean stationary process.")
    if missing_periods:
        insights.append("Missing timestamps were filled only for diagnostics, so forecasting features should keep completeness flags.")
    if outlier_share >= 0.02:
        insights.append("Outliers are frequent enough to distort both point forecasts and interval calibration if left untreated.")
    return insights


def _save_series_plot(prepared: pd.DataFrame, trend: pd.Series | None, config: TimeSeriesDiagnosticsConfig) -> None:
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(prepared["timestamp"], prepared["value_filled"], color="#1f77b4", linewidth=1.2, label="observed")
    if trend is not None:
        ax.plot(prepared["timestamp"], trend, color="#188054", linewidth=2.0, label="trend")
    for event in config.events:
        ax.axvline(event.timestamp, linestyle="--", color=event.color, alpha=0.75)
        ax.text(event.timestamp, ax.get_ylim()[1], event.label, rotation=90, va="top", ha="right", fontsize=8, color=event.color)
    ax.set_title("Time Series Overview")
    ax.set_ylabel("Value")
    ax.legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(config.output_dir / "series.png", dpi=150)
    plt.close(fig)


def _save_acf_pacf(values: pd.Series, config: TimeSeriesDiagnosticsConfig, primary_period: int | None) -> None:
    numeric = pd.to_numeric(values, errors="coerce").dropna().astype(float)
    max_lags = config.max_acf_lags or max(24, (primary_period or 12) * 2)
    max_lags = min(max_lags, max(1, len(numeric) // 2 - 1))
    if len(numeric) < 8 or max_lags < 1:
        for name in ("acf.png", "pacf.png"):
            fig, ax = plt.subplots(figsize=(8, 4))
            ax.text(0.5, 0.5, "Not enough history for this diagnostic.", ha="center", va="center")
            ax.axis("off")
            fig.tight_layout()
            fig.savefig(config.output_dir / name, dpi=150)
            plt.close(fig)
        return

    fig, ax = plt.subplots(figsize=(10, 4))
    plot_acf(numeric, lags=max_lags, ax=ax)
    ax.set_title("Autocorrelation")
    fig.tight_layout()
    fig.savefig(config.output_dir / "acf.png", dpi=150)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(10, 4))
    safe_pacf_lags = min(max_lags, max(1, len(numeric) // 2 - 1))
    plot_pacf(numeric, lags=safe_pacf_lags, ax=ax, method="ywm")
    ax.set_title("Partial Autocorrelation")
    fig.tight_layout()
    fig.savefig(config.output_dir / "pacf.png", dpi=150)
    plt.close(fig)


def _save_decomposition_plot(decomposition: Any, prepared: pd.DataFrame, config: TimeSeriesDiagnosticsConfig) -> None:
    if decomposition is None:
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.text(0.5, 0.5, "Not enough history for seasonal decomposition.", ha="center", va="center")
        ax.axis("off")
        fig.tight_layout()
        fig.savefig(config.output_dir / "stl.png", dpi=150)
        plt.close(fig)
        return

    try:
        if hasattr(decomposition, "plot"):
            fig = decomposition.plot()
            fig.set_size_inches(12, 8)
            fig.tight_layout()
            fig.savefig(config.output_dir / "stl.png", dpi=150)
            plt.close(fig)
            return
    except Exception:
        pass

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(prepared["timestamp"], prepared["value_filled"], label="series")
    ax.set_title("Seasonal decomposition unavailable")
    ax.legend()
    fig.tight_layout()
    fig.savefig(config.output_dir / "stl.png", dpi=150)
    plt.close(fig)


def _save_periodogram(values: pd.Series, config: TimeSeriesDiagnosticsConfig) -> None:
    numeric = pd.to_numeric(values, errors="coerce").dropna().astype(float).to_numpy()
    fig, ax = plt.subplots(figsize=(10, 4))
    if len(numeric) >= 8:
        frequencies, power = periodogram(numeric, detrend="linear", scaling="spectrum")
        ax.plot(frequencies, power, linewidth=1.2)
        ax.set_xlabel("Frequency")
        ax.set_ylabel("Power")
    else:
        ax.text(0.5, 0.5, "Not enough history for a periodogram.", ha="center", va="center")
        ax.axis("off")
    ax.set_title("Periodogram")
    fig.tight_layout()
    fig.savefig(config.output_dir / "periodogram.png", dpi=150)
    plt.close(fig)


def _save_distribution_plot(values: pd.Series, config: TimeSeriesDiagnosticsConfig) -> None:
    numeric = pd.to_numeric(values, errors="coerce").dropna().astype(float)
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    if not numeric.empty:
        axes[0].hist(numeric, bins=min(40, max(10, len(numeric) // 8)), color="#188054", alpha=0.8)
        axes[0].set_title("Distribution")
        axes[1].boxplot(numeric, vert=True)
        axes[1].set_title("Boxplot")
    else:
        for ax in axes:
            ax.text(0.5, 0.5, "No numeric values available.", ha="center", va="center")
            ax.axis("off")
    fig.tight_layout()
    fig.savefig(config.output_dir / "distribution.png", dpi=150)
    plt.close(fig)


def _save_rolling_stats_plot(prepared: pd.DataFrame, window: int, config: TimeSeriesDiagnosticsConfig) -> None:
    rolling_mean = prepared["value_filled"].rolling(window, min_periods=max(2, window // 2)).mean()
    rolling_std = prepared["value_filled"].rolling(window, min_periods=max(2, window // 2)).std()
    fig, axes = plt.subplots(2, 1, figsize=(12, 6), sharex=True)
    axes[0].plot(prepared["timestamp"], prepared["value_filled"], label="value", color="#1f77b4", linewidth=1.0)
    axes[0].plot(prepared["timestamp"], rolling_mean, label=f"{window}-period mean", color="#188054", linewidth=1.8)
    axes[0].legend(loc="upper left")
    axes[0].set_title("Rolling Mean")
    axes[1].plot(prepared["timestamp"], rolling_std, color="#ff7f0e", linewidth=1.5)
    axes[1].set_title("Rolling Standard Deviation")
    fig.tight_layout()
    fig.savefig(config.output_dir / "rolling_stats.png", dpi=150)
    plt.close(fig)


def _save_outlier_plot(prepared: pd.DataFrame, outlier_detail: pd.DataFrame, config: TimeSeriesDiagnosticsConfig) -> None:
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(prepared["timestamp"], prepared["value_filled"], color="#1f77b4", linewidth=1.1, label="value")
    flagged = outlier_detail["outlier_flag"].fillna(False).to_numpy()
    if flagged.any():
        ax.scatter(
            prepared.loc[flagged, "timestamp"],
            prepared.loc[flagged, "value_filled"],
            color="#d62728",
            s=18,
            label="potential anomaly",
            zorder=3,
        )
    ax.set_title("Outlier and anomaly candidates")
    ax.legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(config.output_dir / "outliers.png", dpi=150)
    plt.close(fig)


def _save_profile_plot(profiles: dict[str, pd.DataFrame], config: TimeSeriesDiagnosticsConfig) -> None:
    frames_to_plot = [name for name in ("weekday_profile", "intraday_profile", "monthly_profile") if name in profiles]
    if not frames_to_plot:
        return

    fig, axes = plt.subplots(len(frames_to_plot), 1, figsize=(12, 4 * len(frames_to_plot)))
    if len(frames_to_plot) == 1:
        axes = [axes]

    for axis, name in zip(axes, frames_to_plot):
        frame = profiles[name]
        if name == "weekday_profile":
            axis.plot(frame["weekday"].astype(str), frame["average_value"], marker="o")
            axis.set_title("Average by weekday")
        elif name == "intraday_profile":
            for weekend_label, subset in frame.groupby("weekend"):
                axis.plot(subset["hour"], subset["average_value"], marker="o", label=str(weekend_label))
            axis.legend(loc="upper left")
            axis.set_title("Average hourly profile")
        else:
            axis.plot(frame["month"].astype(str), frame["average_value"], marker="o")
            axis.set_title("Average by month")
        axis.tick_params(axis="x", rotation=30)

    fig.tight_layout()
    fig.savefig(config.output_dir / "seasonal_profile.png", dpi=150)
    plt.close(fig)


def _write_profiles(profiles: dict[str, pd.DataFrame], output_dir: Path) -> None:
    for name, frame in profiles.items():
        frame.to_csv(output_dir / f"{name}.csv", index=False)


def _write_summary_files(summary: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "diagnostics_summary.json").write_text(json.dumps(_serialize(summary), indent=2))

    flat_summary: dict[str, Any] = {}
    for key, value in summary.items():
        if isinstance(value, (list, dict)):
            flat_summary[key] = json.dumps(_serialize(value))
        else:
            flat_summary[key] = _serialize(value)
    pd.DataFrame([flat_summary]).to_csv(output_dir / "diagnostics_summary.csv", index=False)


def _write_markdown_report(summary: dict[str, Any], output_dir: Path) -> None:
    lines = [
        f"# Time Series Diagnostics: {summary['series_name']}",
        "",
        "## Main findings",
    ]
    for insight in summary.get("insights", [])[:6]:
        lines.append(f"- {insight}")

    lines.extend(
        [
            "",
            "## Forecasting guidance",
        ]
    )
    for item in summary.get("recommendations", []):
        lines.append(f"- {item}")

    lines.extend(
        [
            "",
            "## Recommended model families",
        ]
    )
    for method in summary.get("recommended_model_families", []):
        lines.append(f"- {method}")

    lines.extend(
        [
            "",
            "## Key statistics",
            f"- Frequency: {summary.get('frequency')}",
            f"- Time span: {summary.get('timestamp_start')} to {summary.get('timestamp_end')}",
            f"- Missing periods: {summary.get('missing_periods')}",
            f"- Duplicate timestamps: {summary.get('duplicate_timestamps')}",
            f"- Trend strength: {summary.get('trend_strength')}",
            f"- Seasonal strength: {summary.get('seasonal_strength')}",
            f"- Stationarity: {summary.get('stationarity_assessment')}",
            f"- Level shifts detected: {summary.get('level_shift_count')}",
            f"- Dominant periods: {summary.get('dominant_periods')}",
        ]
    )

    (output_dir / "diagnostics_report.md").write_text("\n".join(lines))


def run_time_series_diagnostics(frame: pd.DataFrame, config: TimeSeriesDiagnosticsConfig) -> dict[str, Any]:
    config = _config_with_defaults(config)
    config.output_dir.mkdir(parents=True, exist_ok=True)

    prepared, metadata = _prepare_series_frame(frame, config)
    primary_period = _choose_primary_period(config.candidate_periods, len(prepared), config.primary_period)
    rolling_window = config.rolling_window or max(primary_period or 7, 7)
    values = prepared["value_filled"]

    distribution = _distribution_summary(values)
    stationarity = _stationarity_summary(values)
    autocorrelation = _autocorrelation_summary(values, primary_period)
    dominant_periods = _dominant_periods(values)
    decomposition_summary, decomposition = _decomposition_summary(values, config.candidate_periods, primary_period)
    outlier_detail, outlier_summary = _rolling_outlier_summary(values, rolling_window, config.outlier_threshold)
    level_shifts = _detect_level_shifts(values, prepared["timestamp"])
    baseline_summary = _baseline_error_summary(values, primary_period)
    profiles = _profile_tables(prepared, config.frequency)
    multiple_seasonalities = _multiple_seasonality_flag(dominant_periods, config.candidate_periods)

    trend_component = None
    if decomposition is not None and hasattr(decomposition, "trend"):
        trend_component = pd.Series(np.asarray(decomposition.trend), index=prepared.index)

    summary: dict[str, Any] = {
        "series_name": config.series_name,
        "frequency": _base_frequency_label(config.frequency),
        "series_key": config.frequency,
        **metadata,
        **distribution,
        **stationarity,
        **autocorrelation,
        **decomposition_summary,
        **outlier_summary,
        **baseline_summary,
        "primary_period": primary_period,
        "candidate_periods": list(config.candidate_periods),
        "dominant_periods": [item["period"] for item in dominant_periods],
        "dominant_frequency_peaks": dominant_periods,
        "multiple_seasonalities_detected": multiple_seasonalities,
        "level_shift_count": len(level_shifts),
        "level_shifts": [
            {"label": item["label"], "timestamp": pd.Timestamp(item["timestamp"]).isoformat()}
            for item in level_shifts
        ],
        "event_markers": [
            {"label": event.label, "timestamp": event.timestamp.isoformat(), "color": event.color}
            for event in config.events
        ],
    }

    insights = _build_insights(summary)
    recommended_methods, recommendations, recommendation_scores = _build_model_guidance(summary)
    summary["insights"] = insights
    summary["recommendations"] = recommendations
    summary["recommended_model_families"] = recommended_methods
    summary["recommendation_scores"] = recommendation_scores

    _save_series_plot(prepared, trend_component, config)
    _save_acf_pacf(values, config, primary_period)
    _save_decomposition_plot(decomposition, prepared, config)
    _save_periodogram(values, config)
    _save_distribution_plot(values, config)
    _save_rolling_stats_plot(prepared, rolling_window, config)
    _save_outlier_plot(prepared, outlier_detail, config)
    _save_profile_plot(profiles, config)
    _write_profiles(profiles, config.output_dir)
    _write_summary_files(summary, config.output_dir)
    _write_markdown_report(summary, config.output_dir)

    return _serialize(summary)


def run_diagnostics(
    frame: pd.DataFrame,
    frequency: str,
    output_root: Path,
    regime_definition: RegimeDefinition | None = None,
) -> dict[str, Any]:
    events: list[DiagnosticEvent] = []
    if regime_definition is not None:
        events.extend(
            [
                DiagnosticEvent("shock", pd.Timestamp(regime_definition.pandemic_shock_start)),
                DiagnosticEvent("recovery", pd.Timestamp(regime_definition.recovery_start)),
                DiagnosticEvent("post", pd.Timestamp(regime_definition.post_pandemic_start)),
            ]
        )

    config = TimeSeriesDiagnosticsConfig(
        output_dir=Path(output_root) / frequency,
        series_name=frequency,
        frequency=_base_frequency_label(frequency),
        timestamp_col="bucket_start",
        value_col="trip_count",
        events=tuple(events),
    )
    summary = run_time_series_diagnostics(frame, config)
    summary["series_key"] = frequency
    return summary
