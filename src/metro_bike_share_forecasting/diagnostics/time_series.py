from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "metro_bike_share_matplotlib"))

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.signal import periodogram
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.seasonal import STL
from statsmodels.tsa.stattools import adfuller

from metro_bike_share_forecasting.features.regime import RegimeDefinition


SEASONAL_LAG_MAP = {
    "hourly": 24,
    "daily": 7,
    "weekly": 52,
    "monthly": 12,
    "quarterly": 4,
}


def _dominant_periods(series: pd.Series) -> list[float]:
    if len(series) < 8:
        return []
    frequencies, power = periodogram(series.to_numpy(dtype=float))
    candidate_rows: list[tuple[float, float]] = []
    for frequency, score in zip(frequencies[1:], power[1:]):
        if frequency <= 0:
            continue
        period = 1 / frequency
        if np.isfinite(period) and period <= len(series):
            candidate_rows.append((period, score))
    ranked = sorted(candidate_rows, key=lambda item: item[1], reverse=True)
    periods: list[float] = []
    for period, _ in ranked:
        rounded = round(period, 2)
        if rounded not in periods:
            periods.append(rounded)
        if len(periods) == 3:
            break
    return periods


def _build_diagnostic_insights(
    frequency: str,
    missing_periods: int,
    zero_share: float,
    adf_pvalue: float | None,
    lag1_autocorrelation: float | None,
    seasonal_lag_autocorrelation: float | None,
    dominant_periods: list[float],
    regime_definition: RegimeDefinition,
) -> list[str]:
    insights: list[str] = []
    if missing_periods == 0:
        insights.append("The series is fully continuous for the observed window, so model comparisons are not being distorted by missing periods.")
    else:
        insights.append(f"The series has {missing_periods} missing periods, so completeness flags should remain active in downstream modeling.")

    if lag1_autocorrelation is not None and lag1_autocorrelation > 0.7:
        insights.append("Lag-1 autocorrelation is high, which means recent demand levels strongly influence the next period.")
    elif lag1_autocorrelation is not None:
        insights.append("Lag-1 autocorrelation is moderate, so short-memory models still matter but persistence is less dominant.")

    if seasonal_lag_autocorrelation is not None and seasonal_lag_autocorrelation > 0.4:
        insights.append(
            f"The seasonal lag autocorrelation around {SEASONAL_LAG_MAP[frequency]} periods is meaningful, supporting recurring calendar structure."
        )

    if dominant_periods:
        insights.append(
            "The strongest spectral periods are approximately "
            + ", ".join(str(period) for period in dominant_periods)
            + " periods, which supports using seasonal terms rather than a trend-only model."
        )

    if zero_share > 0.1:
        insights.append("Zero-demand periods are non-trivial, so count-aware models and interval calibration are important.")
    else:
        insights.append("Zero-demand periods are limited, so the main challenge is pattern shift and seasonality rather than zero inflation.")

    if adf_pvalue is not None:
        if adf_pvalue < 0.05:
            insights.append("The ADF test suggests the series is closer to stationary after accounting for recurring structure.")
        else:
            insights.append("The ADF test does not support strong stationarity, so trend and regime terms remain important.")

    insights.append(
        "Pandemic-aware breakpoints are anchored at "
        f"{regime_definition.pandemic_shock_start.date()}, {regime_definition.recovery_start.date()}, and {regime_definition.post_pandemic_start.date()}."
    )
    return insights


def _save_series_plot(frame: pd.DataFrame, output_path: Path, regime_definition: RegimeDefinition) -> None:
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(frame["bucket_start"], frame["trip_count"], linewidth=1.2, label="trip_count")
    for boundary, label in (
        (regime_definition.pandemic_shock_start, "shock"),
        (regime_definition.recovery_start, "recovery"),
        (regime_definition.post_pandemic_start, "post"),
    ):
        ax.axvline(boundary, color="tab:red", linestyle="--", alpha=0.6, label=label)
    ax.set_title("Demand Over Time")
    ax.set_ylabel("Trips")
    ax.legend(loc="upper right")
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def _save_acf_pacf_plots(series: pd.Series, output_dir: Path) -> None:
    lags = min(48, max(len(series) // 4, 1))
    fig_acf = plot_acf(series, lags=lags)
    fig_acf.tight_layout()
    fig_acf.savefig(output_dir / "acf.png", dpi=150)
    plt.close(fig_acf)

    fig_pacf = plot_pacf(series, lags=min(lags, 24), method="ywm")
    fig_pacf.tight_layout()
    fig_pacf.savefig(output_dir / "pacf.png", dpi=150)
    plt.close(fig_pacf)


def _save_stl_plot(series: pd.Series, output_path: Path, period: int) -> None:
    if len(series) <= period * 2:
        return
    result = STL(series, period=period, robust=True).fit()
    fig = result.plot()
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def _save_periodogram(series: pd.Series, output_path: Path) -> None:
    frequencies, power = periodogram(series.to_numpy(dtype=float))
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(frequencies[1:], power[1:])
    ax.set_title("Periodogram")
    ax.set_xlabel("Frequency")
    ax.set_ylabel("Power")
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def _save_profiles(frame: pd.DataFrame, frequency: str, output_dir: Path) -> None:
    enriched = frame.copy()
    enriched["day_of_week"] = pd.to_datetime(enriched["bucket_start"]).dt.day_name()
    weekday_profile = enriched.groupby("day_of_week", as_index=False)["trip_count"].mean()
    weekday_profile.to_csv(output_dir / "weekday_profile.csv", index=False)

    if frequency == "hourly":
        enriched["hour_of_day"] = pd.to_datetime(enriched["bucket_start"]).dt.hour
        hourly_profile = enriched.groupby("hour_of_day", as_index=False)["trip_count"].mean()
        hourly_profile.to_csv(output_dir / "intraday_profile.csv", index=False)

    enriched["month"] = pd.to_datetime(enriched["bucket_start"]).dt.month
    monthly_profile = enriched.groupby("month", as_index=False)["trip_count"].mean()
    monthly_profile.to_csv(output_dir / "monthly_profile.csv", index=False)


def run_diagnostics(
    frame: pd.DataFrame,
    frequency: str,
    output_root: Path,
    regime_definition: RegimeDefinition,
) -> dict[str, Any]:
    output_dir = output_root / frequency
    output_dir.mkdir(parents=True, exist_ok=True)

    series = frame.sort_values("bucket_start")["trip_count"].astype(float)
    timestamped = frame.sort_values("bucket_start").reset_index(drop=True)
    missing_periods = int(timestamped["missing_period_flag"].sum()) if "missing_period_flag" in timestamped else 0
    zero_share = float((series == 0).mean())
    rolling_mean = series.rolling(window=min(14, max(len(series) // 10, 2)), min_periods=1).mean()
    rolling_std = series.rolling(window=min(14, max(len(series) // 10, 2)), min_periods=1).std().fillna(0.0)
    outlier_threshold = rolling_mean + (3 * rolling_std)
    outlier_count = int((series > outlier_threshold).sum())

    adf_pvalue = None
    if len(series) > 20:
        try:
            adf_pvalue = float(adfuller(series)[1])
        except ValueError:
            adf_pvalue = None
    lag1_autocorrelation = float(series.autocorr(lag=1)) if len(series) > 2 else None
    seasonal_lag = SEASONAL_LAG_MAP.get(frequency)
    seasonal_lag_autocorrelation = (
        float(series.autocorr(lag=seasonal_lag))
        if seasonal_lag is not None and len(series) > seasonal_lag + 1
        else None
    )
    dominant_periods = _dominant_periods(series)
    insights = _build_diagnostic_insights(
        frequency=frequency,
        missing_periods=missing_periods,
        zero_share=zero_share,
        adf_pvalue=adf_pvalue,
        lag1_autocorrelation=lag1_autocorrelation,
        seasonal_lag_autocorrelation=seasonal_lag_autocorrelation,
        dominant_periods=dominant_periods,
        regime_definition=regime_definition,
    )

    summary = {
        "frequency": frequency,
        "row_count": int(len(timestamped)),
        "min_timestamp": str(timestamped["bucket_start"].min()) if not timestamped.empty else None,
        "max_timestamp": str(timestamped["bucket_start"].max()) if not timestamped.empty else None,
        "missing_periods": missing_periods,
        "zero_share": round(zero_share, 4),
        "outlier_count": outlier_count,
        "mean_trip_count": round(float(series.mean()), 4) if not series.empty else None,
        "std_trip_count": round(float(series.std()), 4) if len(series) > 1 else None,
        "adf_pvalue": adf_pvalue,
        "lag1_autocorrelation": lag1_autocorrelation,
        "seasonal_lag_autocorrelation": seasonal_lag_autocorrelation,
        "dominant_periods": dominant_periods,
        "insights": insights,
        "regime_definition": regime_definition.as_dict(),
    }

    _save_series_plot(timestamped, output_dir / "series.png", regime_definition)
    _save_acf_pacf_plots(series, output_dir)
    _save_stl_plot(series, output_dir / "stl.png", period=max(2, min(24, len(series) // 5)))
    _save_periodogram(series, output_dir / "periodogram.png")
    _save_profiles(timestamped, frequency, output_dir)

    (output_dir / "diagnostics_summary.json").write_text(json.dumps(summary, indent=2))
    pd.DataFrame([summary]).to_csv(output_dir / "diagnostics_summary.csv", index=False)
    return summary
