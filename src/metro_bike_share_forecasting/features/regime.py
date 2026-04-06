from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd


@dataclass
class RegimeDefinition:
    pandemic_shock_start: pd.Timestamp
    recovery_start: pd.Timestamp
    post_pandemic_start: pd.Timestamp
    detected_breakpoints: list[str]
    detection_method: str

    def as_dict(self) -> dict[str, object]:
        return {
            "pandemic_shock_start": self.pandemic_shock_start.isoformat(),
            "recovery_start": self.recovery_start.isoformat(),
            "post_pandemic_start": self.post_pandemic_start.isoformat(),
            "detected_breakpoints": self.detected_breakpoints,
            "detection_method": self.detection_method,
        }


def _detect_breakpoints(values: np.ndarray, timestamps: Iterable[pd.Timestamp]) -> list[pd.Timestamp]:
    try:
        import ruptures as rpt
    except ImportError:
        return []

    if len(values) < 40:
        return []

    signal = np.log1p(values).reshape(-1, 1)
    algo = rpt.Pelt(model="rbf").fit(signal)
    penalty = max(np.log(len(values)) * np.var(signal), 1.0)
    breakpoints = algo.predict(pen=penalty)
    candidates = list(timestamps)
    return [candidates[index - 1] for index in breakpoints[:-1] if 0 < index <= len(candidates)]


def derive_regime_definition(daily_system_series: pd.DataFrame, settings) -> RegimeDefinition:
    known_shock = pd.Timestamp(settings.pandemic_shock_start)
    known_recovery = pd.Timestamp(settings.recovery_start)
    known_post = pd.Timestamp(settings.post_pandemic_start)

    series = daily_system_series.sort_values("bucket_start").reset_index(drop=True)
    detected = _detect_breakpoints(series["trip_count"].to_numpy(dtype=float), series["bucket_start"])
    detection_method = "known_dates_only"

    def choose_break(known_date: pd.Timestamp) -> pd.Timestamp:
        nonlocal detection_method
        nearby = [
            candidate
            for candidate in detected
            if abs((candidate - known_date).days) <= 120
        ]
        if nearby:
            detection_method = "known_dates_with_changepoint_refinement"
            return min(nearby, key=lambda candidate: abs((candidate - known_date).days))
        return known_date

    return RegimeDefinition(
        pandemic_shock_start=choose_break(known_shock),
        recovery_start=choose_break(known_recovery),
        post_pandemic_start=choose_break(known_post),
        detected_breakpoints=[timestamp.isoformat() for timestamp in detected],
        detection_method=detection_method,
    )


def add_regime_features(frame: pd.DataFrame, timestamp_col: str, regime_definition: RegimeDefinition) -> pd.DataFrame:
    enriched = frame.copy()
    timestamp = pd.to_datetime(enriched[timestamp_col])

    conditions = [
        timestamp < regime_definition.pandemic_shock_start,
        (timestamp >= regime_definition.pandemic_shock_start) & (timestamp < regime_definition.recovery_start),
        (timestamp >= regime_definition.recovery_start) & (timestamp < regime_definition.post_pandemic_start),
        timestamp >= regime_definition.post_pandemic_start,
    ]
    labels = ["pre_pandemic", "pandemic_shock", "recovery", "post_pandemic"]
    enriched["pandemic_phase"] = np.select(conditions, labels, default="unknown")
    enriched["is_lockdown"] = enriched["pandemic_phase"].eq("pandemic_shock").astype(int)
    enriched["is_reopening"] = enriched["pandemic_phase"].eq("recovery").astype(int)
    enriched["is_post_pandemic"] = enriched["pandemic_phase"].eq("post_pandemic").astype(int)
    enriched["days_since_lockdown_start"] = (
        timestamp - regime_definition.pandemic_shock_start
    ).dt.days.clip(lower=0)
    enriched["days_since_reopening_start"] = (
        timestamp - regime_definition.recovery_start
    ).dt.days.clip(lower=0)
    return enriched
