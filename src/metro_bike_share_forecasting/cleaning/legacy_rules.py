from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable

import pandas as pd


LEGACY_RULE_SOURCES = {
    "foundation_sql": "sql/legacy/foundation/metro_bike_share.sql",
    "staging_sql_pattern": "sql/legacy/staging/metro_bike_share-*.sql",
    "feature_sql_pattern": "sql/legacy/features/Processing-*.sql",
}


TEXT_PATTERN = re.compile(r"([a-z]+[\s+\-]?[a-z]*[\s+]?[a-z]*)", flags=re.IGNORECASE)


@dataclass
class LegacyReuseSummary:
    source_files: Dict[str, str]
    reused_rules: list[str]
    notes: list[str]

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_files": self.source_files,
            "reused_rules": self.reused_rules,
            "notes": self.notes,
        }

    def to_json(self) -> str:
        return json.dumps(self.as_dict(), indent=2)


def describe_legacy_reuse() -> LegacyReuseSummary:
    return LegacyReuseSummary(
        source_files=LEGACY_RULE_SOURCES,
        reused_rules=[
            "trip_id integer validation adapted from check_id",
            "safe datetime parsing adapted from Check_date / is_date",
            "duration normalization adapted from check_duration",
            "station and bike identifier validation adapted from check_station / check_b_ids",
            "latitude/longitude validation adapted from check_lat_lon",
            "plan duration filtering adapted from check_plan_duration",
            "passholder and route text normalization adapted from check_text",
            "invalid testing rows and virtual-station geolocation filtering adapted from staging SQL",
        ],
        notes=[
            "The Python cleaner mirrors the intent of the SQL rules while avoiding the original sentinel-value pattern when a safer drop-or-flag rule is better.",
            "The legacy SQL remains the source of truth for the original cleaning heuristics and is referenced explicitly in the README and pipeline summary report.",
        ],
    )


def normalize_column_name(column_name: str) -> str:
    cleaned = re.sub(r"[^0-9a-zA-Z]+", "_", column_name.strip().lower())
    return cleaned.strip("_")


def normalize_text_value(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.lower() == "null":
        return None
    if not TEXT_PATTERN.search(text):
        return None
    if "-" in text:
        left, _, right = text.partition("-")
        return f"{left.strip().title()}-{right.strip().lower()}"
    return text.title()


def parse_datetime_value(value: Any) -> pd.Timestamp | pd.NaT:
    if value is None or pd.isna(value):
        return pd.NaT
    text = str(value).strip()
    if not text:
        return pd.NaT

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %H:%M:%S %p",
        "%m/%d/%Y %H:%M %p",
    ):
        parsed = pd.to_datetime(text, format=fmt, errors="coerce")
        if not pd.isna(parsed):
            return parsed
    return pd.to_datetime(text, errors="coerce")


def parse_datetime_series(series: pd.Series) -> pd.Series:
    text = series.astype("string").str.strip()
    parsed = pd.to_datetime(text, errors="coerce")

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %H:%M:%S %p",
        "%m/%d/%Y %H:%M %p",
    ):
        mask = parsed.isna() & text.notna()
        if not mask.any():
            break
        parsed.loc[mask] = pd.to_datetime(text.loc[mask], format=fmt, errors="coerce")

    return parsed


def numeric_cast(series: pd.Series, target: str = "float") -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if target == "int":
        return numeric.astype("Int64")
    return numeric.astype("Float64")


def build_missingness_summary(frame: pd.DataFrame, columns: Iterable[str]) -> dict[str, float]:
    summary: dict[str, float] = {}
    row_count = max(len(frame), 1)
    for column in columns:
        summary[column] = round(frame[column].isna().sum() / row_count, 4)
    return summary
