from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List

import pandas as pd


NULL_TOKENS = {"", "null", "NULL", "N/A", "n/a", "nan", "NaN"}


@dataclass
class SourceFileProfile:
    source_file: str
    row_count_raw: int
    column_names: list[str]
    schema_drift_detected: bool


@dataclass
class CSVLoadBundle:
    data: pd.DataFrame
    profiles: list[SourceFileProfile]


def discover_csv_files(directory: Path) -> list[Path]:
    return sorted(path for path in directory.glob("*.csv") if path.is_file())


def _clean_raw_frame(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.rename(columns=lambda value: str(value).strip())
    return frame.replace({token: pd.NA for token in NULL_TOKENS})


def load_trip_csvs(paths: Iterable[Path]) -> CSVLoadBundle:
    paths = list(paths)
    data_frames: List[pd.DataFrame] = []
    profiles: list[SourceFileProfile] = []
    baseline_columns: list[str] | None = None

    for path in paths:
        frame = pd.read_csv(path, dtype=str, keep_default_na=False)
        frame = _clean_raw_frame(frame)
        frame["source_file"] = path.name
        frame["source_row_number"] = range(2, len(frame) + 2)
        frame["source_file_path"] = str(path)

        column_names = [column for column in frame.columns if column not in {"source_file", "source_row_number", "source_file_path"}]
        if baseline_columns is None:
            baseline_columns = column_names
        schema_drift_detected = column_names != baseline_columns

        profiles.append(
            SourceFileProfile(
                source_file=path.name,
                row_count_raw=len(frame),
                column_names=column_names,
                schema_drift_detected=schema_drift_detected,
            )
        )
        data_frames.append(frame)

    if not data_frames:
        return CSVLoadBundle(data=pd.DataFrame(), profiles=profiles)

    combined = pd.concat(data_frames, ignore_index=True, sort=False)
    return CSVLoadBundle(data=combined, profiles=profiles)
