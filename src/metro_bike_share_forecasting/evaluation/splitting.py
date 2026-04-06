from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class TemporalSplit:
    frequency: str
    train_frame: pd.DataFrame
    validation_frame: pd.DataFrame
    test_frame: pd.DataFrame

    @property
    def train_plus_validation_frame(self) -> pd.DataFrame:
        return pd.concat([self.train_frame, self.validation_frame], ignore_index=True)

    @property
    def development_frame(self) -> pd.DataFrame:
        return self.train_frame


def build_temporal_split(
    frame: pd.DataFrame,
    frequency: str,
    validation_window: int,
    test_window: int,
    minimum_train_window: int,
) -> TemporalSplit:
    ordered = frame.sort_values("bucket_start").reset_index(drop=True)
    required_rows = minimum_train_window + validation_window + test_window
    if len(ordered) < required_rows:
        raise ValueError(
            f"Not enough rows for {frequency} split: need at least {required_rows}, found {len(ordered)}."
        )

    test_start = len(ordered) - test_window
    validation_start = test_start - validation_window

    train = ordered.iloc[:validation_start].copy()
    validation = ordered.iloc[validation_start:test_start].copy()
    test = ordered.iloc[test_start:].copy()

    return TemporalSplit(
        frequency=frequency,
        train_frame=train,
        validation_frame=validation,
        test_frame=test,
    )


def describe_temporal_split(
    split: TemporalSplit,
    segment_type: str,
    segment_id: str,
) -> pd.DataFrame:
    rows = []
    for window_role, subset in (
        ("train", split.train_frame),
        ("validation", split.validation_frame),
        ("test", split.test_frame),
    ):
        rows.append(
            {
                "frequency": split.frequency,
                "segment_type": segment_type,
                "segment_id": segment_id,
                "window_role": window_role,
                "window_start": subset["bucket_start"].min(),
                "window_end": subset["bucket_start"].max(),
                "row_count": len(subset),
            }
        )
    return pd.DataFrame(rows)
