from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def find_latest_summary(report_dir: Path) -> Path | None:
    candidates = sorted(
        report_dir.glob("forecasting_*_summary.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def load_latest_summary(report_dir: Path) -> dict[str, Any]:
    summary_path = find_latest_summary(report_dir)
    if summary_path is None:
        return {}
    return json.loads(summary_path.read_text())


def find_latest_artifact(directory: Path, pattern: str) -> Path | None:
    candidates = sorted(
        directory.glob(pattern),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _resolve_artifact_path(summary: dict[str, Any], key: str, fallback_directory: Path, fallback_pattern: str) -> Path | None:
    artifact_paths = summary.get("artifact_paths", {}) if isinstance(summary, dict) else {}
    candidate = artifact_paths.get(key)
    if candidate:
        candidate_path = Path(candidate)
        if candidate_path.exists():
            return candidate_path
    return find_latest_artifact(fallback_directory, fallback_pattern)


def load_csv_artifact(
    summary: dict[str, Any],
    key: str,
    fallback_directory: Path,
    fallback_pattern: str,
    parse_dates: list[str] | None = None,
) -> pd.DataFrame:
    artifact_path = _resolve_artifact_path(summary, key, fallback_directory, fallback_pattern)
    if artifact_path is None or not artifact_path.exists():
        return pd.DataFrame()
    requested_parse_dates = parse_dates or []
    header = pd.read_csv(artifact_path, nrows=0, low_memory=False)
    available_parse_dates = [column for column in requested_parse_dates if column in header.columns]
    return pd.read_csv(artifact_path, parse_dates=available_parse_dates, low_memory=False)


def load_processed_aggregates(processed_dir: Path) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    for frequency in ("hourly", "daily", "weekly", "monthly", "quarterly"):
        candidate = processed_dir / f"{frequency}_aggregate.csv.gz"
        if candidate.exists():
            frames[frequency] = pd.read_csv(candidate, parse_dates=["bucket_start", "bucket_end"], low_memory=False)
    return frames


def load_station_coordinates(processed_dir: Path) -> pd.DataFrame:
    cleaned_path = processed_dir / "cleaned_trip_data.csv.gz"
    if not cleaned_path.exists():
        return pd.DataFrame(columns=["station_id", "latitude", "longitude"])

    try:
        frame = pd.read_csv(
            cleaned_path,
            usecols=["start_station", "start_lat", "start_lon"],
            low_memory=False,
        )
    except Exception:
        return pd.DataFrame(columns=["station_id", "latitude", "longitude"])
    frame = frame.dropna(subset=["start_station", "start_lat", "start_lon"]).copy()
    if frame.empty:
        return pd.DataFrame(columns=["station_id", "latitude", "longitude"])

    coordinates = (
        frame.groupby("start_station", as_index=False)
        .agg(
            latitude=("start_lat", "median"),
            longitude=("start_lon", "median"),
        )
        .rename(columns={"start_station": "station_id"})
    )
    coordinates["station_id"] = coordinates["station_id"].astype(str)
    return coordinates


def list_artifacts(directory: Path, patterns: tuple[str, ...]) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    seen: set[Path] = set()
    for pattern in patterns:
        for path in sorted(directory.rglob(pattern)):
            if path.is_dir() or path.name == ".gitkeep" or path in seen:
                continue
            seen.add(path)
            stats = path.stat()
            artifacts.append(
                {
                    "name": path.name,
                    "path": str(path),
                    "relative_path": str(path.relative_to(directory.parent)),
                    "size_mb": round(stats.st_size / (1024 * 1024), 3),
                    "modified_at": stats.st_mtime,
                }
            )
    return artifacts


def list_diagnostic_images(figures_dir: Path) -> dict[str, list[Path]]:
    image_map: dict[str, list[Path]] = {}
    if not figures_dir.exists():
        return image_map

    for frequency_dir in sorted(path for path in figures_dir.iterdir() if path.is_dir()):
        images = sorted(frequency_dir.glob("*.png"))
        if images:
            image_map[frequency_dir.name] = images
    return image_map


def build_dashboard_context(project_root: Path) -> dict[str, Any]:
    outputs_root = project_root / "outputs"
    reports_dir = outputs_root / "reports"
    figures_dir = outputs_root / "figures"
    forecasts_dir = outputs_root / "forecasts"
    processed_dir = project_root / "data" / "processed"

    summary_path = find_latest_summary(reports_dir)
    summary = load_latest_summary(reports_dir)

    return {
        "summary_path": str(summary_path) if summary_path else None,
        "summary": summary,
        "diagnostic_images": list_diagnostic_images(figures_dir),
        "processed_artifacts": list_artifacts(processed_dir, ("*.csv", "*.csv.gz", "*.json")),
        "report_artifacts": list_artifacts(reports_dir, ("*.json", "*.csv")),
        "forecast_artifacts": list_artifacts(forecasts_dir, ("*.json", "*.csv", "*.csv.gz")),
        "aggregate_frames": load_processed_aggregates(processed_dir),
        "station_coordinates": load_station_coordinates(processed_dir),
        "backtest_summary": load_csv_artifact(summary, "backtest_summary", reports_dir, "*_backtest_summary.csv"),
        "backtest_predictions": load_csv_artifact(
            summary,
            "backtest_predictions",
            reports_dir,
            "*_backtest_predictions.csv",
            parse_dates=["target_timestamp"],
        ),
        "backtest_folds": load_csv_artifact(
            summary,
            "backtest_folds",
            reports_dir,
            "*_backtest_folds.csv",
            parse_dates=["training_window_start", "training_window_end", "test_window_start", "test_window_end"],
        ),
        "split_metadata": load_csv_artifact(
            summary,
            "split_metadata",
            reports_dir,
            "*_split_metadata.csv",
            parse_dates=["window_start", "window_end"],
        ),
        "validation_summary": load_csv_artifact(summary, "validation_summary", reports_dir, "*_validation_summary.csv"),
        "test_summary": load_csv_artifact(summary, "test_summary", reports_dir, "*_test_summary.csv"),
        "evaluation_predictions": load_csv_artifact(
            summary,
            "evaluation_predictions",
            reports_dir,
            "*_evaluation_predictions.csv",
            parse_dates=["target_timestamp", "training_window_start", "training_window_end", "holdout_window_start", "holdout_window_end"],
        ),
        "evaluation_metrics": load_csv_artifact(
            summary,
            "evaluation_metrics",
            reports_dir,
            "*_evaluation_metrics.csv",
            parse_dates=["training_window_start", "training_window_end", "holdout_window_start", "holdout_window_end"],
        ),
        "forecast_outputs": load_csv_artifact(
            summary,
            "forecast_outputs",
            forecasts_dir,
            "*_forecast_outputs.csv",
            parse_dates=[
                "target_timestamp",
                "generated_at",
                "training_window_start",
                "training_window_end",
                "selection_train_window_start",
                "selection_train_window_end",
                "validation_window_start",
                "validation_window_end",
                "test_window_start",
                "test_window_end",
            ],
        ),
        "forecast_intervals": load_csv_artifact(summary, "forecast_intervals", forecasts_dir, "*_forecast_intervals.csv"),
        "model_registry": load_csv_artifact(
            summary,
            "model_registry",
            reports_dir,
            "*_model_registry.csv",
            parse_dates=[
                "training_window_start",
                "training_window_end",
                "selection_train_window_start",
                "selection_train_window_end",
                "validation_window_start",
                "validation_window_end",
                "test_window_start",
                "test_window_end",
                "trained_at",
            ],
        ),
        "station_profiles": load_csv_artifact(summary, "station_profiles", reports_dir, "*_station_profiles.csv"),
        "segment_evaluation_summary": load_csv_artifact(
            summary,
            "segment_evaluation_summary",
            reports_dir,
            "*_segment_evaluation_summary.csv",
        ),
        "station_tier_evaluation_summary": load_csv_artifact(
            summary,
            "station_tier_evaluation_summary",
            reports_dir,
            "*_station_tier_evaluation_summary.csv",
        ),
        "horizon_evaluation_summary": load_csv_artifact(
            summary,
            "horizon_evaluation_summary",
            reports_dir,
            "*_horizon_evaluation_summary.csv",
        ),
        "regime_evaluation_summary": load_csv_artifact(
            summary,
            "regime_evaluation_summary",
            reports_dir,
            "*_regime_evaluation_summary.csv",
        ),
        "reconciliation_outputs": load_csv_artifact(
            summary,
            "reconciliation_outputs",
            reports_dir,
            "*_reconciliation_outputs.csv",
            parse_dates=["target_timestamp"],
        ),
    }
