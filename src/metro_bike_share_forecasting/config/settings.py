from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Tuple

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - optional until dependencies are installed
    def load_dotenv(*_args, **_kwargs) -> bool:
        return False


PROJECT_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(PROJECT_ROOT / ".env", override=False)


def _split_csv_env(name: str, default: str) -> Tuple[str, ...]:
    raw_value = os.getenv(name, default)
    return tuple(value.strip() for value in raw_value.split(",") if value.strip())


@dataclass
class Settings:
    project_root: Path = PROJECT_ROOT
    raw_trip_dir: Path = Path(os.getenv("RAW_TRIP_DIR", str(PROJECT_ROOT / "data" / "raw" / "trips")))
    raw_station_dir: Path = Path(os.getenv("RAW_STATION_DIR", str(PROJECT_ROOT / "data" / "raw" / "stations")))
    interim_dir: Path = Path(os.getenv("INTERIM_DIR", str(PROJECT_ROOT / "data" / "interim")))
    processed_dir: Path = Path(os.getenv("PROCESSED_DIR", str(PROJECT_ROOT / "data" / "processed")))
    outputs_reports_dir: Path = Path(os.getenv("OUTPUTS_REPORTS_DIR", str(PROJECT_ROOT / "outputs" / "reports")))
    outputs_figures_dir: Path = Path(os.getenv("OUTPUTS_FIGURES_DIR", str(PROJECT_ROOT / "outputs" / "figures")))
    outputs_forecasts_dir: Path = Path(os.getenv("OUTPUTS_FORECASTS_DIR", str(PROJECT_ROOT / "outputs" / "forecasts")))
    sql_schema_path: Path = Path(
        os.getenv("SQL_SCHEMA_PATH", str(PROJECT_ROOT / "sql" / "forecasting" / "001_create_forecasting_schema.sql"))
    )
    postgres_url: str | None = os.getenv("POSTGRES_URL")
    postgres_schema: str = os.getenv("POSTGRES_SCHEMA", "forecasting")
    raw_timezone: str = os.getenv("RAW_TIMEZONE", "America/Los_Angeles")
    logging_level: str = os.getenv("LOG_LEVEL", "INFO")
    holiday_country: str = os.getenv("HOLIDAY_COUNTRY", "US")
    frequencies: Tuple[str, ...] = field(
        default_factory=lambda: _split_csv_env(
            "FREQUENCIES",
            "hourly,daily,weekly,monthly,quarterly",
        )
    )
    enabled_models: Tuple[str, ...] = field(
        default_factory=lambda: _split_csv_env(
            "ENABLED_MODELS",
            "naive,seasonal_naive,rolling_mean,count_glm,sarimax_fourier,weighted_ensemble",
        )
    )
    station_enabled_models: Tuple[str, ...] = field(
        default_factory=lambda: _split_csv_env(
            "STATION_ENABLED_MODELS",
            "naive,seasonal_naive,rolling_mean,count_glm,weighted_ensemble",
        )
    )
    horizon_map: Dict[str, int] = field(
        default_factory=lambda: {
            "hourly": int(os.getenv("HORIZON_HOURLY", "168")),
            "daily": int(os.getenv("HORIZON_DAILY", "28")),
            "weekly": int(os.getenv("HORIZON_WEEKLY", "12")),
            "monthly": int(os.getenv("HORIZON_MONTHLY", "12")),
            "quarterly": int(os.getenv("HORIZON_QUARTERLY", "6")),
        }
    )
    initial_window_map: Dict[str, int] = field(
        default_factory=lambda: {
            "hourly": int(os.getenv("INITIAL_WINDOW_HOURLY", str(24 * 90))),
            "daily": int(os.getenv("INITIAL_WINDOW_DAILY", "365")),
            "weekly": int(os.getenv("INITIAL_WINDOW_WEEKLY", "104")),
            "monthly": int(os.getenv("INITIAL_WINDOW_MONTHLY", "36")),
            "quarterly": int(os.getenv("INITIAL_WINDOW_QUARTERLY", "16")),
        }
    )
    validation_window_map: Dict[str, int] = field(
        default_factory=lambda: {
            "hourly": int(os.getenv("VALIDATION_WINDOW_HOURLY", str(24 * 28))),
            "daily": int(os.getenv("VALIDATION_WINDOW_DAILY", "56")),
            "weekly": int(os.getenv("VALIDATION_WINDOW_WEEKLY", "12")),
            "monthly": int(os.getenv("VALIDATION_WINDOW_MONTHLY", "6")),
            "quarterly": int(os.getenv("VALIDATION_WINDOW_QUARTERLY", "4")),
        }
    )
    test_window_map: Dict[str, int] = field(
        default_factory=lambda: {
            "hourly": int(os.getenv("TEST_WINDOW_HOURLY", str(24 * 28))),
            "daily": int(os.getenv("TEST_WINDOW_DAILY", "56")),
            "weekly": int(os.getenv("TEST_WINDOW_WEEKLY", "12")),
            "monthly": int(os.getenv("TEST_WINDOW_MONTHLY", "6")),
            "quarterly": int(os.getenv("TEST_WINDOW_QUARTERLY", "4")),
        }
    )
    step_map: Dict[str, int] = field(
        default_factory=lambda: {
            "hourly": int(os.getenv("BACKTEST_STEP_HOURLY", "24")),
            "daily": int(os.getenv("BACKTEST_STEP_DAILY", "7")),
            "weekly": int(os.getenv("BACKTEST_STEP_WEEKLY", "1")),
            "monthly": int(os.getenv("BACKTEST_STEP_MONTHLY", "1")),
            "quarterly": int(os.getenv("BACKTEST_STEP_QUARTERLY", "1")),
        }
    )
    max_backtest_folds: int = int(os.getenv("MAX_BACKTEST_FOLDS", "8"))
    pandemic_shock_start: str = os.getenv("PANDEMIC_SHOCK_START", "2020-03-15")
    recovery_start: str = os.getenv("RECOVERY_START", "2021-06-15")
    post_pandemic_start: str = os.getenv("POST_PANDEMIC_START", "2022-07-01")
    retrain_segment_type: str = os.getenv("RETRAIN_SEGMENT_TYPE", "system_total")
    retrain_segment_id: str = os.getenv("RETRAIN_SEGMENT_ID", "all")
    station_level_top_n: int = int(os.getenv("STATION_LEVEL_TOP_N", "20"))
    station_level_frequencies: Tuple[str, ...] = field(
        default_factory=lambda: _split_csv_env("STATION_LEVEL_FREQUENCIES", "hourly,daily")
    )

    def ensure_runtime_directories(self) -> None:
        for path in (
            self.interim_dir,
            self.processed_dir,
            self.outputs_reports_dir,
            self.outputs_figures_dir,
            self.outputs_forecasts_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

    def horizon_for(self, frequency: str) -> int:
        return self.horizon_map[frequency]

    def initial_window_for(self, frequency: str) -> int:
        return self.initial_window_map[frequency]

    def step_for(self, frequency: str) -> int:
        return self.step_map[frequency]

    def validation_window_for(self, frequency: str) -> int:
        return self.validation_window_map[frequency]

    def test_window_for(self, frequency: str) -> int:
        return self.test_window_map[frequency]


def get_settings() -> Settings:
    settings = Settings()
    settings.ensure_runtime_directories()
    return settings
