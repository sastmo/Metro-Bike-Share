from __future__ import annotations

import argparse
import os
import platform
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
VENV_DIR = PROJECT_ROOT / ".venv"


def _venv_python() -> Path:
    if platform.system() == "Windows":
        return VENV_DIR / "Scripts" / "python.exe"
    return VENV_DIR / "bin" / "python"


def _activation_hint() -> str:
    if platform.system() == "Windows":
        return r".venv\Scripts\activate"
    return "source .venv/bin/activate"


def _run_command(command: list[str], extra_env: dict[str, str] | None = None) -> None:
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    subprocess.run(command, cwd=PROJECT_ROOT, env=env, check=True)


def print_status() -> None:
    print("Metro Bike Share bootstrap status")
    print(f"- project_root: {PROJECT_ROOT}")
    print(f"- platform: {platform.system()} {platform.release()}")
    print(f"- current_python: {sys.executable}")
    print(f"- python_version: {platform.python_version()}")
    print(f"- virtualenv_exists: {VENV_DIR.exists()}")
    print(f"- activation_hint: {_activation_hint()}")


def prepare_environment() -> None:
    if not VENV_DIR.exists():
        print("Creating .venv ...")
        _run_command([sys.executable, "-m", "venv", str(VENV_DIR)])
    else:
        print(".venv already exists. Reusing it.")

    venv_python = _venv_python()
    print("Installing Python dependencies ...")
    _run_command([str(venv_python), "-m", "pip", "install", "--upgrade", "pip"])
    _run_command([str(venv_python), "-m", "pip", "install", "-r", "requirements.txt"])
    print("Environment is ready.")


def launch_dashboard() -> None:
    if not _venv_python().exists():
        raise SystemExit("No .venv found. Run `python scripts/bootstrap.py --prepare` first.")
    _run_command(
        [str(_venv_python()), "-m", "streamlit", "run", "metro_bike_share_studio.py"],
        extra_env={"PYTHONPATH": str(PROJECT_ROOT / "src")},
    )


def run_pipeline(frequencies: str, max_backtest_folds: int) -> None:
    if not _venv_python().exists():
        raise SystemExit("No .venv found. Run `python scripts/bootstrap.py --prepare` first.")
    _run_command(
        [str(_venv_python()), "-m", "metro_bike_share_forecasting.cli", "run-full-pipeline"],
        extra_env={
            "PYTHONPATH": str(PROJECT_ROOT / "src"),
            "FREQUENCIES": frequencies,
            "MAX_BACKTEST_FOLDS": str(max_backtest_folds),
        },
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bootstrap and launch the Metro Bike Share forecasting project.")
    parser.add_argument("--status", action="store_true", help="Print environment and project status.")
    parser.add_argument("--prepare", action="store_true", help="Create .venv and install requirements.")
    parser.add_argument("--dashboard", action="store_true", help="Launch the Streamlit dashboard.")
    parser.add_argument("--run-pipeline", action="store_true", help="Run the forecasting pipeline from the managed .venv.")
    parser.add_argument("--frequencies", default="daily", help="Comma-separated frequencies to run when using --run-pipeline.")
    parser.add_argument("--max-backtest-folds", type=int, default=2, help="Backtest folds to use with --run-pipeline.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not any([args.status, args.prepare, args.dashboard, args.run_pipeline]):
        print_status()
        print("Use --prepare to install dependencies, --dashboard to launch the studio, or --run-pipeline to execute the pipeline.")
        return

    if args.status:
        print_status()
    if args.prepare:
        prepare_environment()
    if args.run_pipeline:
        run_pipeline(args.frequencies, args.max_backtest_folds)
    if args.dashboard:
        launch_dashboard()


if __name__ == "__main__":
    main()
