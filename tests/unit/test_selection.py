from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from metro_bike_share_forecasting.selection.champion import select_champion_model, summarize_backtests


class SelectionTests(unittest.TestCase):
    def test_champion_selection_returns_lowest_composite_score(self) -> None:
        metric_frame = pd.DataFrame(
            [
                {"model_name": "a", "frequency": "daily", "fold_id": 1, "horizon_step": 1, "evaluation_regime": "post_pandemic", "mae": 2, "rmse": 2, "mape": 0.2, "smape": 0.2, "mase": 0.3, "pinball_80": 0.3, "coverage_80": 0.8, "width_80": 4, "bias": 0.1},
                {"model_name": "b", "frequency": "daily", "fold_id": 1, "horizon_step": 1, "evaluation_regime": "post_pandemic", "mae": 1, "rmse": 1, "mape": 0.1, "smape": 0.1, "mase": 0.2, "pinball_80": 0.2, "coverage_80": 0.8, "width_80": 3, "bias": 0.0},
            ]
        )
        summary = summarize_backtests(metric_frame)
        champion = select_champion_model(summary)
        self.assertEqual(champion.iloc[0]["model_name"], "b")


if __name__ == "__main__":
    unittest.main()
