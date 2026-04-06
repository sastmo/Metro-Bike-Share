from __future__ import annotations

import numpy as np
import pandas as pd
import scipy.stats as st
import statsmodels.api as sm

from metro_bike_share_forecasting.features.engineering import build_feature_store
from metro_bike_share_forecasting.forecasting.base import BaseForecaster
from metro_bike_share_forecasting.utils.time import build_future_index


class CountGLMForecaster(BaseForecaster):
    def __init__(self, frequency: str, regime_definition, holiday_country: str) -> None:
        self.frequency = frequency
        self.regime_definition = regime_definition
        self.holiday_country = holiday_country
        self.name = "count_glm"
        self.feature_columns: list[str] = []
        self.result = None
        self.family_name = "poisson"
        self.alpha = 0.0

    def _design_matrix(self, frame: pd.DataFrame) -> pd.DataFrame:
        feature_frame = build_feature_store(frame, self.frequency, self.regime_definition, self.holiday_country)
        model_frame = feature_frame.copy()
        categorical = ["pandemic_phase", "hour_x_phase", "dow_x_phase", "month_x_phase", "weekend_x_phase"]
        available_categorical = [column for column in categorical if column in model_frame.columns]
        model_frame = pd.get_dummies(model_frame, columns=available_categorical, dummy_na=False, drop_first=False)
        excluded = {"target_timestamp", "bucket_end", "generated_at", "feature_payload"}
        x = model_frame.drop(columns=[column for column in excluded if column in model_frame.columns], errors="ignore")
        x = x.drop(columns=["trip_count", "segment_type", "segment_id"], errors="ignore")
        x = x.select_dtypes(include=["number", "bool"]).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        x = x.astype(float)
        return sm.add_constant(x, has_constant="add")

    def fit(self, history: pd.DataFrame) -> "CountGLMForecaster":
        history = history.sort_values("bucket_start").reset_index(drop=True)
        design_matrix = self._design_matrix(history)
        target = history["trip_count"].astype(float)

        dispersion = float(target.var() / max(target.mean(), 1.0)) if len(target) > 1 else 1.0
        if dispersion > 1.5:
            self.family_name = "negative_binomial"
            self.alpha = max((target.var() - target.mean()) / max(target.mean() ** 2, 1.0), 1e-6)
            family = sm.families.NegativeBinomial(alpha=self.alpha)
        else:
            self.family_name = "poisson"
            self.alpha = 0.0
            family = sm.families.Poisson()

        self.result = sm.GLM(target, design_matrix, family=family).fit()
        self.feature_columns = design_matrix.columns.tolist()
        return self

    def _distribution_interval(self, mu: float, level: float) -> tuple[float, float]:
        alpha = (1 - level) / 2
        if self.family_name == "negative_binomial":
            n = 1 / max(self.alpha, 1e-6)
            p = n / (n + mu)
            lower = st.nbinom.ppf(alpha, n, p)
            upper = st.nbinom.ppf(1 - alpha, n, p)
        else:
            lower = st.poisson.ppf(alpha, mu)
            upper = st.poisson.ppf(1 - alpha, mu)
        return max(float(lower), 0.0), max(float(upper), 0.0)

    def forecast(self, history: pd.DataFrame, horizon: int) -> pd.DataFrame:
        if self.result is None:
            raise RuntimeError("Model must be fitted before forecasting.")

        history = history.sort_values("bucket_start").reset_index(drop=True).copy()
        future_index = build_future_index(history["bucket_start"].iloc[-1], self.frequency, horizon)
        predictions: list[dict[str, float | str | pd.Timestamp]] = []
        rolling_history = history.copy()

        for timestamp in future_index:
            next_row = pd.DataFrame(
                [
                    {
                        "bucket_start": timestamp,
                        "trip_count": np.nan,
                        "segment_type": history["segment_type"].iloc[-1],
                        "segment_id": history["segment_id"].iloc[-1],
                        "is_observed": False,
                        "missing_period_flag": False,
                    }
                ]
            )
            scored_history = pd.concat([rolling_history, next_row], ignore_index=True)
            x_future = self._design_matrix(scored_history).tail(1).reindex(columns=self.feature_columns, fill_value=0.0)
            mu = float(self.result.predict(x_future).iloc[0])
            mu = max(mu, 0.0)
            next_row["trip_count"] = mu
            rolling_history = pd.concat([rolling_history, next_row], ignore_index=True)

            record: dict[str, float | str | pd.Timestamp] = {
                "target_timestamp": timestamp,
                "prediction": mu,
                "model_name": self.name,
            }
            for level in (0.50, 0.80, 0.95):
                lower, upper = self._distribution_interval(mu, level)
                record[f"lower_{int(level * 100)}"] = lower
                record[f"upper_{int(level * 100)}"] = upper
            predictions.append(record)

        return pd.DataFrame(predictions)
