from __future__ import annotations

import numpy as np
import pandas as pd
import scipy.stats as st
import statsmodels.api as sm

from metro_bike_share_forecasting.features.engineering import build_feature_store
from metro_bike_share_forecasting.forecasting.base import BaseForecaster
from metro_bike_share_forecasting.utils.time import build_future_index


GLM_CATEGORICAL_FEATURES = {
    "hourly": ["hour_of_day", "day_of_week", "pandemic_phase"],
    "daily": ["day_of_week", "month", "pandemic_phase"],
    "weekly": ["month", "pandemic_phase"],
    "monthly": ["month", "pandemic_phase"],
    "quarterly": ["quarter", "pandemic_phase"],
}


GLM_NUMERIC_FEATURES = {
    "hourly": [
        "is_weekend",
        "is_holiday",
        "lag_1",
        "lag_24",
        "lag_168",
        "rolling_mean_24",
        "rolling_mean_168",
        "rolling_std_24",
        "rolling_std_168",
        "is_lockdown",
        "is_reopening",
        "is_post_pandemic",
        "days_since_lockdown_start",
        "days_since_reopening_start",
        "missing_period_flag",
        "recency_weight",
    ],
    "daily": [
        "is_weekend",
        "is_holiday",
        "lag_1",
        "lag_7",
        "lag_28",
        "rolling_mean_7",
        "rolling_mean_28",
        "rolling_std_7",
        "rolling_std_28",
        "is_lockdown",
        "is_reopening",
        "is_post_pandemic",
        "days_since_lockdown_start",
        "days_since_reopening_start",
        "missing_period_flag",
        "recency_weight",
    ],
    "weekly": [
        "lag_1",
        "lag_4",
        "lag_12",
        "rolling_mean_4",
        "rolling_mean_12",
        "rolling_std_4",
        "rolling_std_12",
        "is_lockdown",
        "is_reopening",
        "is_post_pandemic",
        "days_since_lockdown_start",
        "days_since_reopening_start",
        "missing_period_flag",
        "recency_weight",
    ],
    "monthly": [
        "lag_1",
        "lag_3",
        "lag_12",
        "rolling_mean_3",
        "rolling_mean_12",
        "rolling_std_3",
        "rolling_std_12",
        "is_lockdown",
        "is_reopening",
        "is_post_pandemic",
        "days_since_lockdown_start",
        "days_since_reopening_start",
        "missing_period_flag",
        "recency_weight",
    ],
    "quarterly": [
        "lag_1",
        "lag_2",
        "lag_4",
        "rolling_mean_2",
        "rolling_mean_4",
        "rolling_std_2",
        "rolling_std_4",
        "is_lockdown",
        "is_reopening",
        "is_post_pandemic",
        "days_since_lockdown_start",
        "days_since_reopening_start",
        "missing_period_flag",
        "recency_weight",
    ],
}


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
        for column in ("days_since_lockdown_start", "days_since_reopening_start"):
            if column in model_frame.columns:
                model_frame[column] = np.log1p(pd.to_numeric(model_frame[column], errors="coerce").clip(lower=0))

        numeric_columns = [column for column in GLM_NUMERIC_FEATURES[self.frequency] if column in model_frame.columns]
        categorical_columns = [column for column in GLM_CATEGORICAL_FEATURES[self.frequency] if column in model_frame.columns]
        selected_columns = numeric_columns + categorical_columns
        x = model_frame[selected_columns].copy()
        if categorical_columns:
            x = pd.get_dummies(x, columns=categorical_columns, dummy_na=False, drop_first=True)
        x = x.replace([np.inf, -np.inf], np.nan).fillna(0.0)
        x = x.loc[:, x.nunique(dropna=False) > 1]
        x = x.astype(float).clip(lower=-1e6, upper=1e6)
        return sm.add_constant(x, has_constant="add")

    def fit(self, history: pd.DataFrame) -> "CountGLMForecaster":
        history = history.sort_values("bucket_start").reset_index(drop=True)
        design_matrix = self._design_matrix(history)
        target = history["trip_count"].astype(float)

        dispersion = float(target.var() / max(target.mean(), 1.0)) if len(target) > 1 else 1.0
        candidate_families: list[tuple[str, object]] = []
        if dispersion > 1.5:
            self.alpha = max((target.var() - target.mean()) / max(target.mean() ** 2, 1.0), 1e-6)
            candidate_families.append(("negative_binomial", sm.families.NegativeBinomial(alpha=self.alpha)))
        candidate_families.append(("poisson", sm.families.Poisson()))

        last_error: Exception | None = None
        for family_name, family in candidate_families:
            try:
                self.result = sm.GLM(target, design_matrix, family=family).fit(maxiter=200, tol=1e-8)
                self.family_name = family_name
                if family_name != "negative_binomial":
                    self.alpha = 0.0
                self.feature_columns = design_matrix.columns.tolist()
                return self
            except Exception as exc:  # pragma: no cover - exercised by real data fallback
                last_error = exc

        try:
            regularized = sm.GLM(target, design_matrix, family=sm.families.Poisson()).fit_regularized(
                alpha=0.01,
                L1_wt=0.0,
                maxiter=500,
            )
            self.result = regularized
            self.family_name = "poisson"
            self.alpha = 0.0
            self.feature_columns = design_matrix.columns.tolist()
            return self
        except Exception as exc:  # pragma: no cover - exercised by real data fallback
            last_error = exc

        if last_error is not None:
            raise last_error
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
