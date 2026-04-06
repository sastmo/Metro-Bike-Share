from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class BaseForecaster(ABC):
    name: str

    @abstractmethod
    def fit(self, history: pd.DataFrame) -> "BaseForecaster":
        raise NotImplementedError

    @abstractmethod
    def forecast(self, history: pd.DataFrame, horizon: int) -> pd.DataFrame:
        raise NotImplementedError

