PYTHON ?= python3
PYTHONPATH ?= src

.PHONY: validate test inspect-base run-full run-daily-fast run-daily-station bootstrap studio

validate:
	$(PYTHON) scripts/validate_repo.py

test:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m unittest discover -s tests -p 'test_*.py'

inspect-base:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m metro_bike_share_forecasting.cli inspect-base-logic

run-full:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m metro_bike_share_forecasting.cli run-full-pipeline

run-daily-fast:
	PYTHONPATH=$(PYTHONPATH) FREQUENCIES=daily MAX_BACKTEST_FOLDS=2 STATION_LEVEL_TOP_N=3 $(PYTHON) -m metro_bike_share_forecasting.cli run-full-pipeline

run-daily-station:
	PYTHONPATH=$(PYTHONPATH) FREQUENCIES=daily MAX_BACKTEST_FOLDS=8 STATION_LEVEL_TOP_N=5 STATION_LEVEL_FREQUENCIES=daily STATION_ENABLED_MODELS=naive,seasonal_naive,rolling_mean,count_glm $(PYTHON) -m metro_bike_share_forecasting.cli run-full-pipeline

bootstrap:
	$(PYTHON) scripts/bootstrap.py --prepare

studio:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m streamlit run metro_bike_share_studio.py
