from __future__ import annotations

import argparse
import json

from metro_bike_share_forecasting.cleaning.legacy_rules import describe_legacy_reuse
from metro_bike_share_forecasting.config.settings import get_settings
from metro_bike_share_forecasting.utils.logging import setup_logging


def main() -> None:
    parser = argparse.ArgumentParser(description="Metro Bike Share forecasting pipeline")
    parser.add_argument(
        "command",
        choices=["inspect-base-logic", "run-full-pipeline"],
        help="Pipeline action to run",
    )
    args = parser.parse_args()

    settings = get_settings()
    logger = setup_logging(settings.logging_level)

    if args.command == "inspect-base-logic":
        print(json.dumps(describe_legacy_reuse().as_dict(), indent=2))
        return

    from metro_bike_share_forecasting.orchestration.pipeline import ForecastingPipeline

    pipeline = ForecastingPipeline(settings, logger)
    result = pipeline.run()
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
