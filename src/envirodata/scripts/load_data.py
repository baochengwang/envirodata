"""Load (cache) all environmental factor data for a given date range."""

import sys
import logging
import datetime

from envirodata.environment import Environment

from envirodata.utils.general import get_cli_arguments, get_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = get_cli_arguments()

config = get_config(args.config_file)

environment = Environment(config["environment"])

start_date = datetime.datetime.fromisoformat(config["period"]["start_date"]).astimezone(
    datetime.UTC
)
end_date = datetime.datetime.fromisoformat(config["period"]["end_date"]).astimezone(
    datetime.UTC
)


def main() -> bool:
    """Load (cache) all environmental factor data for a given date range."""
    environment.load(start_date, end_date)
    return True


if __name__ == "__main__":
    sys.exit(main())
