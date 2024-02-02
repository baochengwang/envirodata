import sys
import logging
import datetime

from envirodata.environment import Environment

from envirodata.utils.general import get_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

config = get_config("config.yaml")

environment = Environment(config["environment"])

start_date = datetime.datetime.fromisoformat(config["period"]["start_date"])
end_date = datetime.datetime.fromisoformat(config["period"]["end_date"])


def main():
    environment.load(start_date, end_date)


if __name__ == "__main__":
    sys.exit(main())
