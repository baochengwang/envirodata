import sys
import logging
import datetime

from envirodata.environment import Environment

from envirodata.utils.general import get_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

config = get_config("config.yaml")

environment = Environment(config["environment"])


def main():
    environment.load(datetime.datetime(2023, 1, 1), datetime.datetime(2023, 2, 1))


if __name__ == "__main__":
    sys.exit(main())
