"""Run envirodata REST-API server to geocode addressses and deliver environmental factors."""

import sys
import logging
import datetime

import uvicorn
from fastapi import FastAPI

from envirodata.geocoder import Geocoder
from envirodata.environment import Environment

from envirodata.utils.general import get_config

logger = logging.getLogger(__name__)

app = FastAPI()


config = get_config("config.yaml")

geocoder = Geocoder(**config["geocoder"])
environment = Environment(config["environment"])


def main() -> None:
    """Envirodata REST-API.

    :raises RuntimeError: Unable to geocode address.
    """

    @app.get("/")
    def code(
        postcode: str,
        city: str,
        streetname: str,
        house_number: str = "",
        extension: str = "",
    ):
        # (1) geocode address
        try:
            longitude, latitude = geocoder.geocode(
                postcode, city, streetname, house_number, extension
            )
        except RuntimeError as exc:
            raise RuntimeError("Geocoding address failed: ", exc) from exc

        # (2) get environmental factors
        env = environment.get(datetime.datetime(2023, 1, 2), longitude, latitude)

        env.update({"longitude": longitude, "latitude": latitude})

        return env

    uvicorn.run("envirodata.scripts.run_server:app")


if __name__ == "__main__":
    sys.exit(main())
