"""Run envirodata REST-API server to geocode addressses and deliver environmental factors."""

import sys
import logging
import datetime
import pytz

import uvicorn
from fastapi import FastAPI, HTTPException, status
from fastapi.responses import ORJSONResponse as JSONResponse

from envirodata.geocoder import Geocoder
from envirodata.environment import Environment

from envirodata.utils.general import get_config

logger = logging.getLogger(__name__)

app = FastAPI()


config = get_config("config.yaml")

geocoder = Geocoder(**config["geocoder"])
environment = Environment(config["environment"])

start_date = datetime.datetime.fromisoformat(config["period"]["start_date"])
if start_date.tzinfo is None:
    start_date = start_date.replace(tzinfo=pytz.UTC)

end_date = datetime.datetime.fromisoformat(config["period"]["end_date"])
if end_date.tzinfo is None:
    end_date = end_date.replace(tzinfo=pytz.UTC)


def main() -> None:
    """Envirodata REST-API."""

    @app.get("/")
    def retrieve(
        date: datetime.datetime,
        address: str,
    ):
        """Retrieve environmental factors for a given date and address."""
        if date.tzinfo is None:
            logger.critical("Requested date not timezone-aware, assuming UTC!")
            date = date.replace(tzinfo=pytz.UTC)

        # (0) check date in range?
        if (date < start_date) or (date > end_date):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Requested date out of cached range ({start_date} - {end_date})",
            )

        # (1) geocode address
        try:
            longitude, latitude = geocoder.geocode(address)
        except RuntimeError as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Geocoding address failed: {str(exc)}",
            ) from exc

        # (2) get environmental factors
        env = environment.get(date, longitude, latitude)

        return JSONResponse(env)

    @app.get("/by_elements")
    def retrieve_by_elements(
        date: datetime.datetime,
        postcode: str,
        city: str,
        streetname: str,
        house_number: str = "",
        extension: str = "",
    ):
        """Retrieve environmental factors for a given date and address (as individual
        elements)."""
        address = geocoder.standardize_address(
            postcode, city, streetname, house_number, extension
        )
        return retrieve(date, address)

    uvicorn.run("envirodata.scripts.run_server:app")


if __name__ == "__main__":
    sys.exit(main())
