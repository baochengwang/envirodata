"""Run envirodata REST-API server to geocode addresses
and deliver environmental factors."""

import sys
import logging
import datetime
from importlib.metadata import version
from typing import Any
from io import BytesIO
import pytz

import uvicorn
from fastapi import FastAPI, HTTPException, status, UploadFile, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import ORJSONResponse as JSONResponse
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import pandas as pd
import uvicorn.logging

from envirodata.geocoder import Geocoder
from envirodata.environment import Environment

from envirodata.utils.general import get_cli_arguments, get_config, get_git_commit_hash

# logger = logging.getLogger(__name__)
logger = logging.getLogger("uvicorn.error")
logger.setLevel(logging.DEBUG)

args = get_cli_arguments()

config = get_config(args.config_file)

app = FastAPI(**config["fastapi"])

app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")

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

    def _get_metadata(date: datetime.datetime) -> dict[str, Any]:
        """Create basic metadata for response.

        :param date: date requested
        :type date: datetime.datetime
        :return: basic metadata (package version, git commit, creation date)
        :rtype: dict[str, Any]
        """
        metadata = {
            "package_version": version("envirodata"),
            "git_commit_hash": get_git_commit_hash(),
            "creation_date": datetime.datetime.now(
                tz=datetime.timezone.utc
            ).isoformat(),
        }

        metadata["requested_date_utc"] = date.isoformat()

        return metadata

    def _retrieve(
        date: datetime.datetime,
        address: str,
    ) -> dict[str, Any]:
        """Retrieve environmental factors for a given date and address. (internal)

        :param date: date requested
        :type date: datetime.datetime
        :param address: address requested
        :type address: str
        :raises HTTPException: address could not be geocoded
        :return: exposure estimate
        :rtype: dict[str, Any]
        """
        metadata = _get_metadata(date)

        # (1) geocode address
        try:
            longitude, latitude = geocoder.geocode(address)
        except IOError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Geocoding address failed: {str(exc)}",
            ) from exc

        geocoding = {
            "address": address,
            "location": {"longitude": longitude, "latitude": latitude},
        }

        # (2) get environmental factors
        env = environment.get(date, longitude, latitude)

        result = {"metadata": metadata, "geocoding": geocoding, "environment": env}

        return result

    @app.get("/")
    def home(request: Request):
        return templates.TemplateResponse("home.html", context={"request": request})

    @app.get("/metadata")
    def metadata(request: Request):
        return templates.TemplateResponse(
            "metadata.html",
            context={"request": request, "metadata": environment.metadata()},
        )

    @app.get("/manual")
    def retrieve(
        date: datetime.datetime,
        address: str,
    ) -> JSONResponse:
        """Retrieve environmental factors for a given date and address.

        :param date: date requested
        :type date: datetime.datetime
        :param address: address requested
        :type address: str
        :raises HTTPException: date not within cached range
        :return: exposure estimate
        :rtype: JSONResponse
        """

        if date.tzinfo is None:
            logger.critical("Requested date not timezone-aware, assuming UTC!")
            date = date.replace(tzinfo=pytz.UTC)

        # (0) check date in range?
        if (date < start_date) or (date > end_date):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Requested date out of cached range ({start_date} - {end_date})",
            )

        result = _retrieve(date, address)

        # serializes also datetimes, ...
        json_result = jsonable_encoder(result)

        return JSONResponse(json_result)

    @app.get("/manual_by_elements")
    def retrieve_by_elements(
        date: datetime.datetime,
        postcode: str,
        city: str,
        streetname: str,
        house_number: str = "",
        extension: str = "",
    ) -> JSONResponse:
        """Retrieve environmental factors for a given date and address (as individual
        elements).

        :param date: date requested
        :type date: datetime.datetime
        :param postcode: post code
        :type postcode: str
        :param city: city
        :type city: str
        :param streetname: street name
        :type streetname: str
        :param house_number: house number, defaults to ""
        :type house_number: str, optional
        :param extension: address extension, defaults to ""
        :type extension: str, optional
        :return: exposure estimate
        :rtype: JSONResponse
        """
        address = geocoder.standardize_address(
            postcode, city, streetname, house_number, extension
        )
        return retrieve(date, address)

    @app.post("/excel")
    async def create_upload_file(file: UploadFile) -> StreamingResponse:

        contents = file.file.read()
        data = BytesIO(contents)
        try:
            df = pd.read_excel(data, usecols=["id", "date", "address"])
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
            )

        result = {}

        i = 0
        for idx, row in df.iterrows():
            logger.info("Working on row %d of %d", i, len(df))
            try:
                date = row["date"].tz_localize(pytz.utc)
                result[row["id"]] = _retrieve(date, row["address"])
            except Exception as exc:
                result[row["id"]] = {"failed": str(exc)}
            i += 1

        flat = pd.DataFrame()
        for id, envrow in result.items():
            try:
                env = {"id": id}
                for service, data in envrow["environment"].items():
                    env[service] = data["values"]

                env_pd = pd.json_normalize(env)
            except KeyError:  # if getting env failed previously, make an empty row
                env_pd = pd.DataFrame.from_dict({0: {"id": id}}, orient="index")

            flat = pd.concat([flat, env_pd])

        buffer = BytesIO()
        with pd.ExcelWriter(buffer) as writer:
            flat.to_excel(writer, index=False)

        return StreamingResponse(
            BytesIO(buffer.getvalue()),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=environment.xlsx"},
        )

    uvicorn_config = uvicorn.Config(app, **config["uvicorn"])
    uvicorn_server = uvicorn.Server(uvicorn_config)
    uvicorn_server.run()


if __name__ == "__main__":
    sys.exit(main())
