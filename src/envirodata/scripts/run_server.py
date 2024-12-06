"""Run envirodata REST-API server to geocode addresses
and deliver environmental factors."""

import sys
import logging
import datetime
from importlib.metadata import version
from typing import Any
from io import BytesIO
import pytz
from enum import Enum
import math

import uvicorn
from fastapi import FastAPI, HTTPException, status, UploadFile, Request, BackgroundTasks
from fastapi.encoders import jsonable_encoder
from fastapi.responses import ORJSONResponse as JSONResponse
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import pandas as pd
import markdown
import uvicorn.logging

from envirodata.geocoder import Geocoder
from envirodata.environment import Environment

from envirodata.utils.general import get_cli_arguments, get_config, get_git_commit_hash

README_md_fpath = Path(__file__).parent.parent.parent.parent / "README.md"
INSTALL_md_fpath = Path(__file__).parent.parent.parent.parent / "INSTALL.md"

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


class ExcelJob:
    class Status(Enum):
        ERROR = "ERROR"
        PENDING = "PENDING"
        SUCCESS = "SUCCESS"
        STARTED = "STARTED"

    def __init__(self):
        self.reset()

    def reset(self):
        self.status = ExcelJob.Status.PENDING
        self.percentDone = 0.0

        self.buffer = BytesIO()

    def run(self, contents):
        try:
            self.status = ExcelJob.Status.STARTED

            data = BytesIO(contents)
            try:
                df = pd.read_excel(data, usecols=["id", "date", "address"])
            except ValueError as exc:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
                ) from exc

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
                self.percentDone = math.floor(float(i) / len(df) * 100.0)

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

            self.buffer = BytesIO()
            with pd.ExcelWriter(self.buffer) as writer:
                flat.to_excel(writer, index=False)

            self.percentDone = 0.0

            self.status = ExcelJob.Status.SUCCESS
        except Exception as e:
            self.status = ExcelJob.Status.ERROR

    def get_state(self):
        if self.status == ExcelJob.Status.STARTED:
            return self.percentDone
        return self.status


excelJob = ExcelJob()


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
        "creation_date": datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
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


def validate_date(date: datetime.datetime) -> datetime.datetime:
    if date.tzinfo is None:
        logger.critical("Requested date not timezone-aware, assuming UTC!")
        date = date.replace(tzinfo=pytz.UTC)

    # (0) check date in range?
    if (date < start_date) or (date > end_date):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Requested date out of cached range " f"({start_date} - {end_date})",
        )
    return date


def main() -> None:
    """Envirodata REST-API."""

    @app.get("/")
    def home(request: Request):
        with open(README_md_fpath, "r") as f:
            readme_str = f.read()
        readme_html = markdown.markdown(readme_str)
        return templates.TemplateResponse(
            "home.html", context={"request": request, "readme": readme_html}
        )

    @app.get("/install")
    def install(request: Request):
        with open(INSTALL_md_fpath, "r") as f:
            install_str = f.read()
        install_html = markdown.markdown(install_str)
        return templates.TemplateResponse(
            "install.html", context={"request": request, "install": install_html}
        )

    @app.get("/manual")
    def manual(request: Request):
        return templates.TemplateResponse(
            "manual.html",
            context={"request": request, "metadata": environment.metadata()},
        )

    @app.get("/excel")
    def excel(request: Request):
        return templates.TemplateResponse(
            "excel.html",
            context={"request": request, "metadata": environment.metadata()},
        )

    @app.get("/metadata")
    def metadata(request: Request):
        return templates.TemplateResponse(
            "metadata.html",
            context={"request": request, "metadata": environment.metadata()},
        )

    @app.get("/api/html")
    def api_html(
        request: Request, date: datetime.datetime, address: str
    ) -> HTMLResponse:
        date = validate_date(date)

        result = _retrieve(date, address)

        return templates.TemplateResponse(
            "result_table.html",
            context={"request": request, "environment": result["environment"]},
        )

    @app.get("/api/simple")
    def api_simple(date: datetime.datetime, address: str) -> JSONResponse:
        """Retrieve environmental factors for a given date and address.

        :param date: date requested
        :type date: datetime.datetime
        :param address: address requested
        :type address: str
        :raises HTTPException: date not within cached range
        :return: exposure estimate
        :rtype: JSONResponse
        """

        date = validate_date(date)

        result = _retrieve(date, address)

        # serializes also datetimes, ...
        json_result = jsonable_encoder(result)

        return JSONResponse(json_result)

    @app.post("/api/excel/submit", status_code=status.HTTP_201_CREATED)
    async def api_excel_submit(file: UploadFile, background_tasks: BackgroundTasks):

        if excelJob.status == ExcelJob.Status.STARTED:
            return {"message": "Job is running"}

        contents = file.file.read()
        background_tasks.add_task(excelJob.run, contents)
        return {"message": "Job created"}

    @app.get("/api/excel/status", status_code=status.HTTP_200_OK)
    def api_excel_status():
        return excelJob.get_state()

    @app.get("/api/excel/get")
    def api_excel_get() -> StreamingResponse:
        if not excelJob.get_state() == excelJob.Status.SUCCESS:
            raise HTTPException(
                status_code=status.HTTP_425_TOO_EARLY, detail="Result not ready yet!"
            )

        return StreamingResponse(
            BytesIO(excelJob.buffer.getvalue()),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=environment.xlsx"},
        )

    @app.get("/api/excel/reset", status_code=status.HTTP_200_OK)
    def api_excel_reset():
        excelJob.reset()
        return True

    uvicorn_config = uvicorn.Config(app, **config["uvicorn"])
    uvicorn_server = uvicorn.Server(uvicorn_config)
    uvicorn_server.run()


if __name__ == "__main__":
    sys.exit(main())
