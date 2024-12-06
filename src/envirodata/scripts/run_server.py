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
from pathlib import Path
import threading

import uvicorn
from fastapi import FastAPI, HTTPException, status, UploadFile, Request
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


class ExcelJob(threading.Thread):

    MAX_MSG_LENGTH = 3

    class Status(Enum):
        ERROR = "ERROR"
        PENDING = "PENDING"
        SUCCESS = "SUCCESS"
        STARTED = "STARTED"

    def __init__(self, contents):
        super().__init__()
        self.killed = False

        self.messages = [""] * self.MAX_MSG_LENGTH
        self.status = ExcelJob.Status.PENDING
        self.percentDone = 0.0

        self.buffer = BytesIO()
        self.contents = contents

    def kill(self):
        self.killed = True

    def add_message(self, msg):
        self.messages.append(msg)
        if len(self.messages) > self.MAX_MSG_LENGTH:
            self.messages.pop(0)

    def run(self):
        try:
            self.status = ExcelJob.Status.STARTED

            data = BytesIO(self.contents)
            try:
                df = pd.read_excel(data, usecols=["id", "date", "address"])
            except ValueError as exc:
                self.status = ExcelJob.Status.ERROR
                self.add_message(str(exc))
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
                ) from exc

            result = {}

            i = 0
            for idx, row in df.iterrows():
                if self.killed:
                    return
                logger.info("Working on row %d of %d", i, len(df))
                try:
                    date = row["date"].tz_localize(pytz.utc)
                    result[row["id"]] = _retrieve(date, row["address"])
                    self.add_message(f"Successfully retrieved row {i} of {len(df)}")
                except Exception as exc:
                    result[row["id"]] = {"failed": str(exc)}
                    self.add_message(f"Error retrieving row {i} of {len(df)}")
                i += 1
                self.percentDone = math.floor(float(i) / len(df) * 100.0)

            flat = pd.DataFrame()
            for id, envrow in result.items():
                if self.killed:
                    return
                try:
                    env = {"id": id}
                    for service, data in envrow["environment"].items():
                        env[service] = data["values"]

                    env_pd = pd.json_normalize(env)
                    self.add_message(f"Successfully converted row {i} of {len(df)}")
                except KeyError:  # if getting env failed previously, make an empty row
                    env_pd = pd.DataFrame.from_dict({0: {"id": id}}, orient="index")
                    self.add_message(f"Ignoring row {i} of {len(df)}")

                flat = pd.concat([flat, env_pd])

            self.add_message("Writing to output file")
            self.buffer = BytesIO()
            with pd.ExcelWriter(self.buffer) as writer:
                flat.to_excel(writer, index=False)

            self.add_message("Done")
            self.percentDone = 0.0

            self.status = ExcelJob.Status.SUCCESS
        except Exception:
            self.add_message("Failed")
            self.status = ExcelJob.Status.ERROR

    def get_buffer(self):
        return self.buffer

    def get_state(self):
        return {
            "state": self.status,
            "messages": self.messages,
            "percent": self.percentDone,
        }


# there is only one...
excel_task_name = "EXCEL"

running_threads: dict[str, ExcelJob] = {}


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
    def api_excel_submit(file: UploadFile):

        if excel_task_name in running_threads:
            return {"message": "Job is already running"}

        if not file:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="No file selected"
            )

        contents = file.file.read()

        running_threads[excel_task_name] = ExcelJob(contents)
        running_threads[excel_task_name].start()

        return {"message": "Job created"}

    @app.get("/api/excel/status", status_code=status.HTTP_200_OK)
    def api_excel_status() -> JSONResponse:
        state = {
            "state": ExcelJob.Status.PENDING,
            "messages": ["", "", ""],
            "percent": 0.0,
        }
        if excel_task_name in running_threads:
            state = running_threads[excel_task_name].get_state()
        return JSONResponse(state)

    @app.get("/api/excel/get")
    def api_excel_get() -> StreamingResponse:
        if excel_task_name not in running_threads:
            raise HTTPException(
                status_code=status.HTTP_425_TOO_EARLY,
                detail="Processing has not started yet!",
            )
        jobstate = running_threads[excel_task_name].get_state()
        if not jobstate["state"] == ExcelJob.Status.SUCCESS:
            raise HTTPException(
                status_code=status.HTTP_425_TOO_EARLY, detail="Result not ready (yet)!"
            )

        buffer = running_threads[excel_task_name].get_buffer()
        return StreamingResponse(
            BytesIO(buffer.getvalue()),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=environment.xlsx"},
        )

    @app.get("/api/excel/reset", status_code=status.HTTP_200_OK)
    def api_excel_reset():
        # no thread to kill anyway
        if excel_task_name not in running_threads:
            return True

        running_threads[excel_task_name].kill()
        running_threads[excel_task_name].join()

        if running_threads[excel_task_name].is_alive():
            raise IOError("Excel thread still alive!")

        del running_threads[excel_task_name]

        return True

    uvicorn_config = uvicorn.Config(app, **config["uvicorn"])
    uvicorn_server = uvicorn.Server(uvicorn_config)
    uvicorn_server.run()


if __name__ == "__main__":
    sys.exit(main())
