"""Envirodata service for Wetterdienst REST-API datasets."""

import logging
import datetime

import requests  # type: ignore

from envirodata.services.base import (
    BaseLoader,
    BaseGetter,
    DayMinimum,
    DayAverage,
    DayMaximum,
    SevenDayAverage,
    ThirtyDayAverage,
)

from envirodata.utils.cacheDB import CacheDB

logger = logging.getLogger(__name__)

TIME_RESOLUTION = datetime.timedelta(hours=1)


class Loader(BaseLoader):
    """Load (cache) cdsapi dataset."""

    def __init__(
        self,
        rest_api_url: str,
        db_uri: str,
        obs_requests: list,
        bbox: list[float],
    ) -> None:
        """Load (cache) dataset."""
        self.rest_api_url = rest_api_url
        self.obs_requests = obs_requests
        self.bbox = bbox

        self.cache_db = CacheDB(db_uri)

    def load(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
    ) -> None:
        """Load (cache) all data between given dates.

        :param start_date: First date to load
        :type start_date: datetime.datetime
        :param end_date: Last date to load
        :type end_date: datetime.datetime
        """

        for period in ["historical", "recent"]:
            for obs in self.obs_requests:
                stations_url = f"{self.rest_api_url}/stations"

                params = {
                    "provider": "dwd",
                    "network": "observation",
                    "parameter": "humidity",
                    "resolution": "daily",
                    "period": period,
                    "bbox": ",".join([str(x) for x in self.bbox]),
                }
                params.update(obs)

                logger.critical(params)

                response = requests.get(stations_url, params=params, timeout=10)

                stations_data = response.json()

                stations = {s["station_id"]: s for s in stations_data["stations"]}

                values_url = f"{self.rest_api_url}/values"
                params.update(
                    {
                        "date": start_date.strftime("%Y-%m-%d")
                        + "/"
                        + end_date.strftime("%Y-%m-%d")
                    }
                )
                response = requests.get(values_url, params=params, timeout=120)
                data = response.json()
                # in data["metadata"], we have the full author info incl. copyright...!

                for entry in data["values"]:
                    station_id = entry["station_id"]
                    lon = stations[station_id]["longitude"]
                    lat = stations[station_id]["latitude"]

                    parameter = entry["parameter"]
                    date = datetime.datetime.fromisoformat(entry["date"])
                    value = entry["value"]

                    if value:
                        try:
                            self.cache_db.insert(
                                station_id, lon, lat, [parameter], [date], [value]
                            )
                        except Exception as exc:
                            logger.debug(exc)


class Getter(BaseGetter):
    """Get values from dataset."""

    def __init__(self, db_uri: str, variable_translation_table: dict) -> None:
        """_summary_

        :param db_uri: (SQLAlchemy) URI for db connection
        :type db_uri: str
        :param variable_translation_table: Translation table from file
        variable name to name in Envirodata API.
        :type variable_translation_table: dict
        """
        self.variable_translation_table = variable_translation_table
        self.cache_db = CacheDB(db_uri)

    @property
    def time_resolution(self):
        return datetime.timedelta(hours=1)

    @property
    def statistics(self):
        return [
            DayMinimum,
            DayAverage,
            DayMaximum,
            SevenDayAverage,
            ThirtyDayAverage,
        ]

    def _get(
        self,
        date: datetime.datetime,
        longitude: float,
        latitude: float,
        variable: str,
    ) -> float:
        """Get value for variable out of cache DB

        :param date: Date to retrieve
        :type date: datetime.datetime
        :param longitude: Geographical longitude
        :type longitude: float
        :param latitude: Geographical latitude
        :type latitude: float
        :param variable: Variable to retrieve
        :type variable: str
        :return: Value for variable at given point in time and space.
        :rtype: float
        """
        fvar = self.variable_translation_table[variable]
        return self.cache_db.get(longitude, latitude, date, fvar)

    def _get_range(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        longitude: float,
        latitude: float,
        variable: str,
    ) -> float:
        """Get value for variable out of cache DB

        :param date: Date to retrieve
        :type date: datetime.datetime
        :param longitude: Geographical longitude
        :type longitude: float
        :param latitude: Geographical latitude
        :type latitude: float
        :param variable: Variable to retrieve
        :type variable: str
        :return: Value for variable at given point in time and space.
        :rtype: float
        """
        fvar = self.variable_translation_table[variable]
        return self.cache_db.get_range(longitude, latitude, start_date, end_date, fvar)
