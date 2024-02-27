"""Envirodata service for station observation data (from DWD)."""

import logging
import datetime

import polars as pl

from wetterdienst import Settings
from wetterdienst.provider.dwd.observation import DwdObservationRequest

from envirodata.services.base import BaseLoader, BaseGetter
from envirodata.services.tools import CacheDB

logger = logging.getLogger(__name__)

class Loader(BaseLoader):
    """Load (cache) station observations dataset."""

    def __init__(
        self, area: list | tuple, wd_cache_dir: str, db_uri: str, obs_requests: list
    ) -> None:
        """Load (cache) station observations dataset. Setup cache DB.

        :param area: Bounding box of the area to cache (as [ lonmin, latmin, lonmax, latmax ])
        :type area: list | tuple
        :param wd_cache_dir: Cache path for the wetterdienst package
        :type wd_cache_dir: str
        :param db_uri: DB connection URI for the cache DB
        :type db_uri: str
        :param obs_requests: Combinations of parameter and resolution
        (see wetterdienst package) to cache
        :type obs_requests: list
        """
        self.area = area
        self.wd_cache_dir = wd_cache_dir
        self.obs_requests = obs_requests

        self.cache_db = CacheDB(db_uri)

    def cache_parameter(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        parameter: str,
        resolution: str,
    ):
        """Cache parameter (at resolution) downloaded by wetterdienst package into cacheDB.

        :param start_date: First date to cache
        :type start_date: datetime.datetime
        :param end_date: Last date to cache
        :type end_date: datetime.datetime
        :param parameter: Parameter
        :type parameter: str
        :param resolution: Resolution
        :type resolution: str
        """
        settings = Settings(cache_dir=self.wd_cache_dir)

        request = DwdObservationRequest(
            parameter=parameter,
            resolution=resolution,
            start_date=start_date,
            end_date=end_date,
            settings=settings,
        )

        stations = request.filter_by_bbox(*self.area)

        nstations = len(stations.station_id)

        logger.critical("Caching %s %s", parameter, resolution)

        i = 0
        for result in stations.values.query():
            logger.critical("Loading station %d of %d", i, nstations)
            i += 1
            data = result.df.drop_nulls()

            station_id = result.df["station_id"][0]

            stp = pl.col("station_id")
            lon = result.stations.df.filter(stp == station_id)["longitude"][0]
            lat = result.stations.df.filter(stp == station_id)["latitude"][0]

            dates = list(data["date"])
            parameters = list(data["parameter"])
            values = list(data["value"])

            if len(dates) > 0 and len(parameters) > 0 and len(values) > 0:
                self.cache_db.insert(station_id, lon, lat, parameters, dates, values)

    def load(self, start_date: datetime.datetime, end_date: datetime.datetime) -> None:
        """Cache all obs_request sets for given date range.

        :param start_date: First date to cache
        :type start_date: datetime.datetime
        :param end_date: Last date to cache
        :type end_date: datetime.datetime
        """
        for obs_request in self.obs_requests:
            self.cache_parameter(
                start_date,
                end_date,
                **obs_request,
            )


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

    def get(
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
