"""Envirodata service for station observation data (from DWD)."""

import logging
import datetime

import polars as pl
import numpy as np

from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    String,
    Float,
    UniqueConstraint,
    TIMESTAMP,
    insert,
    select,
)

from wetterdienst import Settings
from wetterdienst.provider.dwd.observation import DwdObservationRequest

logger = logging.getLogger(__name__)


class CacheDB:
    """SQLAlchemy DB interface to cache retrieved values"""

    def __init__(self, db_uri: str) -> None:
        """SQLAlchemy DB interface to cache retrieved values

        :param db_uri: (SQLAlchemy) URI for db connection
        :type db_uri: str
        """
        logger.critical("Setting up cache DB at %s", db_uri)
        self.engine = create_engine(
            db_uri, echo=logger.getEffectiveLevel() == logging.DEBUG
        )
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)
        if "stations" not in self.metadata.tables:
            _ = Table(
                "stations",
                self.metadata,
                Column("station_id", String, primary_key=True),
                Column("longitude", Float),
                Column("latitude", Float),
            )
        if "variables" not in self.metadata.tables:
            _ = Table(
                "variables",
                self.metadata,
                Column("station_id", String),
                Column("parameter", String),
                UniqueConstraint("station_id", "parameter"),
            )
        self.metadata.create_all(self.engine)

    def create_station_table(
        self, station_id: str, longitude: float, latitude: float
    ) -> None:
        """Create a new data table for a station, remember station in stations table.

        :param station_id: Station ID
        :type station_id: str
        :param longitude: Geographical longitude
        :type longitude: float
        :param latitude: Geographical latitude
        :type latitude: float
        """
        logger.critical("Adding station table for %s", station_id)
        _ = Table(
            station_id,
            self.metadata,
            Column("date", TIMESTAMP),
            Column("parameter", String),
            Column("value", Float),
            UniqueConstraint("date", "parameter"),
        )

        stmt = insert(self.metadata.tables["stations"]).values(
            station_id=station_id, longitude=longitude, latitude=latitude
        )
        with self.engine.connect() as conn:
            _ = conn.execute(stmt)
            conn.commit()

        self.metadata.create_all(self.engine)

    def _is_cached(self, station_id: str, parameter: str) -> bool:
        """Check if parameter has already been cached for station.

        :param station_id: Station id
        :type station_id: str
        :param parameter: Parameter
        :type parameter: str
        :return: Is parameter already cached for station?
        :rtype: bool
        """
        tbl = self.metadata.tables["variables"]

        stmt = (
            select(tbl)
            .where(tbl.c.station_id == station_id)
            .where(tbl.c.parameter == parameter)
        )

        with self.engine.connect() as conn:
            mei = conn.execute(stmt)
            result = mei.all()

        return len(result) > 0

    def remember_cached_variable(self, station_id: str, parameter: str) -> None:
        """Remember that we have cached a parameter for a station.

        :param station_id: Station id
        :type station_id: str
        :param parameter: Parameter
        :type parameter: str
        """
        with self.engine.connect() as conn:
            _ = conn.execute(
                insert(self.metadata.tables["variables"]),
                {"station_id": station_id, "parameter": parameter},
            )
            conn.commit()

    def insert(
        self,
        station_id: str,
        longitude: float,
        latitude: float,
        parameters: list,
        dates: list,
        values: list,
    ) -> None:
        """Insert (many) values for a parameter at a given station (and its location) into the db.

        :param station_id: Station id
        :type station_id: str
        :param longitude: Geographical longitude
        :type longitude: float
        :param latitude: Geographical latitude
        :type latitude: float
        :param parameters: List of parameters (actually just repeating the same parameter!)
        :type parameters: list
        :param dates: list of the dates to insert (datetime.datetimes)
        :type dates: list
        :param values: list of values to insert (floats)
        :type values: list
        """
        logger.critical("Inserting data for %s %s", station_id, parameters[0])
        if not station_id in self.metadata.tables:
            self.create_station_table(station_id, longitude, latitude)

        if self._is_cached(station_id, parameters[0]):
            logger.critical("Data already cached for %s %s", station_id, parameters[0])
            return

        data = [
            {"parameter": prm, "date": dat, "value": val}
            for prm, dat, val in zip(parameters, dates, values)
        ]

        with self.engine.connect() as conn:
            _ = conn.execute(
                insert(self.metadata.tables[station_id]),
                data,
            )
            conn.commit()

        self.remember_cached_variable(station_id, parameters[0])

    def _get(self, station_id: str, date: datetime.datetime, parameter: str) -> float:
        """Get value for a station at a given date.

        :param station_id: Station id
        :type station_id: str
        :param date: Date
        :type date: datetime.datetime
        :param parameter: Parameter
        :type parameter: str
        :raises ValueError: Station id unknown
        :return: Value for given date and parameter
        :rtype: float
        """
        if not station_id in self.metadata.tables:
            raise ValueError(f"Unknown station ID {station_id}")

        tbl = self.metadata.tables[station_id]

        stmt = (
            select(tbl.c.value)
            .where(tbl.c.parameter == parameter)
            .where(tbl.c.date == date)
            .order_by(tbl.c.date)
        )

        with self.engine.connect() as conn:
            mei = conn.execute(stmt)
            result = (
                mei.first()
            )  # there can be only one: date & parameter combination is uniqueconstraint on table!

        if result:
            result = result[0]  # returns 1-element tuple...

        return result

    def _get_from_stations(self, var: str) -> list:
        """Get values of a given variable from the stations table

        :param var: Variable name
        :type var: str
        :return: Values of this variable
        :rtype: list
        """
        tbl = self.metadata.tables["stations"]
        stmt = select(tbl.c[var])
        with self.engine.connect() as conn:
            mei = conn.execute(stmt)
            result = mei.all()

        return [x[0] for x in result]

    def get(
        self, longitude: float, latitude: float, date: datetime.datetime, parameter: str
    ) -> float:
        """Get value of the station closest to the given space coordinates that actually has data.

        :param longitude: Geographical longitude
        :type longitude: float
        :param latitude: Geographical latitude
        :type latitude: float
        :param date: Date
        :type date: datetime.datetime
        :param parameter: Parameter
        :type parameter: str
        :return: Value of the variable requested
        :rtype: float
        """
        lons = np.array(self._get_from_stations("longitude"))
        lats = np.array(self._get_from_stations("latitude"))
        station_ids = self._get_from_stations("station_id")

        measure = (lons - longitude) ** 2 + (lats - latitude) ** 2

        idx_by_distance = np.argsort(measure)

        station_ids_to_check = [station_ids[i] for i in idx_by_distance]

        data = None
        found = False
        while not found and len(station_ids_to_check) > 0:
            station_id = station_ids_to_check.pop(0)
            data = self._get(station_id, date, parameter)

            if data:
                found = True
                break

        if not found:
            logger.critical(
                "Nothing found for %s at %s, %s on %s",
                parameter,
                longitude,
                latitude,
                date.isoformat(),
            )

        return data


class Loader:
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


class Getter:
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
