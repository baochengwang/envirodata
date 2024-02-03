import logging

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
    def __init__(self, db_uri):
        logger.critical("Setting up cache DB at %s", db_uri)
        self.engine = create_engine(db_uri)  # , echo=True)
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)
        if not "stations" in self.metadata.tables:
            _ = Table(
                "stations",
                self.metadata,
                Column("station_id", String, primary_key=True),
                Column("longitude", Float),
                Column("latitude", Float),
            )
        if not "variables" in self.metadata.tables:
            _ = Table(
                "variables",
                self.metadata,
                Column("station_id", String),
                Column("parameter", String),
                UniqueConstraint("station_id", "parameter"),
            )
        self.metadata.create_all(self.engine)

    def create_station_table(self, station_id, longitude, latitude):
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

    def _is_cached(self, station_id, parameter):
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

    def remember_cached_variable(self, station_id, parameter):
        with self.engine.connect() as conn:
            _ = conn.execute(
                insert(self.metadata.tables["variables"]),
                {"station_id": station_id, "parameter": parameter},
            )
            conn.commit()

    def insert(self, station_id, longitude, latitude, parameters, dates, values):
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

    def _get(self, station_id, date, parameter):
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
            result = mei.all()

        return [x[0] for x in result]

    def _get_from_stations(self, var):
        tbl = self.metadata.tables["stations"]
        stmt = select(tbl.c[var])
        with self.engine.connect() as conn:
            mei = conn.execute(stmt)
            result = mei.all()

        return [x[0] for x in result]

    def get(self, longitude, latitude, date, parameter):
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

            if len(data) > 0:
                found = True
                data = data[0]
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
    def __init__(self, area, wd_cache_dir, db_uri, obs_requests):
        self.area = area
        self.wd_cache_dir = wd_cache_dir
        self.obs_requests = obs_requests

        self.cache_db = CacheDB(db_uri)

    def cache_parameter(
        self,
        start_date,
        end_date,
        parameter,
        resolution,
    ):
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

            dates = [t for t in data["date"]]
            parameters = [x for x in data["parameter"]]
            values = [x for x in data["value"]]

            if len(dates) > 0 and len(parameters) > 0 and len(values) > 0:
                self.cache_db.insert(station_id, lon, lat, parameters, dates, values)

    def cache(self, start_date, end_date):
        for obs_request in self.obs_requests:
            self.cache_parameter(
                start_date,
                end_date,
                **obs_request,
            )


class Getter:
    def __init__(self, db_uri, variable_translation_table):
        self.variable_translation_table = variable_translation_table
        self.cacheDB = CacheDB(db_uri)

    def get(
        self,
        date,
        variable,
        longitude,
        latitude,
    ):
        fvar = self.variable_translation_table[variable]
        return self.cacheDB.get(longitude, latitude, date, fvar)
