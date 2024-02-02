import logging
import datetime
import os

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
    def __init__(self, conn):
        logger.debug("Setting up cache DB at %s", conn)
        self.engine = create_engine(conn)  # , echo=True)
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
        self.metadata.create_all(self.engine)

    def create_station_table(self, station_id, longitude, latitude):
        logger.debug("Adding station table for %s", station_id)
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

    def insert(self, station_id, longitude, latitude, parameters, dates, values):
        logger.debug("Inserting data for %s %s", station_id, parameters[0])
        if not station_id in self.metadata.tables:
            self.create_station_table(station_id, longitude, latitude)

        data = [
            {"parameter": prm, "date": dat, "value": val}
            for prm, dat, val in zip(parameters, dates, values)
        ]

        with self.engine.connect() as conn:
            result = conn.execute(
                insert(self.metadata.tables[station_id]),
                data,
            )
            conn.commit()

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


def cache_parameter(date, parameter, resolution, area, wd_cache_dir, cache_db):
    settings = Settings(cache_dir=wd_cache_dir)

    request = DwdObservationRequest(
        parameter=parameter,
        resolution=resolution,
        start_date=date,
        end_date=date + datetime.timedelta(hours=23, minutes=59, seconds=59),
        settings=settings,
    )

    stations = request.filter_by_bbox(*area)

    logger.info("Caching %s %s for %s", parameter, resolution, date.isoformat())

    for result in stations.values.query():
        data = result.df.drop_nulls()

        station_id = result.df["station_id"][0]

        stp = pl.col("station_id")
        lon = result.stations.df.filter(stp == station_id)["longitude"][0]
        lat = result.stations.df.filter(stp == station_id)["latitude"][0]

        dates = [t for t in data["date"]]
        parameters = [x for x in data["parameter"]]
        values = [x for x in data["value"]]

        if len(dates) > 0 and len(parameters) > 0 and len(values) > 0:
            cache_db.insert(station_id, lon, lat, parameters, dates, values)


def cache(date, area, wd_cache_dir, db_connection, obs_requests):
    cache_db = CacheDB(db_connection)
    for obs_request in obs_requests:
        cache_parameter(
            date, area=area, wd_cache_dir=wd_cache_dir, cache_db=cache_db, **obs_request
        )


def get(date, variable, longitude, latitude, db_connection, variable_translation_table):
    cache_db = CacheDB(db_connection)
    fvar = variable_translation_table[variable]
    return cache_db.get(longitude, latitude, date, fvar)
