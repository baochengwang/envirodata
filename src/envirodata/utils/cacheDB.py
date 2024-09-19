import logging
import datetime

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

logger = logging.getLogger(__name__)


class CacheDB:
    """SQLAlchemy DB interface to cache retrieved values"""

    def __init__(self, db_uri: str) -> None:
        """SQLAlchemy DB interface to cache retrieved values

        :param db_uri: (SQLAlchemy) URI for db connection
        :type db_uri: str
        """
        logger.debug("Setting up cache DB at %s", db_uri)
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
        # if we have it already, ignore
        if station_id in self.metadata.tables:
            return

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

    def create_variable_table_entry(self, station_id: str, parameter: str) -> None:
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

        if len(result) == 0:
            with self.engine.connect() as conn:
                _ = conn.execute(
                    insert(self.metadata.tables["variables"]),
                    {"station_id": station_id, "parameter": parameter},
                )
                conn.commit()

        return

    def insert(
        self,
        station_id: str,
        longitude: float,
        latitude: float,
        parameters: list,
        dates: list,
        values: list,
    ) -> None:
        """Insert (many) values for a parameter at a given station (and its location)
        into the db.

        :param station_id: Station id
        :type station_id: str
        :param longitude: Geographical longitude
        :type longitude: float
        :param latitude: Geographical latitude
        :type latitude: float
        :param parameters: List of parameters (actually just repeating the
        same parameter!)
        :type parameters: list
        :param dates: list of the dates to insert (datetime.datetimes)
        :type dates: list
        :param values: list of values to insert (floats)
        :type values: list
        """
        logger.critical("Inserting data for %s %s", station_id, parameters[0])

        self.create_station_table(station_id, longitude, latitude)
        self.create_variable_table_entry(station_id, parameters[0])

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

    def _get(
        self,
        station_id: str,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        parameter: str,
    ) -> list[float]:
        """Get value for a station at a given date.

        :param station_id: Station id
        :type station_id: str
        :param start_date: Start date
        :type start_date: datetime.datetime
        :param end_date: End date
        :type end_date: datetime.datetime
        :param parameter: Parameter
        :type parameter: str
        :raises ValueError: Station id unknown
        :return: Value for given date and parameter
        :rtype: float
        """
        if station_id not in self.metadata.tables:
            raise ValueError(f"Unknown station ID {station_id}")

        tbl = self.metadata.tables[station_id]

        stmt = (
            select(tbl.c.value)
            .where(tbl.c.parameter == parameter)
            .where(tbl.c.date >= start_date)
            .where(tbl.c.date <= end_date)
            .order_by(tbl.c.date)
        )

        result = []
        with self.engine.connect() as conn:
            mei = conn.execute(stmt)
            # if there are any values
            all_values = mei.all()
            if len(all_values) > 0:
                # we merge them into a list
                result = [float(x[0]) for x in all_values]

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
        """Get value of the station closest to the given space coordinates
        that actually has data.

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

        data = self.get_range(
            longitude, latitude, date, date + datetime.timedelta(days=1), parameter
        )
        if len(data) < 1:
            return np.nan
        else:
            return data[0]

    def get_range(
        self,
        longitude: float,
        latitude: float,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        parameter: str,
    ) -> list[float]:
        """Get value of the station closest to the given space coordinates
        that actually has data.

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

        data = []
        found = False
        while not found and len(station_ids_to_check) > 0:
            station_id = station_ids_to_check.pop(0)
            data = self._get(station_id, start_date, end_date, parameter)

            if data:
                found = True
                break

        if not found:
            logger.debug(
                "Nothing found for %s at %s, %s between %s and %s",
                parameter,
                longitude,
                latitude,
                start_date.isoformat(),
                end_date.isoformat(),
            )

        return data
