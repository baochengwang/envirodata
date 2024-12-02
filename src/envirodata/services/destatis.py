"""Base (blueprint) Envirodata service for datasets."""

import logging
import os
import datetime
from sqlalchemy import (
    create_engine,
    Table,
    Column,
    Integer,
    Float,
    String,
    MetaData,
    select,
)
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy.engine import make_url
import pandas as pd
import random

import numpy as np

from envirodata.services.base import BaseLoader, BaseGetter
from envirodata.utils.general import copy_or_download
from envirodata.utils.spatial import calculate_inspire_grid_id

logger = logging.getLogger(__name__)

TIME_RESOLUTION = datetime.timedelta(hours=1)


class Loader(BaseLoader):
    """Load dataset."""

    def __init__(
        self,
        db_url: str,
        csv_paths: dict[str, str],
        separator: str = ";",
        decimal: str = ",",
        grid_id_field: str = "GITTER_ID_100m",
        cache_path: str = "cache",
    ) -> None:
        """Load dataset into local cache.

        :param csv_path: Path to input csv from Zensus
        :type csv_path: str
        :param db_url: Path to DB connection
        :type db_url: str
        :param separator: csv field separator character, defaults to ';'
        :type separator: str

        """
        self.csv_paths = csv_paths
        self.csv_separator = separator
        self.csv_decimal = decimal
        self.db_url = db_url
        self.grid_id_field = grid_id_field

        logger.debug("Memory DB located at %s", db_url)

        url = make_url(db_url)
        # something like sqlite:///from/here/data.sqlite3
        if db_url.startswith("sqlite:"):
            if url.database is not None:
                os.makedirs(os.path.dirname(url.database), exist_ok=True)

        self.cache_path = cache_path
        os.makedirs(self.cache_path, exist_ok=True)

    def _load_csv(self, variable_name: str, csv_path: str):
        tmp_path = os.path.join(
            self.cache_path, str(random.randint(10, 1000000)) + ".csv"
        )
        copy_or_download(
            csv_path,
            tmp_path,
        )

        # https://medium.com/@anusoosanbaby/efficiently-importing-csv-data-into-postgresql-using-python-and-sqlalchemy-052693aa921a
        df = pd.read_csv(tmp_path, sep=self.csv_separator, decimal=self.csv_decimal)

        try:
            os.remove(tmp_path)
        except FileNotFoundError:
            pass

        engine = create_engine(self.db_url)

        metadata = MetaData()

        def infer_sqlalchemy_type(dtype):
            """Map pandas dtype to SQLAlchemy's types"""
            if "int" in dtype.name:
                return Integer
            elif "float" in dtype.name:
                return Float
            elif "object" in dtype.name:
                return String
            else:
                return String

        columns: list[Column] = [
            Column(
                name,
                infer_sqlalchemy_type(dtype),
                primary_key=(name == self.grid_id_field),  # make grid ID a primary key
            )
            for name, dtype in df.dtypes.items()
        ]

        table = Table(variable_name, metadata, *columns)

        table.create(engine, checkfirst=True)

        df.to_sql(variable_name, con=engine, if_exists="append", index=False)

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

        for variable_name, csv_path in self.csv_paths.items():
            self._load_csv(variable_name, csv_path)


class Getter(BaseGetter):
    """Get values from cached dataset."""

    def __init__(
        self,
        db_url: str,
        grid_id_field: str = "GITTER_ID_100m",
        resolution: int = 100,
    ):
        """Get values from cached dataset.

        :param cache_path: Path to data cache
        :type cache_path: str | pathlib.Path
        :param output_crs: pyproj string describing output CRS, defaults to "EPSG:4326"

        """

        self.resolution = resolution
        self.grid_field_id = grid_id_field

        # table needs primary key for this to work!
        self.base = automap_base()

        engine = create_engine(db_url)

        self.base.prepare(autoload_with=engine)
        self.session = Session(engine)

    @property
    def time_resolution(self):
        """Time resolution of the dataset."""
        return datetime.timedelta(days=1)  # useless?

    def _get(
        self,
        date: datetime.datetime,
        longitude: float,
        latitude: float,
        variable: str,
    ) -> tuple[datetime.datetime, float]:
        """Get value for variable out of cached NetCDF4 file

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

        grid_id = calculate_inspire_grid_id(
            longitude, latitude, cell_size=self.resolution
        )

        grid_column = getattr(self.base.classes[variable], self.grid_field_id)
        var_column = getattr(self.base.classes[variable], variable)

        stmt = select(var_column).where(grid_column == grid_id)

        row = self.session.scalars(stmt).first()

        if row is not None:
            value = row
        else:
            value = np.nan

        return date, value
