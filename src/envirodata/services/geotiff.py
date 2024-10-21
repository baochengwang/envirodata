"""Base (blueprint) Envirodata service for datasets."""

import logging
import os
import datetime
from collections import OrderedDict
import shutil

import numpy as np
import rasterio
from pyproj import Transformer

from envirodata.services.base import BaseLoader, BaseGetter

logger = logging.getLogger(__name__)

TIME_RESOLUTION = datetime.timedelta(hours=1)


class Loader(BaseLoader):
    """Load dataset."""

    def __init__(
        self,
        data_table: dict | OrderedDict,
        cache_path: str,
    ) -> None:
        """Load dataset into local cache.

        :param data_table: Dict of variable names (key) and path to corresponding GeoTIFF (value).
        :type data_table: dict | OrderedDict
        :param cache_path: Path to data cache
        :type cache_path: str | pathlib.Path

        """
        self.data_table = data_table
        self.cache_path = cache_path

        os.makedirs(self.cache_path, exist_ok=True)

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
        for variable, input_path in self.data_table.items():
            output_path = os.path.join(self.cache_path, variable + ".tif")
            if os.path.exists(input_path):
                shutil.copy(input_path, output_path)
            else:
                logger.critical(
                    f"Input path {input_path} for variable {variable} does not exist."
                )


class Getter(BaseGetter):
    """Get values from cached dataset."""

    def __init__(
        self,
        cache_path,
        statistics: dict[str, list[str]],
        output_crs="EPSG:4326",
    ):
        """Get values from cached dataset.

        :param cache_path: Path to data cache
        :type cache_path: str | pathlib.Path
        :param output_crs: pyproj string describing output CRS, defaults to "EPSG:4326"

        """

        def read(fname):
            dset = rasterio.open(fname)
            return (dset, dset.read(1))

        self.data = {
            x.replace(".tif", ""): read(os.path.join(cache_path, x))
            for x in os.listdir(cache_path)
            if x.endswith(".tif")
        }

        self.transformers = {
            name: Transformer.from_crs(output_crs, data[0].crs, always_xy=True)
            for name, data in self.data.items()
        }

        self._variable_statistics = statistics

    @property
    def time_resolution(self):
        """Time resolution of the dataset."""
        return datetime.timedelta(hours=1)

    @property
    def variable_statistics(self) -> dict[str, list[str]]:
        """Statistics to be calculated for a given variable."""
        return self._variable_statistics

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
        x, y = self.transformers[variable].transform(longitude, latitude)

        row, col = self.data[variable][0].index(x, y)

        logger.debug(
            "%d, %d -> %d, %d -> %d, %d (dataset: %s)",
            longitude,
            latitude,
            x,
            y,
            row,
            col,
            self.data[variable][0].shape,
        )

        # if we are sampling outside the raster bounds, return NaN
        value = np.nan
        if (
            row < 0
            or row >= self.data[variable][0].shape[0]
            or col < 0
            or col >= self.data[variable][0].shape[1]
        ):
            logger.debug("Out of bounds sampling for %s!", variable)
        else:
            logger.debug("Valid sampling for %s!", variable)
            value = float(self.data[variable][1][row, col])

        return date, value
