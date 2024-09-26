"""Envirodata service for Copernicus cdsapi datasets."""

import os
import logging
import datetime
import copy

import cdsapi  # type: ignore
import netCDF4  # type: ignore
import numpy as np

from envirodata.services.base import BaseLoader, BaseGetter

logger = logging.getLogger(__name__)


class Loader(BaseLoader):
    """Load (cache) cdsapi dataset."""

    def __init__(
        self,
        dataset: str,
        request: dict,
        output_fpath_pattern: str,
        cdsurl: str | None = os.environ.get("CDSAPI_URL"),
        cdskey: str | None = os.environ.get("CDSAPI_KEY"),
    ) -> None:
        """Load (cache) cdsapi dataset.

        :param dataset: name of the dataset in cdsapi
        :type dataset: str
        :param request: request parameters (from ADS/CDS)
        :type request: dict
        :param output_fpath_pattern: path to output file (including strftime
        date placeholders)
        :type output_fpath_pattern: str
        :param cdsurl: cdsapi url, defaults to os.environ.get("CDSAPI_URL")
        :type cdsurl: str, optional
        :param cdskey: cdsapi key, defaults to os.environ.get("CDSAPI_KEY")
        :type cdskey: str, optional
        """
        self.dataset = dataset
        self.request = request
        self.output_fpath_pattern = output_fpath_pattern
        self.cdsurl = cdsurl
        self.cdskey = cdskey

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
        cur_date = start_date
        while cur_date <= end_date:
            self._download_date(cur_date)
            cur_date += datetime.timedelta(days=1)

    def _download_date(
        self,
        date: datetime.datetime,
    ) -> None:
        """Load (cache) data for a single date.

        :param date: Date to load
        :type date: datetime.datetime
        :raises IOError: Downloading data failed.
        """
        output_fname = date.strftime(self.output_fpath_pattern)

        output_dir = os.path.dirname(output_fname)
        os.makedirs(output_dir, exist_ok=True)

        if not os.path.exists(output_fname):
            try:
                c = cdsapi.Client(url=self.cdsurl, key=self.cdskey)
                request = copy.copy(self.request)

                for datevar in ["date", "year", "month", "day"]:
                    if datevar in request:
                        request[datevar] = date.strftime(request[datevar])

                logger.info(
                    "Downloading from %s for %s", self.dataset, date.isoformat()
                )

                c.retrieve(
                    self.dataset,
                    request,
                    output_fname,
                )
            except Exception as exc:
                logger.critical(
                    "Could not download new data for %s, continuing anyway: %s",
                    date.isoformat(),
                    exc,
                )

        try:
            nc = netCDF4.Dataset(output_fname)  # pylint: disable=no-member
            nc.close()
        except Exception as exc:
            raise IOError(f"No data found for {date.isoformat()}!") from exc


class Getter(BaseGetter):
    """Get values from dataset."""

    def __init__(
        self,
        cache_fpath_pattern: str,
        time_calculation: str,
        variable_translation_table: dict,
        statistics: dict[str, list[str]],
    ):
        """Get values from dataset.

        :param cache_fpath_pattern: File path pattern in cache (with
        datetime.strftime date placeholders, should match
        output_fpath_pattern in Loader)
        :type cache_fpath_pattern: str
        :param time_calculation: How to calculate date from NetCDF time variable
        (name of option in self.time_calculators)
        :type time_calculation: str
        :param variable_translation_table: Translation table from file
        variable name to name in Envirodata API.
        :type variable_translation_table: dict
        :raises IOError: Unable to find appropriate method to compute time.
        """
        self.cache_fpath_pattern = cache_fpath_pattern
        self.variable_translation_table = variable_translation_table

        time_calculators = {
            "time_since_analysis": self._calc_time_since_analysis,
            "hours_since_epoch": self._calc_time_epoch,
        }
        if time_calculation not in time_calculators:
            raise ValueError("Unknown method to compute time.")
        self.calc_time = time_calculators[time_calculation]

        self._variable_statistics = statistics

    @property
    def time_resolution(self):
        """Time resolution of the dataset."""
        return datetime.timedelta(hours=1)

    @property
    def variable_statistics(self) -> dict[str, list[str]]:
        """Statistics to be calculated for a given variable."""
        return self._variable_statistics

    def _calc_time_since_analysis(
        self, date: datetime.datetime, nc: netCDF4.Dataset  # pylint: disable=no-member
    ) -> list:
        """Calculate time from netCDF4 file as seconds since midnight of the
        current day.

        :param date: Date to process
        :type date: datetime.datetime
        :param nc: NetCDF4 file
        :type nc: netCDF4.Dataset
        :return: List of datetimes in file
        :rtype: list
        """
        return [
            date.replace(hour=0, minute=0, second=0)
            + datetime.timedelta(hours=float(x))
            for x in nc.variables["time"][:]
        ]

    def _calc_time_epoch(
        self, date: datetime.datetime, nc: netCDF4.Dataset  # pylint: disable=no-member
    ) -> list:
        """Calculate time from netCDF4 file based on time variable attributes.

        :param date: Date to process
        :type date: datetime.datetime
        :param nc: NetCDF4 file
        :type nc: netCDF4.Dataset
        :return: List of datetimes in file
        :rtype: list
        """
        return netCDF4.num2date(  # pylint: disable=no-member
            nc.variables["time"],
            nc.variables["time"].units,
            calendar=nc.variables["time"].calendar,
            only_use_cftime_datetimes=False,
        )

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
        :raises IOError: No data found
        :raises RuntimeError: Unknown number of dimensions in netCDF4 file
        :raises RuntimeError: Unable to get data
        :return: Value for variable at given point in time and space.
        :rtype: float
        """
        output_fname = date.strftime(self.cache_fpath_pattern)

        try:
            nc = netCDF4.Dataset(output_fname)  # pylint: disable=no-member
        except OSError:
            logger.info("No data found for {:s}!".format(date.strftime("%Y-%m-%d")))
            return date, np.nan

        def _get_index(lons, lats, lon, lat):
            ddelta = (lons - lon) ** 2 + (lats - lat) ** 2
            idxes = np.where(ddelta == np.min(ddelta))
            return (idxes[0][0], idxes[1][0])

        lons, lats = np.meshgrid(
            nc.variables["longitude"], nc.variables["latitude"]
        )  # pylint: disable=unsubscriptable-object

        times = self.calc_time(date, nc)

        tdeltas = [np.abs((date - t).total_seconds()) for t in times]

        tidx = np.where(tdeltas == np.min(tdeltas))[0][0]  # no idea why sub-levels
        xidx, yidx = _get_index(lons, lats, longitude, latitude)

        fvarname = self.variable_translation_table[variable]

        logger.debug(
            "%s: %s, %s, %s is at %s: %s, %s, %s",
            variable,
            date.isoformat(),
            longitude,
            latitude,
            fvarname,
            tidx,
            xidx,
            yidx,
        )

        try:
            if len(nc.dimensions) == 3:
                return date, float(nc.variables[fvarname][tidx, yidx, xidx])
            elif len(nc.dimensions) == 4:
                return date, float(nc.variables[fvarname][tidx, 0, yidx, xidx])
            else:
                raise RuntimeError("Unknown number of dimensions in NetCDF file.")
        except Exception as exc:
            raise RuntimeError(
                f"Could not get data for {variable} (file variable: {fvarname})"
            ) from exc
