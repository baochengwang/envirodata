"""Envirodata service for Copernicus cdsapi datasets."""

import os
import logging
import datetime
import copy
from typing import Any

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
        dataset_start_date: datetime.datetime | str = datetime.datetime(
            1, 1, 1, tzinfo=datetime.timezone.utc
        ),
        dataset_end_date: datetime.datetime | str = datetime.datetime.now(
            datetime.timezone.utc
        ),
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

        if not isinstance(dataset_start_date, datetime.datetime):
            dataset_start_date = datetime.datetime.fromisoformat(dataset_start_date)

        if dataset_start_date.tzinfo is None:
            dataset_start_date = dataset_start_date.replace(
                tzinfo=datetime.timezone.utc
            )

        if not isinstance(dataset_end_date, datetime.datetime):
            dataset_end_date = datetime.datetime.fromisoformat(dataset_end_date)

        if dataset_end_date.tzinfo is None:
            dataset_end_date = dataset_end_date.replace(tzinfo=datetime.timezone.utc)

        self.dataset_start_date = dataset_start_date
        self.dataset_end_date = dataset_end_date

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
        # iterate monthly
        cur_date = start_date.replace(day=1, hour=0, minute=0)
        while cur_date <= end_date:
            if (
                cur_date <= self.dataset_end_date
                and cur_date >= self.dataset_start_date
            ):
                try:
                    self._download_date(cur_date)
                except IOError as exc:
                    logger.info(
                        f"Could not download data for {cur_date.strftime('%Y-%m')}: {exc}"
                    )
            # beginning of next month...
            cur_date += datetime.timedelta(days=31)
            cur_date = cur_date.replace(day=1)

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
                c = cdsapi.Client(quiet=True, url=self.cdsurl, key=self.cdskey)
                request = copy.copy(self.request)

                for datevar in ["date", "year", "month", "day"]:
                    if datevar in request:
                        # get until next month...
                        end_date = date + datetime.timedelta(days=31)
                        end_date = end_date.replace(day=1) - datetime.timedelta(days=1)

                        request[datevar] = (
                            date.strftime("%Y-%m-%d")
                            + "/"
                            + end_date.strftime("%Y-%m-%d")
                        )

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
    ):
        """Get values from dataset.

        :param cache_fpath_pattern: File path pattern in cache (with
        datetime.strftime date placeholders, should match
        output_fpath_pattern in Loader)
        :type cache_fpath_pattern: str
        :param time_calculation: How to calculate date from NetCDF time variable
        (name of option in self.time_calculators)
        :type time_calculation: str
        :raises IOError: Unable to find appropriate method to compute time.
        """
        self.cache_fpath_pattern = cache_fpath_pattern

        time_calculators = {
            "time_since_analysis": self._calc_time_since_analysis,
            "hours_since_epoch": self._calc_time_epoch,
        }
        if time_calculation not in time_calculators:
            raise ValueError("Unknown method to compute time.")
        self.calc_time = time_calculators[time_calculation]

        self.lons = None
        self.lats = None

    @property
    def time_resolution(self):
        """Time resolution of the dataset."""
        return datetime.timedelta(hours=1)

    def _calc_time_since_analysis(
        self, date: datetime.datetime, nc: netCDF4.Dataset  # pylint: disable=no-member
    ) -> list[datetime.datetime]:
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
    ) -> datetime.datetime | np.ndarray[Any, np.dtype[np.object_]]:
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

    def _get_lons_lats(self, nc):
        if self.lons is None:
            self.lons, self.lats = np.meshgrid(
                nc.variables["longitude"], nc.variables["latitude"]
            )  # pylint: disable=unsubscriptable-object
        return self.lons, self.lats

    def _get_from_one(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        longitude: float,
        latitude: float,
        variable: str,
    ) -> tuple[list[datetime.datetime], list[float]]:
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

        output_fname = start_date.strftime(self.cache_fpath_pattern)

        try:
            nc = netCDF4.Dataset(output_fname)  # pylint: disable=no-member
        except OSError as exc:
            logger.info(
                "No data found for {:s}!".format(start_date.strftime("%Y-%m-%d"))
            )
            raise exc

        lons, lats = self._get_lons_lats(nc)

        times = self.calc_time(start_date, nc)

        tidxes = np.where(
            np.logical_and(np.array(times) >= start_date, np.array(times) <= end_date)
        )[0]

        def _get_index(lons, lats, lon, lat):
            ddelta = (lons - lon) ** 2 + (lats - lat) ** 2
            idxes = np.where(ddelta == np.min(ddelta))
            return (idxes[0][0], idxes[1][0])

        xidx, yidx = _get_index(lons, lats, longitude, latitude)

        chosen_times = [times[i] for i in tidxes]

        try:
            if len(nc.dimensions) == 3:
                values = [float(nc.variables[variable][i, yidx, xidx]) for i in tidxes]
                return chosen_times, values
            elif len(nc.dimensions) == 4:
                values = [
                    float(nc.variables[variable][i, 0, yidx, xidx]) for i in tidxes
                ]
                return chosen_times, values
            else:
                raise RuntimeError("Unknown number of dimensions in NetCDF file.")
        except Exception as exc:
            raise RuntimeError(f"Could not get data for {variable}") from exc

    def _get_range(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        longitude: float,
        latitude: float,
        variable: str,
    ) -> tuple[list[datetime.datetime], list[float]]:

        # start with startdate
        cur_start_date = start_date
        # until the end of the month
        cur_end_date = start_date + datetime.timedelta(days=31)
        cur_end_date = cur_end_date.replace(
            day=1, hour=0, minute=0
        ) - datetime.timedelta(hours=1)
        # or only up to the end_date if that is before end of month
        cur_end_date = cur_end_date if cur_end_date <= end_date else end_date

        times = []
        values = []
        while cur_start_date < end_date:
            logger.debug("%s %s" % (cur_start_date, cur_end_date))
            _times, _values = self._get_from_one(
                cur_start_date, cur_end_date, longitude, latitude, variable
            )

            times += _times
            values += _values

            cur_start_date = cur_start_date + datetime.timedelta(days=31)
            cur_start_date = cur_start_date.replace(day=1, hour=0, minute=0)

            cur_end_date = cur_start_date + datetime.timedelta(days=31)
            cur_end_date = cur_end_date.replace(
                day=1, hour=0, minute=0
            ) - datetime.timedelta(hours=1)
            # or only up to the end_date if that is before end of month
            cur_end_date = cur_end_date if cur_end_date <= end_date else end_date

        return times, values

    def _get(
        self,
        date: datetime.datetime,
        longitude: float,
        latitude: float,
        variable: str,
    ) -> tuple[datetime.datetime, float]:
        _dates, _values = self._get_range(date, date, longitude, latitude, variable)
        return _dates[0], _values[0]
