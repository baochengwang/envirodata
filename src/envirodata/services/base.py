"""Base (blueprint) Envirodata service for datasets."""

import logging
import datetime
from typing import Callable
import copy
import numpy as np
import timezonefinder
from pytz import timezone, utc
from pytz.exceptions import UnknownTimeZoneError

import abc

from envirodata.utils.statistics import Statistic, AvailableStatistics

logger = logging.getLogger(__name__)


TF = timezonefinder.TimezoneFinder()


class BaseLoader(metaclass=abc.ABCMeta):
    @classmethod
    def __subclasshook__(cls, subclass):
        return hasattr(subclass, "load") and callable(subclass.load) and NotImplemented

    @abc.abstractmethod
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
        raise NotImplementedError


class BaseGetter(metaclass=abc.ABCMeta):
    @classmethod
    def __subclasshook__(cls, subclass):
        is_valid_subclass = True

        # need a _get method!
        is_valid_subclass &= hasattr(subclass, "_get")
        is_valid_subclass &= callable(subclass._get)
        is_valid_subclass &= NotImplemented

        is_valid_subclass &= hasattr(subclass, "time_resolution")
        is_valid_subclass &= hasattr(subclass, "variable_statistics")

        return is_valid_subclass

    @property
    @abc.abstractmethod
    def time_resolution(self) -> datetime.timedelta:
        """Time resolution of the dataset."""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def variable_statistics(self) -> dict[str, list[str]]:
        """Statistics to be calculated for a given variable."""
        raise NotImplementedError

    @abc.abstractmethod
    def _get(
        self,
        date: datetime.datetime,
        longitude: float,
        latitude: float,
        variable: str,
    ) -> tuple[datetime.datetime, float]:
        """Get value for variable out of the (cached) input dataset
        for a given place in time and space (internal)

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
        raise NotImplementedError

    def _get_range(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        longitude: float,
        latitude: float,
        variable: str,
    ) -> tuple[list[datetime.datetime], list[float]]:
        """Get value for variable out of the (cached) input dataset
        for a given period in time and space (internal)

        :param start_date: First date to retrieve
        :type start_date: datetime.datetime
        :param end_date: Last date to retrieve
        :type end_date: datetime.datetime
        :param longitude: Geographical longitude
        :type longitude: float
        :param latitude: Geographical latitude
        :type latitude: float
        :param variable: Variable to retrieve
        :type variable: str
        :return: Value for variable at given point in time and space.
        :rtype: float
        """
        values: list[float] = []
        times: list[datetime.datetime] = []
        current_date = start_date

        while current_date < end_date:
            new_times, new_values = self._get(
                current_date, longitude, latitude, variable
            )
            values.append(new_values)
            times.append(new_times)
            current_date += self.time_resolution

        return times, values

    def _get_statistics_for_variable(self, variable: str) -> list[Statistic]:
        """Get statistics object for a given variable.

        :param variable: Variable to get statistics information
        :type variable: str
        :return: List of statistics to calculate
        :rtype: list[Statistic]
        """
        stats_to_be_calculated = []
        if variable in self.variable_statistics:
            for stat in self.variable_statistics[variable]:
                found = False
                for available_stat in AvailableStatistics:
                    if available_stat.name == stat:
                        stats_to_be_calculated.append(available_stat)
                        found = True
                        break
                if not found:
                    logger.critical(
                        "Unknown statistic %s requested for %s.", stat, variable
                    )

        return stats_to_be_calculated

    def _calc_statistic(
        self,
        date: datetime.datetime,
        longitude: float,
        latitude: float,
        variable: str,
        statistic: Statistic,
        tz=utc,
    ) -> float:
        """Calculate a statistic for a given variable at a given place over a given
        time period

        :param start_date: First date to retrieve
        :type start_date: datetime.datetime
        :param end_date: Last date to retrieve
        :type end_date: datetime.datetime
        :param longitude: Geographical longitude
        :type longitude: float
        :param latitude: Geographical latitude
        :type latitude: float
        :param variable: Variable to retrieve
        :type variable: str
        :param aggregation_function: Function to apply (e.g., np.nanmean)
        :type aggregation_function: Callable
        :return: Aggregate value
        :rtype: float
        """

        # our input has to be in UTC
        assert date.tzinfo is not None
        assert date.tzinfo == utc

        # UTC bounds
        start_date = date + statistic.begin
        end_date = date + statistic.end

        # in case we request "daily" statistics:
        # "Daily" refers to 0 - 23:59 LT.
        # Hence, rebase start_date and end_date (UTC) to
        # 00:00 LT of the day of start_date and end_date, respectively.
        if statistic.daily:
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            start_date -= tz.utcoffset(start_date.replace(tzinfo=None))

            end_date = end_date.replace(hour=23, minute=59, second=59)
            end_date -= tz.utcoffset(end_date.replace(tzinfo=None))

        # times are in UTC
        times, values = self._get_range(
            start_date, end_date, longitude, latitude, variable
        )

        if not any([np.isfinite(x) for x in values]):
            return np.nan

        assert times[0].tzinfo is not None
        assert (times[0].tzinfo == utc) or (times[0].tzinfo == datetime.timezone.utc)

        # localize times to tz of location and make naive,
        # statistics will be calculated in LT - is easier.
        times_local = [t.astimezone(tz).replace(tzinfo=None) for t in times]

        result = statistic.function(times_local, values)

        logger.debug(
            statistic.name,
            len(times_local),
            len(values),
            np.nanmin(values),
            np.nanmean(values),
            np.nanmax(values),
        )
        logger.debug(values)

        logger.debug(" > ")

        logger.debug(result)

        logger.debug("-----")

        return result

    def get(
        self,
        date: datetime.datetime,
        longitude: float,
        latitude: float,
        variable: str,
    ) -> dict:
        """Get value for variable out of the input dataset
        for a given place in time and space

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
        result = {}

        # find time zone for location
        tzname = TF.timezone_at(lng=longitude, lat=latitude)
        if tzname is None:
            raise UnknownTimeZoneError
        try:
            tz = timezone(tzname)
        except UnknownTimeZoneError as exc:
            raise UnknownTimeZoneError from exc

        for statistic in self._get_statistics_for_variable(variable):
            result[statistic.name] = self._calc_statistic(
                date,
                longitude,
                latitude,
                variable,
                statistic,
                tz=tz,
            )

        return result
