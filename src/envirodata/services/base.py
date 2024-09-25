"""Base (blueprint) Envirodata service for datasets."""

import logging
import datetime
from typing import Callable
from dataclasses import dataclass
import copy
import numpy as np

import abc

logger = logging.getLogger(__name__)


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


@dataclass
class Statistic:
    """Parameters of a statistic to be calculated for a given variable.

    :param name: (Arbitrary) name of this statistic, used in config.
    :type name: str
    :param begin: Start of the statistics period (e.g., yesterday)
    :type begin: datetime.timedelta
    :param end: End time of the statistics period (e.g., today)
    :type end: datetime.timedelta
    :param function: Function to apply over the retrieved data (e.g., np.nanmean)
    :type function: Callable
    :param aggregate: Method to base the date of the statistics period on (e.g., daily,
    weekly)
    :type aggregate: str
    """

    name: str
    begin: datetime.timedelta
    end: datetime.timedelta
    function: Callable
    aggregate: str = "None"


AvailableStatistics = [
    Statistic(
        "day_mean",
        datetime.timedelta(days=0),
        datetime.timedelta(days=1),
        np.nanmean,
        "daily",
    ),
    Statistic(
        "day_minimum",
        datetime.timedelta(days=0),
        datetime.timedelta(days=1),
        np.nanmin,
        "daily",
    ),
    Statistic(
        "day_maximum",
        datetime.timedelta(days=0),
        datetime.timedelta(days=1),
        np.nanmax,
        "daily",
    ),
    Statistic(
        "day_sum",
        datetime.timedelta(days=0),
        datetime.timedelta(days=1),
        np.nansum,
        "daily",
    ),
    Statistic(
        "week_mean",
        datetime.timedelta(days=0),
        datetime.timedelta(days=7),
        np.nanmean,
        "weekly",
    ),
    Statistic(
        "7day_mean",
        datetime.timedelta(days=-7),
        datetime.timedelta(days=0),
        np.nanmean,
    ),
    Statistic(
        "7day_sum",
        datetime.timedelta(days=-7),
        datetime.timedelta(days=0),
        np.nansum,
    ),
    Statistic(
        "30day_mean",
        datetime.timedelta(days=-30),
        datetime.timedelta(days=0),
        np.nanmean,
    ),
    Statistic(
        "30day_sum",
        datetime.timedelta(days=-30),
        datetime.timedelta(days=0),
        np.nansum,
    ),
]


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
    ) -> float:
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
    ):
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
        current_date = start_date

        while current_date < end_date:
            values.append(self._get(current_date, longitude, latitude, variable))
            current_date += self.time_resolution

        return values

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
                for available_stat in AvailableStatistics:
                    if available_stat.name == stat:
                        stats_to_be_calculated.append(available_stat)

        return stats_to_be_calculated

    def _calc_statistic(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        longitude: float,
        latitude: float,
        variable: str,
        aggregation_function: Callable,
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

        values = self._get_range(start_date, end_date, longitude, latitude, variable)

        if not any([np.isfinite(x) for x in values]):
            return np.nan

        result = aggregation_function(values)

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

        result["actual"] = self._get(date, longitude, latitude, variable)

        for statistic in self._get_statistics_for_variable(variable):
            # is the beginning of the statistics period
            valid_date = copy.copy(date)

            if statistic.aggregate == "daily":
                # start with 00:00 of the day requested
                valid_date = valid_date.replace(hour=0, minute=0, second=0)

            if statistic.aggregate == "weekly":
                # start with Monday, 00:00 of the week requested
                valid_date = valid_date.replace(hour=0, minute=0, second=0)
                while valid_date.weekday() != 0:
                    valid_date -= datetime.timedelta(days=1)

            start_date = valid_date + statistic.begin
            end_date = valid_date + statistic.end

            result[statistic.name] = self._calc_statistic(
                start_date,
                end_date,
                longitude,
                latitude,
                variable,
                statistic.function,
            )

        return result
