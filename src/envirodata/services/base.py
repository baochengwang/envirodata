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
    name: str
    begin: datetime.timedelta
    end: datetime.timedelta
    function: Callable
    aggregate: str = "None"


DayAverage = Statistic(
    "day_average",
    datetime.timedelta(days=0),
    datetime.timedelta(days=1),
    np.nanmean,
    "daily",
)
DayMinimum = Statistic(
    "day_minimum",
    datetime.timedelta(days=0),
    datetime.timedelta(days=1),
    np.nanmin,
    "daily",
)
DayMaximum = Statistic(
    "day_maximum",
    datetime.timedelta(days=0),
    datetime.timedelta(days=1),
    np.nanmax,
    "daily",
)
WeekAverage = Statistic(
    "week_average",
    datetime.timedelta(days=0),
    datetime.timedelta(days=7),
    np.nanmean,
    "weekly",
)
SevenDayAverage = Statistic(
    "7day_average",
    datetime.timedelta(days=-7),
    datetime.timedelta(days=0),
    np.nanmean,
)
ThirtyDayAverage = Statistic(
    "30day_average",
    datetime.timedelta(days=-30),
    datetime.timedelta(days=0),
    np.nanmean,
)


class BaseGetter(metaclass=abc.ABCMeta):
    @classmethod
    def __subclasshook__(cls, subclass):
        is_valid_subclass = True

        # need a _get method!
        is_valid_subclass &= hasattr(subclass, "_get")
        is_valid_subclass &= callable(subclass._get)
        is_valid_subclass &= NotImplemented

        is_valid_subclass &= hasattr(subclass, "time_resolution")

        return is_valid_subclass

    @property
    @abc.abstractmethod
    def time_resolution(self) -> datetime.timedelta:
        raise NotImplementedError

    @property
    def statistics(self) -> list[Statistic]:
        return [
            DayMinimum,
            DayAverage,
            DayMaximum,
            SevenDayAverage,
            ThirtyDayAverage,
        ]

    @abc.abstractmethod
    def _get(
        self,
        date: datetime.datetime,
        longitude: float,
        latitude: float,
        variable: str,
    ) -> float:
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
        raise NotImplementedError

    def _get_range(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        longitude: float,
        latitude: float,
        variable: str,
    ):
        values: list[float] = []
        current_date = start_date

        while current_date < end_date:
            values.append(self._get(current_date, longitude, latitude, variable))
            current_date += self.time_resolution

        return values

    def _calc_statistics(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        longitude: float,
        latitude: float,
        variable: str,
        aggregation_function: Callable,
    ) -> float:

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

        result = {}

        result["actual"] = self._get(date, longitude, latitude, variable)

        for statistic in self.statistics:
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

            result[statistic.name] = self._calc_statistics(
                start_date,
                end_date,
                longitude,
                latitude,
                variable,
                statistic.function,
            )

        return result
