"""Base (blueprint) Envirodata service for datasets."""

import abc
import datetime
import logging
from collections import OrderedDict
import copy

import confuse  # type: ignore
import numpy as np
import timezonefinder
from pytz import timezone, utc
from pytz.exceptions import UnknownTimeZoneError

from envirodata.utils.general import load_callable
from envirodata.utils.statistics import AvailableStatistics, Statistic

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

        while current_date <= end_date:
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
        all_times: np.ndarray[datetime.datetime],
        all_values: np.ndarray[float],
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

        start_date, end_date = self._get_statistics_time_range(statistic, date, tz)

        valid_idx = np.logical_and(all_times >= start_date, all_times <= end_date)

        times = all_times[valid_idx]
        values = all_values[valid_idx]

        if not np.any(np.isfinite(values)):
            return np.nan

        assert times[0].tzinfo is not None
        assert (times[0].tzinfo == utc) or (times[0].tzinfo == datetime.timezone.utc)

        # localize times to tz of location and make naive,
        # statistics will be calculated in LT - is easier.
        times_local = [t.astimezone(tz).replace(tzinfo=None) for t in times]

        logger.debug(statistic.name)
        result = statistic.function(times_local, values)

        return result

    def _get_statistics_time_range(
        self,
        statistic: Statistic,
        date: datetime.datetime,
        tz,
    ) -> tuple[datetime.datetime, datetime.datetime]:
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

        return start_date, end_date

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

        # our input has to be in UTC
        assert date.tzinfo is not None
        assert date.tzinfo == utc

        # find time zone for location
        tzname = TF.timezone_at(lng=longitude, lat=latitude)
        if tzname is None:
            raise UnknownTimeZoneError
        try:
            tz = timezone(tzname)
        except UnknownTimeZoneError as exc:
            raise UnknownTimeZoneError from exc

        # get max time range needed for statistics
        start_date = copy.copy(date)
        end_date = copy.copy(date)

        for statistic in self._get_statistics_for_variable(variable):
            new_start_date, new_end_date = self._get_statistics_time_range(
                statistic, date, tz
            )
            start_date = min(start_date, new_start_date)
            end_date = max(end_date, new_end_date)

        # load data
        _times, _values = self._get_range(
            start_date, end_date, longitude, latitude, variable
        )

        times = np.array(_times)
        values = np.array(_values)

        logger.debug(variable)

        # get all statistics (the current value is also a "statistic")
        for statistic in self._get_statistics_for_variable(variable):
            result[statistic.name] = self._calc_statistic(
                date,
                times,
                values,
                statistic,
                tz=tz,
            )

        return result


class Service:
    """An environmental factors service providing one or several
    variables from a common source dataset."""

    def __init__(self, config: dict | OrderedDict | confuse.Configuration) -> None:
        """A service is created based on the provided configuration.
        A service is a python module with methods to load and get variable data.

        :param config: Configuration of the service, needs to contain
        information on input and output config.
        :type config: dict | OrderedDict | confuse.Configuration
        """
        self.variables: list[str] = config["variables"]

        self._loader: None | BaseLoader = None
        self._loader_config = config["input"]

        self._getter: None | BaseGetter = None
        self._getter_config = config["output"]

    def load(self, start_date: datetime.datetime, end_date: datetime.datetime) -> None:
        """Load / cache data for this service.

        :param start_date: Beginning of time period to load.
        :type start_date: datetime.datetime
        :param end_date: End of time period to load.
        :type end_date: datetime.datetime
        """
        if self._loader is None:
            input_class = load_callable(self._loader_config["module"], "Loader")
            self._loader = input_class(**self._loader_config["config"])

        self._loader.load(start_date, end_date)

    def get(
        self,
        date: datetime.datetime,
        longitude: float,
        latitude: float,
        variables: list | None = None,
    ) -> dict:
        """Retrieve values for (a subset of) the variables in this dataset at
        a given point in time and space.

        :param date: Date to retrieve
        :type date: datetime.datetime
        :param longitude: Geographical longitude
        :type longitude: float
        :param latitude: Geographical latitude
        :type latitude: float
        :param variables: List of variables, defaults to all variables known
        :type variables: list, optional
        :return: Values of all requested variables
        :rtype: dict
        """
        if self._getter is None:
            output_class = load_callable(self._getter_config["module"], "Getter")
            self._getter = output_class(**self._getter_config["config"])

        if not variables:
            variables = self.variables

        variables = [v for v in variables if v in self.variables]

        return {
            variable: self._getter.get(date, longitude, latitude, variable)
            for variable in variables
        }
