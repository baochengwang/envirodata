"""Base (blueprint) Envirodata service for datasets."""

import abc
import datetime
import logging
from collections import OrderedDict
import copy
import os
from dataclasses import dataclass, field, fields
import yaml
import json
from typing import Any

import confuse  # type: ignore
import numpy as np
import timezonefinder
from pytz import timezone, utc
from pytz.exceptions import UnknownTimeZoneError

from envirodata.utils.general import load_callable
from envirodata.utils.statistics import AvailableStatistics, Statistic

logger = logging.getLogger(__name__)


TF = timezonefinder.TimezoneFinder()


@dataclass
class Variable:
    name: str
    long_name: str
    description: str
    units: str
    statistics: list[Statistic] = field(default_factory=list)

    @property
    def metadata(self) -> dict[str, Any]:
        return {f.name: getattr(self, f.name) for f in fields(self)}

    @property
    def metadata_serialized(self):
        _items = {}
        for name, value in self.metadata.items():
            try:
                # metadata should be JSON serializable
                # if it is not - just ignore??
                _ = json.dumps(value)
                _items[name] = value
            except TypeError:
                pass
        return _items

    def __post_init__(self) -> None:
        _statistics = []
        for s in self.statistics:
            if not isinstance(s, Statistic):
                _new_stat = None
                for available_stat in AvailableStatistics:
                    if available_stat.name == s:
                        _new_stat = available_stat
                if not _new_stat:
                    raise ValueError("Statistic <{s}> is unknown!")
                else:
                    _statistics.append(_new_stat)
            else:
                _statistics.append(s)
        self.statistics = _statistics


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

        return is_valid_subclass

    @property
    @abc.abstractmethod
    def time_resolution(self) -> datetime.timedelta:
        """Time resolution of the dataset."""
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

        # stupid JSON encoder shortcoming: can't encode int64
        if isinstance(result, np.int64):
            result = int(result)

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
        variable: Variable,
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

        for statistic in variable.statistics:
            new_start_date, new_end_date = self._get_statistics_time_range(
                statistic, date, tz
            )
            start_date = min(start_date, new_start_date)
            end_date = max(end_date, new_end_date)

        # load data
        _times, _values = self._get_range(
            start_date, end_date, longitude, latitude, variable.name
        )

        times = np.array(_times)
        values = np.array(_values)

        logger.debug(variable)

        # get all statistics (the current value is also a "statistic")
        for statistic in variable.statistics:
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
        self.variables: list[Variable] = self._load_variables(config["variables"])

        self._loader: None | BaseLoader = None
        self._loader_config = config["input"]

        self._getter: None | BaseGetter = None
        self._getter_config = config["output"]

    def _load_variables(self, variable_path) -> list[Variable]:
        _variables: list[Variable] = []
        for variable_config_fname in os.listdir(variable_path):
            variable_config_fpath = os.path.join(variable_path, variable_config_fname)
            variable_config = yaml.load(
                open(variable_config_fpath, "rb"), yaml.SafeLoader
            )
            _new_variable = Variable(**variable_config)
            _variables.append(_new_variable)

        return _variables

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
    ) -> dict[str, dict]:
        """Retrieve values for (a subset of) the variables in this dataset at
        a given point in time and space.

        :param date: Date to retrieve
        :type date: datetime.datetime
        :param longitude: Geographical longitude
        :type longitude: float
        :param latitude: Geographical latitude
        :type latitude: float
        :return: Values of all requested variables, and metadata for each variable
        :rtype: dict[str, dict]
        """
        if self._getter is None:
            output_class = load_callable(self._getter_config["module"], "Getter")
            self._getter = output_class(**self._getter_config["config"])

        return {
            "values": {
                variable.name: self._getter.get(date, longitude, latitude, variable)
                for variable in self.variables
            },
            "metadata": {
                variable.name: variable.metadata for variable in self.variables
            },
        }
