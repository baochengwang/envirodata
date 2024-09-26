import logging
import datetime
from typing import Any

import requests  # type: ignore
import numpy as np

from envirodata.services.base import (
    BaseLoader,
    BaseGetter,
)

logger = logging.getLogger(__name__)


class Loader(BaseLoader):
    def __init__(
        self,
    ) -> None:
        """Load DWD dataset."""

    def load(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
    ) -> None:
        """Load DWD all data between given dates. Ignored here as we get directly
        through BrightSky API calls.

        :param start_date: First date to load
        :type start_date: datetime.datetime
        :param end_date: Last date to load
        :type end_date: datetime.datetime
        """

        logger.info("Will use BrightSky docker container directly.")


class Getter(BaseGetter):
    """Get values from dataset."""

    def __init__(self, api_url: str, statistics: dict[str, list[str]]) -> None:
        """_summary_

        :param api_url: BrightSky weather API endpoint URI
        :type api_url: str
        """
        self.api_url = api_url
        self._variable_statistics = statistics

    @property
    def time_resolution(self):
        """Time resolution of the dataset."""
        return datetime.timedelta(hours=1)

    @property
    def variable_statistics(self) -> dict[str, list[str]]:
        """Statistics to be calculated for a given variable."""
        return self._variable_statistics

    def _load_json_from_api(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        longitude: float,
        latitude: float,
    ) -> Any:
        """Call BrightSky API and retrieve DWD data.

        :param start_date: First date to load
        :type start_date: datetime.datetime
        :param end_date: Last date to load
        :type end_date: datetime.datetime
        :param longitude: Geographical longitude
        :type longitude: float
        :param latitude: Geographical latitude
        :type latitude: float
        :raises IOError: Error in calling the API
        :raises IOError: Error in decoding API response
        :return: API response as json
        :rtype: Any
        """
        params = {
            "lat": str(latitude),
            "lon": str(longitude),
            "date": start_date.isoformat(sep="T"),
            "last_date": end_date.isoformat(sep="T"),
            "tz": "Etc/UTC",
            "units": "si",
        }

        try:
            response = requests.get(self.api_url, params=params, timeout=10)
            response.raise_for_status()
        except requests.exceptions.RequestException as exc:
            logger.critical(
                "Could not get data for %s - %s: %s (%s)",
                start_date.isoformat,
                end_date.isoformat,
                response.reason,
                str(response.status_code),
            )
            raise IOError from exc

        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError as exc:
            logger.critical("Could not decode response: %s", str(exc))
            raise IOError from exc

        return data

    def _get_range(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        longitude: float,
        latitude: float,
        variable: str,
    ) -> tuple[list[datetime.datetime], list[float]]:
        """Get value for variable out of cache DB

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

        _start_date = start_date - self.time_resolution / 2.0
        _end_date = end_date + self.time_resolution / 2.0

        data = self._load_json_from_api(_start_date, _end_date, longitude, latitude)

        result = []
        times = []

        if "weather" in data:
            for step_data in data["weather"]:
                if variable in step_data:
                    value = step_data[variable]
                    try:
                        result.append(float(value))
                        times.append(
                            datetime.datetime.fromisoformat(step_data["timestamp"])
                        )
                    except ValueError:
                        logger.debug("Could not cast result for %s as float", variable)
                    except TypeError:
                        logger.debug("Could not cast result for %s as float", variable)
                    except OverflowError:
                        logger.debug("Could not cast result for %s as float", variable)

        return times, result

    def _get(
        self,
        date: datetime.datetime,
        longitude: float,
        latitude: float,
        variable: str,
    ) -> tuple[datetime.datetime, float]:
        """Get value for variable out of cache DB

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

        times, data = self._get_range(date, date, longitude, latitude, variable)

        if len(data) > 0:
            return times[0], data[0]
        else:
            return date, np.nan
