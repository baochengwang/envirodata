"""Main interface to environmental factors"""

import logging
from collections import OrderedDict
import datetime

import confuse  # type: ignore

from envirodata.utils.general import load_callable
from envirodata.services.base import BaseGetter, BaseLoader

logger = logging.getLogger()


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


class Environment:
    """Environmental factors interface"""

    def __init__(self, config: dict | OrderedDict | confuse.Configuration) -> None:
        self.services: dict[str, Service] = {}
        self.register_services(config["services"])

    def register_services(
        self, config: dict | OrderedDict | confuse.Configuration
    ) -> None:
        """Register environmental factor services (read: datasets)
        with the main interface.

        :param config: Configuration of the service
        :type config: dict | OrderedDict | confuse.Configuration
        """
        for service_config in config:
            logger.info("Registered service %s", service_config["label"])
            self.services[service_config["label"]] = Service(service_config)
        # self.services["DWD"] = Service(config[0])

    def load(self, start_date: datetime.datetime, end_date: datetime.datetime) -> None:
        """Load (download, cache) all environmental factor data between start date
        and end date.

        :param start_date: First date to load
        :type start_date: datetime.datetime
        :param end_date: Last date to load
        :type end_date: datetime.datetime
        """
        for servicename, service in self.services.items():
            logger.info("Loading data for service %s", servicename)
            service.load(start_date, end_date)
        # self.services["DWD"].load(start_date, end_date)

    def get(
        self,
        date: datetime.datetime,
        longitude: float,
        latitude: float,
        variables: list | None = None,
    ) -> dict:
        """Retrieve values for (a subset of) all known variables at
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
        result = {}
        for servicename, service in self.services.items():
            try:
                result[servicename] = service.get(
                    date,
                    longitude,
                    latitude,
                    variables=variables,
                )
                logger.debug(f"Done with {servicename}")
            except Exception as exc:
                logger.critical(
                    "Could not retrieve data for service %s: %s",
                    servicename,
                    str(exc),
                )
        return result
