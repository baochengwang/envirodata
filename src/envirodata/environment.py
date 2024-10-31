"""Main interface to environmental factors"""

import logging
from collections import OrderedDict
import datetime

import confuse  # type: ignore

from envirodata.services.base import Service

logger = logging.getLogger()


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

    def load(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        servicenames=None,
    ) -> None:
        """Load (download, cache) all environmental factor data between start date
        and end date.

        :param start_date: First date to load
        :type start_date: datetime.datetime
        :param end_date: Last date to load
        :type end_date: datetime.datetime
        """
        if servicenames is None:
            servicenames = list(self.services.keys())

        for servicename, service in self.services.items():
            if servicename in servicenames:
                logger.info("Loading data for service %s", servicename)
                service.load(start_date, end_date)

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
                logger.debug("Loaded data for %s", servicename)
            except Exception as exc:
                logger.critical(
                    "Could not retrieve data for service %s: %s",
                    servicename,
                    str(exc),
                )

        return result
