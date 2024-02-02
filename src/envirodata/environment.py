import logging  # for error message reporting
import datetime

import psycopg2
from psycopg2.extras import LoggingConnection

from envirodata.utils.general import load_object

# error message
logger = logging.getLogger()


class Service:
    def __init__(self, config):
        self.variables = config["variables"]
        self.config = config

    def load(self, start_date, end_date):
        input_config = self.config["input"]
        input_method = load_object(input_config["module"], input_config["method"])

        cur_date = start_date
        while cur_date <= end_date:
            input_method(cur_date, **input_config["config"])
            cur_date += datetime.timedelta(days=1)

    def get(self, date, longitude, latitude, variables=None):
        if not variables:
            variables = self.variables

        output_config = self.config["output"]
        output_method = load_object(output_config["module"], output_config["method"])

        return {
            variable: output_method(
                date, variable, longitude, latitude, **output_config["config"]
            )
            for variable in variables
        }


class Environment:
    def __init__(self, config):
        self.conn = psycopg2.connect(
            connection_factory=LoggingConnection, **config["database"]
        )
        self.conn.initialize(logger)

        self.services = {}

        self.register_services(config["services"])

    def register_services(self, config):
        for service_config in config:
            self.services[service_config["label"]] = Service(service_config)

    def load(self, start_date, end_date):
        for servicename, service in self.services.items():
            logger.info(f"Loading data for service {servicename}")
            service.load(start_date, end_date)

    def get(self, date, longitude, latitude, variables=None):
        result = {}
        for servicename, service in self.services.items():
            result[servicename] = service.get(
                date,
                longitude,
                latitude,
                variables=variables,
            )
        return result
