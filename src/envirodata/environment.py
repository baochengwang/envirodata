import logging  # for error message reporting
import datetime

from envirodata.utils.general import load_object

# error message
logger = logging.getLogger()


class Service:
    def __init__(self, config):
        self.variables = config["variables"]
        self.config = config

        input_config = self.config["input"]
        input_class = load_object(input_config["module"], "Loader")
        self._loader = input_class(**input_config["config"])

        output_config = self.config["output"]
        output_class = load_object(output_config["module"], "Getter")
        self._getter = output_class(**output_config["config"])

    def load(self, start_date, end_date):
        self._loader.load(start_date, end_date)

    def get(self, date, longitude, latitude, variables=None):
        if not variables:
            variables = self.variables

        return {
            variable: self._getter.get(date, variable, longitude, latitude)
            for variable in variables
        }


class Environment:
    def __init__(self, config):
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
