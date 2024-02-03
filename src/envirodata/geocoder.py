import re  # regular expression to handle non-standard addresss input
import logging  # for error message reporting

import requests

# error message
logger = logging.getLogger()


class Geocoder:
    def __init__(self, url):
        self.url = url

    def standardize_address(self, postcode, city, streetname, house_number, extension):
        return f"{streetname} {house_number} {extension}, {postcode} {city}"

    def geocode(self, postcode, city, streetname, house_number, extension):
        address = self.standardize_address(
            postcode, city, streetname, house_number, extension
        )
        response = requests.get(self.url, params={"q": address}, timeout=10)

        response.raise_for_status()

        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError as exc:
            raise IOError("Malformed JSON response") from exc

        if len(data) > 0:
            # probably have to sort by place_rank
            best_match = data[0]
            return float(best_match["lon"]), float(best_match["lat"])
        else:
            raise IOError(f"Could not geocode address {address}")
