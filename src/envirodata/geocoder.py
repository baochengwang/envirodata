"""Main Geocoding interface"""

import logging  # for error message reporting

import requests

logger = logging.getLogger()


class Geocoder:
    """Geocoder interface"""

    def __init__(self, url: str) -> None:
        self.url = url

    def standardize_address(
        self,
        postcode: str,
        city: str,
        streetname: str,
        house_number: str,
        extension: str = "",
    ) -> str:
        """Generate a single address string to use in geocoding server
        requests out of parts of an address.

        :param postcode: Postcode
        :type postcode: str
        :param city: City
        :type city: str
        :param streetname: Street name
        :type streetname: str
        :param house_number: House number
        :type house_number: str
        :param extension: Address extension, defaults to ""
        :type extension: str, optional
        :return: Full address
        :rtype: str
        """

        return f"{streetname} {house_number} {extension}, {postcode} {city}"

    def geocode(
        self,
        postcode: str,
        city: str,
        streetname: str,
        house_number: str,
        extension: str = "",
    ) -> (float, float):
        """Geocode an address and return coordinates

        :param postcode: Postcode
        :type postcode: str
        :param city: City
        :type city: str
        :param streetname: Street name
        :type streetname: str
        :param house_number: House number
        :type house_number: str
        :param extension: Address extension, defaults to ""
        :type extension: str, optional
        :raises IOError: JSON response is malformed
        :raises IOError: Address could not be geocoded
        :return: Coordinates (longitude, latitude) of the geocoded address
        :rtype: float, float
        """

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
