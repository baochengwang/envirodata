import re  # regular expression to handle non-standard addresss input
import logging  # for error message reporting

import psycopg2
from psycopg2.extras import LoggingConnection

# error message
logger = logging.getLogger()


class Geocoder:
    def __init__(self, config):
        self.conn = psycopg2.connect(
            connection_factory=LoggingConnection, **config["database"]
        )
        self.conn.initialize(logger)

        # regular expression to check if a string ended with number(s), or LIKE ~ 12 - 15, 12c
        # Street names with numbers, e.g., "Straße der 17. Juni" do not fall into this case.
        self.re_str_hnr = re.compile(r"([0-9]{0,5}\W{0,3}[0-9]+)([a-zA-Z]{0,1})$")

    def _hnr_string_to_int(self, hnr_string):
        # Function to convert house number (hnr) to integer
        #  e.g. "101" -- 101, "12c" -- 12, "12-14c" -> 13.
        hnr_string = re.sub("[^0-9,-]", "", hnr_string).split(
            "-"
        )  # for entry such as "12-14","12a"
        hnr_int = list(
            map(int, hnr_string)
        )  # or assign the house number before "-"::  int(hnr[0])
        hnr_int = int(sum(hnr_int) / len(hnr_int))
        return hnr_int

    def coords_from_address(self, postcode, city, streetname, house_number, extension):
        street = streetname  # if not found nor undefined, return ''.

        # regular expression to replace abbre.
        if not "straße" in street.lower():
            street = re.sub(
                "[sS]tr[.]{0,1}", "straße", street
            )  # replace 'str.' to 'straße'

        # IF street name is correctly given, assign hnr_derived to 0.
        # hnr_derived will be used to assign hnr, if hnr is not given in JSON file.
        hnr_derived = 0
        adz_derived = ""

        # if street name ENDED with numbers, it is highly probable that house number is included.
        # update the default hnr_derived
        if self.re_str_hnr.search(street):  # if True (street name ended with numbers)
            hnr_string = self.re_str_hnr.search(street)  # return the matched string(s)
            street = street.replace(
                hnr_string.group(0), ""
            )  # update the read street name
            hnr_derived = self._hnr_string_to_int(
                hnr_string.group(1)
            )  # overwrite the default hnr_derived
            if hnr_string.group(2) != "":
                adz_derived = hnr_string.group(2)  # separate adz from string.

        # read house number (hnr); IF not defined, return 0, centroid of the street returned
        hnr = house_number

        if hnr is None or not hnr:
            hnr = hnr_derived

        # if hnr is read as an string, remove adz, and convert to integer!
        if isinstance(
            hnr, str
        ):  # if hnr is a string. # str is data type. DO NOT USE IT AS VARIABLE NAME !
            hnr = self._hnr_string_to_int(hnr)

        # get ZIP code, if not available, return None
        plz = postcode

        # get adressezusatz (adz)
        adz = extension if adz_derived == "" else adz_derived

        # get city name (ort)
        ort = city

        ADDRESS_MATCH = """select * from address_match(%s,%s,%s,%s,%s);"""  # street, house_number, add_address, city, plz, lat, lon, logger

        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    ADDRESS_MATCH,
                    (
                        street,
                        hnr,
                        adz,
                        ort,
                        plz,
                    ),
                )
                result = cursor.fetchone()

        print("Fakey fakey")
        return 11.1, 48.3
