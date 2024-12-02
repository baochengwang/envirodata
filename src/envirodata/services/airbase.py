import datetime
import hashlib
import logging
import os
import copy

import geopandas as gp  # type: ignore
import pandas as pd
import requests
from shapely import Point  # type: ignore
import numpy as np

from envirodata.services.base import BaseGetter, BaseLoader

logger = logging.getLogger(__name__)

METADATA_FNAME = "metadata.parquet"
STATION_DATA_PATH = os.path.join(
    os.path.dirname(__file__), "_data", "airbase", "stations_and_measurements.csv"
)

# Different datasets - not documented, educated guess per
# https://eeadmz1-downloads-webapp.azurewebsites.net:
# 1: Historical Airbase data delivered between 2002 and 2012
#    before Air Quality Directive 2008/50/EC entered into force.
# 2: Verified data (E1a) from 2013 to 2022 reported by countries
#    by 30 September each year for the previous year.
# 3: Unverified data transmitted continuously (Up To Date/UTD/E2a)
#    data from the beginning of 2023.

# priority: which value takes precedence if multiple exist (higher value is better)
DATASETS = [
    {"name": "archived", "dbindex": 1, "priority": 3},
    {"name": "verified", "dbindex": 2, "priority": 2},
    {"name": "uptodate", "dbindex": 3, "priority": 1},
]


class Loader(BaseLoader):
    """Load (cache) airbase dataset."""

    def __init__(
        self,
        cache_path: str,
        pollutants: dict[str, str] | None = None,
        countries: list[str] | None = None,
        bbox: list[float] | None = None,
    ) -> None:
        """Load (cache) cdsapi dataset.

        :param output_fpath: path to output directory
        :type output_fpath: str
        :param pollutants: list of pollutants to get
        :type pollutants: list, defaults to all polllutants
        :param countries: list of country abbreviations to get
        :type countries: list, defaults to all countries
        :param bbox: list of lat/lon bounds (xmin, ymin, xmax, ymax)
        :type bbox: list, defaults to none
        """

        os.makedirs(cache_path, exist_ok=True)
        self.cache_path = cache_path

        self.countries = countries

        # translate short property names ("CO") into id
        # ("http://dd.eionet.europa.eu/vocabulary/aq/pollutant/10")
        apiUrl = "https://eeadmz1-downloads-api-appservice.azurewebsites.net/"
        endpoint = "Pollutant"

        plist = requests.get(apiUrl + endpoint).json()
        self.pollutants = {}
        if pollutants is not None:
            for item in plist:
                if item["notation"] in pollutants:
                    self.pollutants[item["notation"]] = item["id"]

        # get info on stations, locations and pollutants
        if not os.path.exists(STATION_DATA_PATH):
            raise IOError(
                "Need to download station metadata "
                "csv from here: "
                "https://discomap.eea.europa.eu/App/AQViewer/index.html?fqn=Airquality_Dissem.b2g.measurements&SamplingPointStatus=Active "
                "and put there: "
                f"{STATION_DATA_PATH}"
            )

        # get metadata table
        file_age = datetime.datetime.now() - datetime.datetime.fromtimestamp(
            os.path.getmtime(STATION_DATA_PATH)
        )
        if file_age > datetime.timedelta(days=365):
            logger.warning(
                "AIRBASE station metadata file older than a year, "
                "might want to re-download!"
            )

        df = pd.read_csv(STATION_DATA_PATH, low_memory=False)

        self.metadata = gp.GeoDataFrame(
            df, geometry=gp.points_from_xy(df.Longitude, df.Latitude), crs="EPSG:4326"
        )

        if bbox is not None:
            self.metadata = self.metadata.cx[bbox[0] : bbox[1], bbox[2] : bbox[3]]

        # make datetimes
        self.metadata["Operational Activity Begin"] = pd.to_datetime(
            self.metadata["Operational Activity Begin"], utc=True, format="mixed"
        )
        # empty ones are assumed to have data for whole period?!
        #        metadata["Operational Activity Begin"] =
        # metadata["Operational Activity Begin"].fillna(
        #            datetime.datetime(1901, 1, 1, tzinfo=datetime.timezone.utc)
        #        )

        self.metadata["Operational Activity End"] = pd.to_datetime(
            self.metadata["Operational Activity End"], utc=True, format="mixed"
        )
        # empty ones end next year
        #        metadata["Operational Activity End"] =
        # metadata["Operational Activity End"].fillna(
        #            datetime.datetime.now(tz=datetime.timezone.utc) +
        # datetime.timedelta(days=365)
        #        )

        self.metadata = self.metadata.set_index("Sampling Point Id")

        self.metadata_path = os.path.join(self.cache_path, METADATA_FNAME)

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

        if os.path.exists(self.metadata_path):
            logger.info("AIRBASE data already downloaded.")
            return

        # (1) get list of files to download

        apiUrl = "https://eeadmz1-downloads-api-appservice.azurewebsites.net/"
        endpoint = "ParquetFile/urls"

        nDatasets = len(DATASETS)
        iDataset = 1

        # Request body
        for dataset in DATASETS:
            logger.info(f"Dataset {dataset['name']} ({dataset['dbindex']})")
            request_body = {
                "countries": self.countries,
                "cities": [],
                "pollutants": [self.pollutants[x] for x in self.pollutants],
                "dataset": dataset["dbindex"],
                # "dateTimeStart": start_date.isoformat(sep="T"),
                # "dateTimeEnd": end_date.isoformat(sep="T"),
                "aggregationType": "hour",
            }

            urlReq = requests.post(apiUrl + endpoint, json=request_body)
            urlListString = urlReq.content.decode(urlReq.encoding)
            urlList = urlListString.splitlines()[1:]

            # (2) download!

            def decode_url(url):
                fname = url.split("/")[-1]
                for samplingPointId, samplingPoint in self.metadata.iterrows():
                    if samplingPointId in fname:
                        return samplingPoint
                return None

            self.metadata[f"localFilePath_{dataset['dbindex']}"] = None

            # download each individual file
            i = 0
            N = len(urlList)
            for aUrl in urlList:
                # find corresponding metadata
                samplingPoint = decode_url(aUrl)
                if samplingPoint is not None:
                    dataReq = requests.get(aUrl)
                    # make a pretty hash to save locally
                    fname = hashlib.md5(aUrl.encode("utf-8")).hexdigest() + ".parquet"
                    fpath = os.path.join(self.cache_path, fname)
                    # save local path to metadata
                    self.metadata.loc[
                        samplingPoint.name, f"localFilePath_{dataset['dbindex']}"
                    ] = fpath
                    # and save the data (to the cache)
                    with open(fpath, "wb") as f:
                        f.write(dataReq.content)
                i += 1
                logger.info(f"Cached file {i}/{N} (dataset {iDataset}/{nDatasets})")

            iDataset += 1

        # save metadata
        self.metadata.to_parquet(self.metadata_path)


class Getter(BaseGetter):
    def __init__(
        self,
        cache_path: str,
    ):
        """Get values from dataset.

        :param cache_path: Cache path
        :type cache_path: str
        :param countries: list of country abbreviations to get
        :type countries: list, defaults to all countries
        """
        self.cache_path = cache_path

        if not os.path.exists(os.path.join(self.cache_path, METADATA_FNAME)):
            raise IOError("No metadata found - did you load data?")

        self.metadata = gp.read_parquet(os.path.join(self.cache_path, METADATA_FNAME))

    @property
    def time_resolution(self):
        """Time resolution of the dataset."""
        return datetime.timedelta(hours=1)

    def _get(
        self,
        date: datetime.datetime,
        longitude: float,
        latitude: float,
        variable: str,
    ) -> tuple[datetime.datetime, float]:
        """Get value for variable out of cached NetCDF4 file

        :param date: Date to retrieve
        :type date: datetime.datetime
        :param longitude: Geographical longitude
        :type longitude: float
        :param latitude: Geographical latitude
        :type latitude: float
        :param variable: Variable to retrieve
        :type variable: str
        :raises IOError: No data found
        :raises RuntimeError: Unknown number of dimensions in netCDF4 file
        :raises RuntimeError: Unable to get data
        :return: Value for variable at given point in time and space.
        :rtype: float
        """

        times, values = self._get_range(date, date, longitude, latitude, variable)
        return times[0], values[0]

    def _get_range(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        longitude: float,
        latitude: float,
        variable: str,
    ) -> tuple[list[datetime.datetime], list[float]]:
        """Get value for variable out of cached NetCDF4 file

        :param date: Date to retrieve
        :type date: datetime.datetime
        :param longitude: Geographical longitude
        :type longitude: float
        :param latitude: Geographical latitude
        :type latitude: float
        :param variable: Variable to retrieve
        :type variable: str
        :raises IOError: No data found
        :raises RuntimeError: Unknown number of dimensions in netCDF4 file
        :raises RuntimeError: Unable to get data
        :return: Times and Values for variable at given point in time and space.
        :rtype: tuple[list[datetime.datetime], list[float]]
        """

        ds = self.metadata.copy()

        # stations that ever started measuring
        ds = ds[ds["Operational Activity Begin"].apply(lambda x: not pd.isnull(x))]

        # either no measurement end date or end date after requested date
        def good_end_date(x, date):
            if pd.isnull(x):
                return True
            else:
                return x > date

        ds = ds[
            ds["Operational Activity End"].apply(lambda x: good_end_date(x, end_date))
        ]

        if ds.empty:
            return [start_date], [np.nan]

        # and only stations that actually measure that pollutant
        ds = ds[ds["Air Pollutant"] == variable]

        if ds.empty:
            return [start_date], [np.nan]

        # and only stations that we have cached in any of the datasets
        hasCachedData = ds["Country"].isna()
        for dataset in DATASETS:
            hasCachedData = (
                hasCachedData | ds[f"localFilePath_{dataset['dbindex']}"].notna()
            )

        ds = ds[hasCachedData]

        if ds.empty:
            return [start_date], [np.nan]

        # OK - there should be something!

        point = Point(longitude, latitude)

        closest_df_item, distance = ds.sindex.nearest(
            point, return_distance=True, return_all=False
        )

        # get item from df, make dict - improve, pls! ;)
        stationRow = ds.iloc[closest_df_item[1]].to_dict(orient="index")
        stationId = list(stationRow.keys())[0]
        station = stationRow[stationId]

        result = [np.nan]
        times = [copy.copy(start_date)]

        highest_prio_found: int = 0
        for dataset in DATASETS:

            dataFpath = station[f"localFilePath_{dataset['dbindex']}"]
            if dataFpath is None:
                continue

            data = pd.read_parquet(dataFpath)
            data["Start"] = pd.to_datetime(data["Start"], utc=True)
            data["End"] = pd.to_datetime(data["End"], utc=True)

            pretty_start_date = pd.Timestamp(start_date).tz_convert("UTC")
            pretty_end_date = pd.Timestamp(end_date).tz_convert("UTC")

            data = data[
                (data["Start"] < pretty_end_date) & (data["End"] > pretty_start_date)
            ]

            # only valid measurements! https://dd.eionet.europa.eu/vocabulary/aq/observationvalidity
            data = data[data["Validity"] > 0]

            if data.empty:
                continue

            # better data supersedes existing data
            if dataset["priority"] > highest_prio_found:
                tmp = data.Value.tolist()
                result = [float(x) for x in tmp]
                times = data.Start.tolist()
                highest_prio_found = dataset["priority"]

        return times, result
