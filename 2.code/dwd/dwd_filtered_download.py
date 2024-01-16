#!/usr/bin/env python3


from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
import re
import logging

import yaml

# ----------------------------------------------
#   Constant
# ----------------------------------------------

# ABBS = {'wind':'ff',
#         'air_temperature':'tu',
#         'dew_point':'td',
#         'moisture': 'tf',
#         'solar':'sd',
#         'sun':'sd',
#         'precipitation':'rr',
#         'extreme_wind':'fx',
#         'extreme_temperature':'tx'}

# ----------------------------------------------
#   Input Data
# ----------------------------------------------

with open("config.yaml", "r") as f:
    config = yaml.load(f, Loader=yaml.Loader)

# variable name
# vars = ['air_temperature','wind','solar','extreme_temperature','precipitation','extreme_wind']
var = config["var"]

t_interval = config["t_interval"]  # ['10_minutes','hourly']

state = config["state"]  # state is a list, wrapped in []

target_folder = config["target_folder"]

year_start = config["year_start"]

year_end = config["year_end"]

# ----------------------------------------------
#    Check INPUT
# ----------------------------------------------
# if state is given as string, --> list
if isinstance(state, str):
    state = [state]

# if the target folder does not exist, then create it.
if not Path(target_folder).is_dir():
    Path(target_folder).mkdir(parents=True, exist_ok=True)


# ----------------------------------------------
#    START FUNCTIONS
# ----------------------------------------------


# function to list all data in the url, with an extension of `ext`
def listFD(url, ext=""):
    page = requests.get(url).text
    soup = BeautifulSoup(page, "html.parser")
    links = [
        node.get("href")
        for node in soup.find_all("a")
        if node.get("href").endswith(ext)
    ]
    return links


def dwd_meta_reader(var, t_res="10_minutes"):
    # parent URL folder where all variables at temporal resolution `t_res` are available
    p_list = (
        "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/"
        + t_res
        + "/"
    )

    # available variables downloadable
    avs = listFD(p_list)

    avs = [av.rstrip("/") for av in avs]

    # if variable is not available for the defined temporal resolution, return a message!
    if not var in avs:
        logging.error(f"{t_res} {var} is NOT AVAILABLE for download!")
        return

    # url where metadata is stored
    base_url = p_list + var + "/historical/"

    url_meta = listFD(base_url, "txt")

    # the metadata file is most often ended with '*_Beschreibung_Stationen.txt';
    # It has not been systematically checked whether this holds TURE.
    pattern = re.compile(r"Beschreibung_Stationen.txt")

    url_meta = [url for url in url_meta if pattern.search(url)]

    # the complete URL for meta data.
    url_meta = "".join([base_url] + url_meta)

    # read header
    header = pd.read_csv(url_meta, nrows=1, delimiter=" ", encoding="latin1")

    # read raw data
    data = pd.read_fwf(
        url_meta,
        widths=[6, 9, 8, 15, 12, 10, 42, 98],
        header=None,
        skiprows=2,
        encoding="latin1",
    )

    # rename columns
    station_meta = data.rename(columns=dict(zip(data.columns, header.columns)))

    # convert column types
    station_meta = station_meta.astype(
        {"Stations_id": "int", "von_datum": "str", "bis_datum": "str"}
    )

    # convert von_datum and bis_datum to datatime.date format
    station_meta[["von_datum", "bis_datum"]] = station_meta[
        ["von_datum", "bis_datum"]
    ].apply(pd.to_datetime, format="%Y%m%d")

    return station_meta


# function to download a single file
def download_url(args):
    url, fn = args[0], args[1]

    try:
        r = requests.get(url)
        with open(fn, "wb") as f:
            f.write(r.content)
    except Exception as e:
        print("Exception in download_url():", e)


# function to parallel downloading
def download_parallel(args):
    cpus = cpu_count()
    results = ThreadPool(cpus - 1).imap_unordered(download_url, args)


# function to list downloadable zip files for variable `var`
def dwd_file_list(
    var, ids, t_res="10_minutes", y_start=2010, y_end=2022, target_folder="./"
):
    # parent URL folder where all variables at temporal resolution `t_res` are available
    p_list = (
        "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/"
        + t_res
        + "/"
    )

    # available variables downloadable
    avs = listFD(p_list)

    avs = [av.rstrip("/") for av in avs]

    # if variable is not available for the defined temporal resolution, return a message!
    if not var in avs:
        logging.error(f"{t_res} {var} is NOT AVAILABLE for download!")
        return

    # url where metadata is stored
    base_url = p_list + var + "/historical/"

    # file extension for DWD Climate Data
    ext = "zip"

    # list all zip files within the base_url
    fns = listFD(base_url, ext)

    # Creating a dataframe
    df = pd.DataFrame({"url": fns})

    # Extracting the third part of the strings in the 'url' column
    df[["id", "von_datum", "bis_datum"]] = (
        df["url"].str.split("_", expand=True).loc[:, [2, 3, 4]]
    )

    df = df.astype({"id": int})

    df[["von_datum", "bis_datum"]] = df[["von_datum", "bis_datum"]].apply(
        pd.to_datetime, format="%Y%m%d"
    )

    df = df[df["id"].isin(ids)]

    df = df[df.bis_datum >= datetime(y_start, 1, 1)]

    df = df[df.von_datum <= datetime(y_end, 1, 1)]

    # full_urls are URLs for downloading
    df = df.assign(full_url=base_url + df.url, local=target_folder + df.url)

    return df


# ---------------------------------------
#    END FUNCTIONS
# ---------------------------------------

fm = dwd_meta_reader(var)

# all IDs in defined federal state
station_ids = fm.query(f"Bundesland in {state}")["Stations_id"].to_list()

# all available zip files and their metadata
# Default temporal resolution 10-min
zip_files = dwd_file_list(
    var,
    station_ids,
    y_start=year_start,
    y_end=year_end,
    target_folder="../../0.raw/dwd/",
)

urls = zip(zip_files.full_url, zip_files.local)

# ---------------------------------------

# download_parallel(urls)
