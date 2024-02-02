# EnviroData Project

This is the script repository to build up a geocoding service based on PostgreSQL in the framework of the Intramural Project -- "EnviroData". 

It provides a Python package called `envirodata`.

## Development setup

 1) Install [docker](https://www.docker.com).
 2) Install [poetry](http://poetry.eustace.io).
 3) Clone repo into folder called `WORKPATH` from here on.
 4) Setup dockerized PostGIS server for geocoding
    1) create a slimmed down version of your Hauskoordinaten `.csv` file (might need to use [test data](https://www.ldbv.bayern.de/produkte/kataster/hauskoordinaten.html) or ask Christoph / Bin) using `tools/geocoding_server/trim_hauskoordinaten.sh`.
    2) create docker container with PostGIS by following calls in `tools/geocoding_server/create_postgres_server.bash`. 
 5) Create poetry environment and install packages
    1) go to `WORKPATH`
    2) run `poetry install`

## Configuration

`envirodata` is configured through one configuration file, `config.yaml`, in `WORKPATH`.

## Running

For development, usage without actually installing the package works through calling `poetry shell` when in `WORKPATH`. This gives you a shell with access to a `python3` that knows about the new package, and also enables command line scripts (e.g., `load_data`, `run_server`) defined in `pyproject.toml`.

 1) Downloading and caching data

    Uses the `src/envirodata/Environment.py` class to cache data from all services defined in `config.yaml`.

    Just execute `load_data`.

 2) Running the actual server

    Just execute `run_server`.

    Runs a [FastAPI](https://fastapi.tiangolo.com) server,uses `src/envirodata/Geocoder.py` to geocode address requests, and requests environmental parameters from `src/envirodata/Environment.py`.

## Services implemented

### CDSAPI

Get model data fields from Copernicus (model results) [Atmosphere Data Store](https://ads.atmosphere.copernicus.eu/) or the [Climate Data Store](https://cds.climate.copernicus.eu/).

Model data is cached as NetCDF files.

### Wetterdienst

Get station observations from DWD using the [wetterdienst](https://wetterdienst.readthedocs.io/en/latest/) package.

Station data is cached into a local (sqlite3 file-) database.




