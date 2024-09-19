# EnviroData Project

Provide geocoded environmental factor attribution for health care applications.

It provides a Python package called `envirodata`.

## Development setup

 1) Install [docker](https://www.docker.com).
 2) Install [poetry](http://poetry.eustace.io).
 3) Clone repo into folder called `WORKPATH` from here on.
 4) Setup and start dockerized Nomatim geocoder (`tools/setup_nomatim_docker.bash`) 
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

 3) Test it out

    Once you cached data and run the server, you can test the API by pointing your browser to http://localhost:8000/docs.

## Services implemented

### CDSAPI

Get model data fields from Copernicus (model results) [Atmosphere Data Store](https://ads.atmosphere.copernicus.eu/) or the [Climate Data Store](https://cds.climate.copernicus.eu/).

Model data is cached as NetCDF files.

### Wetterdienst

Get station observations from DWD using the [wetterdienst](https://wetterdienst.readthedocs.io/en/latest/) package.

Station data is cached into a local (sqlite3 file-) database.

### GeoTIFF

Get arbitrary GeoTIFF datasets (without any time dependence) using [rasterio](https://rasterio.readthedocs.io/en/stable/).

GeoTIFF data is cached by copying the files into the cache directory.

## Documentation

[Sphinx](https://www.sphinx-doc.org/en/master/) API documentation can be created using `tools/build_docs.bash`, and then be found at `docs/_build/index.html`.


# Tipps and tricks

After installing poetry and making sure its installation location (e.g., `~/.local/bin`) is in `$PATH`, you might need to log out and log in again, before the correct poetry is picked up.

