# EnviroData Project

Provide geocoded environmental factor attribution for health care applications.

This repository provides a Python package called `envirodata`, as well as a Docker container.

The application is split into 2 parts:

 1) Preparation and downloading of environmental data

Environmental data is downloaded and cached locally. This requires internet access. Needs to be done upon inital installation, 
and potentially repeated to cache new data.

 2) Run local envirodata service

Environmental factor attribution can be requested through a REST-API. No internet access needed, all actions are local and conform
with data protection. This is the default mode to run envirodata.

## Development setup

 1) Install [docker](https://www.docker.com).

 2) Clone envirodata repo (need username or deploy token - contact us):
   
    `git clone https://git.rz.uni-augsburg.de/mmbees-git/envirodata.git`

 3) Go to the directory where you cloned the repository in.

    `cd envirodata`

 4) Setup and start dockerized Nomatim geocoder (might need to `sudo` this...):
   
   `bash tools/setup_nomatim_docker.bash`

 5) Setup and start BrightSky DWD data provider (might need to `sudo` this...):
 
   `bash tools/setup_brightsky_api.bash`

 6) Build envirodata docker container (might need to `sudo` this...):
 
   `bash tools/build_envirodata_docker.bash`

  7) Start an Envirodata container and load data

    `bash tools/run_loader_container.bash`

  8) Run the Envirodata service

    `bash tools/start_server_container.bash`

## Configuration

`envirodata` is configured through one configuration file, `config.yaml`, in `WORKPATH`.

## Setup and running

For development, usage without actually installing the package works through calling `poetry shell` when in `WORKPATH`. This gives you a shell with access to a `python3` that knows about the new package, and also enables command line scripts (e.g., `load_data`, `run_server`) defined in `pyproject.toml`.

### Preparation and downloading data

    Uses the `src/envirodata/Environment.py` class to cache data from all services defined in `config.yaml`.

    Just execute `load_data` and wait (will be several hours!).

## Running

    Just execute `run_server`.

    Runs a [FastAPI](https://fastapi.tiangolo.com) server,uses `src/envirodata/Geocoder.py` to geocode address requests, and requests environmental parameters from `src/envirodata/Environment.py`.

    You can test the API by pointing your browser to http://localhost:8000/docs.

## Services implemented

### CDSAPI

Get model data fields from Copernicus (model results) [Atmosphere Data Store](https://ads.atmosphere.copernicus.eu/) or the [Climate Data Store](https://cds.climate.copernicus.eu/).

Model data is cached as NetCDF files.

### DWD

Get station observations from DWD using [BrightSky](https://brightsky.dev).

Station data is provided through BrightSky running and caching in a local Docker container.

### GeoTIFF

Get arbitrary GeoTIFF datasets (without any time dependence) using [rasterio](https://rasterio.readthedocs.io/en/stable/).

GeoTIFF data is cached by copying the files into the cache directory.

## Documentation

[Sphinx](https://www.sphinx-doc.org/en/master/) API documentation can be created using `tools/build_docs.bash`, and then be found at `docs/_build/index.html`.


# Tipps and tricks

After installing poetry and making sure its installation location (e.g., `~/.local/bin`) is in `$PATH`, you might need to log out and log in again, before the correct poetry is picked up.

