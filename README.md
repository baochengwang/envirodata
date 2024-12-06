# EnviroData - envirocoding for the masses!

Provide geocoded environmental factor attribution for health care applications.

This repository provides a Python package called `envirodata`, as well as a Docker container.

## Problem statement

Understanding impacts of the environment on health requires estimation of the personal exposure of each individual (patient, cohort participant, ...) and finding associations with health outcomes.

Environmental factors (e.g., air temperature, air quality, distance to green space, mean household income) vary in space and time, they are spatiotemporal fields. Often, this happens on very small spatial scales. Example: if you investigate health effects of ambient noise, it makes a 
large difference if you live right next to a motorway, or one block away.

Epidemiological studies on environmental influences face a problem: location (be it residential address or movement patterns) are personal identifiable information, and need to be protected, especially when they are in a health context.

The traditional way is to degrade location information until anonymity can be ensured, e.g. by using postcode only instead of street address. This step happens within the guarded context of, e.g., a hospital or a study center. Degraded (anonymized) location information can then be transferred out of the guarded context and associated with environmental factors (e.g., within a specialized research group at a university). 

However:
 - important details in environmental exposure estimates are lost
 - each research project has to repeat this work again and again

## Proposed solution

Instead of taking degraded location information out of the guarded context, we bring the environmental factors into the guarded context and provide a way to associate them in a data protection compliant manner. The following components are needed:

### Local geocoder

A way to translate an address into a geographic coordinate locally on a computer without resorting to external services (e.g., Google Maps).

### Data cache, extractor and aggregator
 
A way to cache various datasets on environmental factors locally. Methods to extract from these datasets at specified location / time combinations. Provisions to calculate statistical averages over space and time.

## Structure of EnviroData

The EnviroData application is therefore split into 2 parts:

### (1) Preparation and downloading of data (online)

This requires internet access. Needs to be done upon inital installation outside of the guarded context. Potentially, it needs to be repeated when new data becomes available. 

 - Setting up a local geocoder with current data.
 - Download environmental data and cache locally.

### (2) Provide offline envirocoding service in guarded context

Provide a set of methods (API) to request environmental factor information for a given combination of address and time. No internet access needed, all actions are local and conform with data protection. This is the default mode to run envirodata.

## Implementation

EnviroData consists of:

 - Geocoding: Offline geocoding using the [Nominatim](https://nominatim.org) geocoder based on OpenStreetMap data
 - Services: An extensible way to add new environmental factor datasets
 - Statistics: A way to define new statistical aggregations
 - API: methods to retrieve individual exposure information

## Services implemented

### CDSAPI

Get model data fields from Copernicus (model results) [Atmosphere Data Store](https://ads.atmosphere.copernicus.eu/) or the [Climate Data Store](https://cds.climate.copernicus.eu/). Model data is cached as NetCDF files.

### DWD

Get station observations from DWD using [BrightSky](https://brightsky.dev).

Station data is provided through BrightSky running and caching in a local Docker container.

### GeoTIFF

Get arbitrary GeoTIFF datasets (without any time dependence) using [rasterio](https://rasterio.readthedocs.io/en/stable/).

GeoTIFF data is cached by copying the files into the cache directory.

## Documentation

[Sphinx](https://www.sphinx-doc.org/en/master/) API documentation can be created using `tools/build_docs.bash`, and then be found at `docs/_build/index.html`.
