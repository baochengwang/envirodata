# Instruction for script-based downloading ERA5-land data

- To download ERA5-Land Data, the [.cdsapirc](https://cds.climate.copernicus.eu/api-how-to
) file should be saved in `$HOME/.cdsapi_climate` !!


- To download data from Copernicus CAMS (atmospheric reanalysis data), save the cdsapi file in `$HOME/.cdsapi_atmosphere`!


## ERA5-Land

- Files are downloaded and saved as [var]_[year].zip; when unzipped, all files are named as data.nc by default.  
- A script is used to unzip each file and save it with a unique name.

