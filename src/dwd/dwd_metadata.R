## ---------------------------
##
## Purpose of script: To download and process DWD station meta data
##
## Author: Bin Zhou
##
## Date Created: 2024-01-10
##
## Copyright (c) Bin Zhou, 2021
## Email: bin.zhou@med.uni-augsburg.de
##
## ---------------------------
##
## Notes:
##
## ---------------------------
library(data.table)
library(magrittr)
library(dplyr)
library(sf)
library(mapview)


# URL of DWD station meta-data.
URL = 'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/recent/zehn_min_tu_Beschreibung_Stationen.txt'

# read header
header = fread(URL, nrows = 1, header = TRUE) %>%
  names()


fread(URL)

# read meta data
station.meta =  file(URL, encoding = 'latin1') %>%
  read.fwf(
    widths = c(6, 9, 8, 15, 12, 10, 42, 98),
    header = FALSE,
    skip = 2
  ) %>%
  data.table()

# rename columns
names(station.meta) = header

# trim spaces in columns: Stationsname & Bundesland

station.meta %<>% mutate(Stationsname =trimws(Stationsname),
                         Bundesland = trimws(Bundesland))


# convert von_datum/bis_datum [numeric] to `Date` format
station.meta %<>%
  mutate(
    von_datum = as.Date(as.character(von_datum), format = '%Y%m%d'),
    bis_datum = as.Date(as.character(bis_datum), format = '%Y%m%d')
  )

# convert to sf spatial object
station.sf = station.meta %>%
  st_as_sf(coords = c('geoLaenge', 'geoBreite'), crs = 4326)

# view station location
mapview(station.sf)

# to select only stations within Bayern
station.bayern <- station.sf %>%
  filter(Bundesland == 'Bayern')

mapview(station.bayern)
# to select stations with continuous measurement ranging from 2008-2023
#
station.meta %>%
  filter(von_datum > '2008-01-01',
         bis_datum > '2024-01-01')
