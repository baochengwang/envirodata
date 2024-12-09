# EnviroData - envirocoding for the masses!

> *envirocoding:* associating a point in time and space with information on the environment that was present.

## Problem statement

Understanding impacts of the environment on health requires estimation of the personal exposure of each individual (patient, cohort participant, ...) to certain environmental factors, and finding associations with health outcomes.

Environmental factors (e.g., air temperature, air quality, distance to green space, mean household income) vary in space and time, they are spatiotemporal fields. Often, this variation happens on very small spatial and temporal scales. *Example 1: if you investigate health effects of ambient noise, it makes a large difference if you live right next to a motorway, or one block away.* *Example 2: a thunderstorm passes within half an hour, might lead to an acute asthma attack due to stirred up dust.*

Epidemiological studies on environmental influences face a problem: location (be it residential address or movement patterns) is personal identifiable information, and needs to be protected. Especially when working in a health context.

| Patient ID | Residential address                       | Date/time of admission | ICD10 |
| ---------- | ----------------------------------------- | ---------------------- | ----- |
| abcd1234   | Werner-von-Siemens-Str. 6, 86159 Augsburg | 2024-12-01 12:00       | J44.1 |
| abcd1235   | ...                                       | ...                    | ...   |

*Table 1: Personal identifiable information usually present in health datasets.*

The traditional way is to degrade location and time information until anonymity can be ensured, e.g. by using only postcode instead of street address.

| Patient ID | Postcode                            | Date of admission | ICD10 |
| ---------- | ----------------------------------- | ------------------| ----- |
| abcd1234   | 86159                               | 2024-12-01        | J44.1 |
| abcd1235   | ...                                 | ...               | ...   |

*Table 2: Anonymized (degraded) information on space and time.*

Table 2 could now be handed over to environmental health professionals (also outside the hospital) for association with environmental factors.

**Important information on environmental factors are lost!** *(just think of the example with the motorway.)*

## Proposed solution

Instead of taking anonymized (degraded) location information out of the guarded context of a hospital / study center, we bring environmental factors data into the guarded context and provide a way to associate them with individual information.

We make use of the fact that once association with environmental factors is done, the resulting dataset is not personal identifying information anymore:

| Patient ID | Air temperature (K) | PM2.5 concentration (ug/m3) | Ozone concentration (ppbv) | ICD10 |
| ---------- | ------------------- | --------------------------- | -------------------------- | ----- |
| abcd1234   | 286.5               | 8.3                         | 46.4                       | J44.1 |
| abcd1235   | ...                 | ...                         | ...                        | ...   |

*Table 3: Association with environmental factors.*

**Note that we could also remove the pseudonym (Patient ID), and would have a completely anonymized dataset.**

To make this work, two components are required:

### Local geocoder

A way to translate an address into a geographic coordinate locally on a computer without resorting to external services (e.g., Google Maps). Environmental factor datasets (maps) usually work with coordinates.

### Data cache, extractor and aggregator

A way to cache various datasets on environmental factors locally. Methods to extract from these datasets at specified location / time combinations. Provisions to calculate statistical averages over space and time.

## Implementation and structure of EnviroData

The EnviroData application is therefore split into 2 parts:

### Preparation and downloading of data

Set up a local geocoder with current data, and download environmental data and cache locally. This part requires internet access and is done upon initial installation *outside of the guarded context*. Potentially, it needs to be repeated when new data becomes available.

### Offline envirocoding service in guarded context

Provide a set of methods (API) to request environmental factor information for a given combination of address and time. No internet access needed, all actions are local and conform with data protection. This is the default mode to run envirodata.

## Implementation

### Geocoding

Offline geocoding using the [Nominatim](https://nominatim.org) geocoder based on OpenStreetMap data.

### Services

Definition of methods and classes to provide environmental factor data. An way for users to add new datasets.

Examples implemented:
- station data of the German Weather Service
- sociodemographic information from the Federal Statistical Office of Germany Census
- model results from the Copernicus Atmospheric Monitoring Service

### Statistics

Definition of typically used temporal and spatial statistics. A way to define new statistical aggregations for users.

Examples:
- current
- daily minimum, daily mean, daily max
- 7 day daily maximum
- MDA8 (for ozone)

### API and web interface

Methods to retrieve individual exposure information. Implemented is a web interface (standard at [http://localhost:8000](http://localhost:8000)) and a REST-API.

Example of a REST-API call to envirocode our office on January 1st, 2020:

```
curl -X 'GET' \
  'http://localhost:8000/api/simple?date=2020-01-01T12%3A00%3A00&address=Werner-von-Siemens%20Str.%206%2C%2086159%20Augsburg' \
  -H 'accept: application/json'
```

Result (abbreviated):

```
{
  "metadata": {
    "package_version": "0.1.0",
    "git_commit_hash": "fd746a30880fcc624d574eb9dbf68528553ac7ca",
    "creation_date": "2024-12-09T12:28:52.779540+00:00",
    "requested_date_utc": "2020-01-01T12:00:00+00:00"
  },
  "geocoding": {
    "address": "Werner-von-Siemens Str. 6, 86159 Augsburg",
    "address_found": "Haus 18 BMK Group, 6, Werner-von-Siemens-Straße, Universitätsviertel, Augsburg, Bayern, 86159, Deutschland",
    "location": {
      "longitude": 10.902599924137931,
      "latitude": 48.3448287
    }
  },
  "environment": {
    "DWD": {
      "values": {
        "dew_point": {
          "current": 273.05
        },
        "precipitation": {
          "day_sum": 0
        },
        "wind_speed": {
          "current": 3.9,
          "3day_mean": 1.5614583333333334,
          "7day_mean": 2.2390624999999997
        },
        [...]
      },
      "metadata": {
        "service": {
          "description": "Station data from the German Weather service",
          "url": "https://www.dwd.de",
          "license": "https://www.dwd.de/copyright"
        },
        "variables": {
          "dew_point": {
            "name": "dew_point",
            "long_name": "dew_point",
            "description": "Dew point",
            "units": "K",
            "statistics": [
              {
                "name": "current",
                "begin": -1800,
                "end": 1800,
                "function": {},
                "daily": false
              }
            ]
          },
          [...]
        }
      }
    },
    [...]
  }
}
```