# EnviroData Geocoder 

This is the script repository to build up a geocoding service based on PostgreSQL in the framework of the Intramural Project -- "EnviroData". 

## Prerequisites

- python 3.11.5
	- flask 3.0.0 [REST API]
	- psycopg2 2.9.7 [PostgreSQL database adapter for Python]
- mamba 1.4.2
- conda 23.3.1
- psql 16.0 
- postgis 3.4.0 


### Useful `psql` commands

- __run `.sql` script from `bash`__:  `psql -U $USER -d hks -a -f pgd_reader.sql`
	`hks` is the postgresql database where hauskoordinaten data are stored.


### How to start `flask` locally

- `flask` is installed under the `mamba` environment `autogis` in `zsh`. 
- run `zsh; mamba activate autogis; flask`


## Roadmap


1. Trim Hauskoordinaten data using `csvkit` to keep essential address columns.
   - install `csvkit` module via `pip install csvkit`
   - `str`: street name
   - `hnr`: house number
   -  `adz`: Adressezusatz, additional address information
   -  `postplz`: Postleitzahl, postal code
   -  `kreis`: Landkreis
   -  `regez`: Bezirkverwaltung
   -  `land`: Bundesland
   -  `ostwert`: false easting under UTM32N [epsg code: 32632]
   -  `nordwert`: false northing under UTM32N

2. Insert Hauskoordinaten data to a PostgreSQL database.

3. Build Python REST API and define fuzzy searching rules.

4. ...