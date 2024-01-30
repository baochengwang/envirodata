# EnviroData Project

This is the script repository to build up a geocoding service based on PostgreSQL in the framework of the Intramural Project -- "EnviroData". 

## Prerequisites

- python 3.12.0
	- flask 3.0.0 [REST API]
	- psycopg2 2.9.7 [PostgreSQL database adapter for Python]
- mamba 1.4.2
- conda 23.3.1
- psql 16.0 
- postgis 3.4.0 
- poetry 1.7.1 [Installation](https://python-poetry.org/docs/#installing-with-the-official-installer)



[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)


### Useful `psql` commands

- __run `.sql` script from `bash`__:  `psql -U $USER -d hks -a -f pgd_reader.sql`;
    - `hks` is the postgresql database where hauskoordinaten data are stored.
    - `pgd_reader.sql` file reads Hauskoordinaten csv data and convert it to psql data table. 

### [`pgcli`](https://github.com/dbcli/pgcli): auto-completion and syntax highlighting in `psql`

- install `pgcli` for auto-completion in `psql` terminal: `pip install -U pgcli` or `brew install pgcli` (on MacOS).
- USAGE: `$ pgcli database_name`


### How to start `flask` locally

- `flask` is installed under the `mamba` environment `autogis` in `zsh`. 
- run `zsh; mamba activate autogis;`
- `flask -A [geocoding python script, e.g., geocoder.py] run`;
- OR define the environment variable `FLASK_APP` using `export FLASK_APP=geocoder.py`, then run `flask run`


### How to test REAT API
#### Insomnia GUI
- install [Insomnia](https://insomnia.rest/) to test REST API.

#### In terminal.
- via `curl` in bash:
    - `curl -X GET -H 'Content-Type: application/json' -d '{"str": "Grafngerstr.","ort": "Moosach","hnr": "11-15","plz": "322200","time": "2012-10-11"}' http://127.0.0.1:5000/geocoder/`
    - or refer to json file where query data are store: `-d @[FILENAME]`: `curl -X GET -H 'Content-Type: application/json' -d @./2.code/query_example.json http://127.0.0.1:5000/geocoder/`


## Roadmap


1. Trim Hauskoordinaten data using `csvkit` to keep essential address columns.
   - install `csvkit` module via `pip install csvkit`
   - `str`: street name
   - `hnr`: house number
   -  `adz`: Adressezusatz, additional address information
   -  `postplz`: Postleitzahl, postal code
   - `postonm`: Ort name (place name)
   -  `kreis`: Landkreis
   -  `regez`: Bezirkverwaltung
   -  `land`: Bundesland
   -  `ostwert`: false easting under UTM32N [epsg code: 32632]
   -  `nordwert`: false northing under UTM32N

2. Insert Hauskoordinaten data to a PostgreSQL database.

3. Build Python REST API and define fuzzy searching rules.

4. Refine the fuzzy string matching rules.
    - Preprocess address entered by users 
        - set a thesaurus (str(.) -> straße)
        - seperate str+hnr+(adz.) into individual entries
    - Check whether postplz and postonm match
        - TRUE: -> END
        - FALSE -> Use postplz for query
            - FOUND -> END
            - NOT FOUND -> postplz be misspelled, use postonm for query.
                - FOUND -> END
                - NOT FOUND -> ERROR: Re-check Input
    - For `str` query, combine `daitch_mokotoff()` and `levenshtein()` rules to sharpen the matching rules.
    
    

## Data sources

- UBA Luftmessstationen Metadata: [URL](https://www.env-it.de/stationen/public/stationList.do)

