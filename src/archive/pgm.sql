-- Enable PostGIS
CREATE EXTENSION postgis;

-- Enable Topology
CREATE EXTENSION postgis_topology;

-- Enable PostGIS Advanced 3D and other geoprocessing algorithms
CREATE EXTENSION postgis_sfcgal;

-- fuzzy string matching
CREATE EXTENSION fuzzystrmatch;

-- rule based standardizer
CREATE EXTENSION address_standardizer;

-- display postgis version
SELECT PostGIS_full_version();

-- display postgresql version
SELECT version();

-- remove extension: DROP EXTENSION IF EXISTS [extension_name]

CREATE TABLE hauskds (
--    id SERIAL PRIMARY KEY,
    str text,
    hnr integer,
    adz text,
    postplz integer,
    postonm text,
    kreis text,
    regbez text,
    land text,
    ostwert double precision,
    nordwert double precision
    );

-- working directory = project root path
\COPY hauskds FROM '0.raw/moosach_lite.csv' WITH CSV HEADER;


-- Display the first rows of a data table
SELECT * FROM hauskds LIMIT 10;


-- set coordinate reference system code (EPSG ID): UTM32N on WGS84
\set CRS 32632  

\set CRS_WGS84 4326  # WGS84


--  add a geom point column
ALTER TABLE hauskds
ADD COLUMN geom GEOMETRY(Point, :CRS);

-- convert coordinates (ostwert,nordwert) into point
UPDATE hauskds
SET geom = ST_SetSRID(ST_MakePoint(ostwert, nordwert), :CRS);
