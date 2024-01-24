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

-- -- Enable US Tiger Geocoder
-- CREATE EXTENSION postgis_tiger_geocoder;

-- display postgis version
SELECT PostGIS_full_version();

-- display postgresql version
SELECT version();

-- remove extension: DROP EXTENSION IF EXISTS [extension_name]

DROP TABLE IF EXISTS hauskds;

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
    ostwert decimal,
    nordwert decimal
    );




-- craete gin (Generalized Inverted Index) index for columns where fuzzystrmatching is applied
CREATE INDEX ix_dm ON hauskds USING gin (daitch_mokotoff(str)) WITH (fastupdate = off);
CREATE INDEX ix_dm_city ON hauskds USING gin (daitch_mokotoff(postonm)) WITH (fastupdate = off);


-- working directory = project root path
-- \COPY hauskds FROM 'raw/moosach_lite.csv' WITH CSV HEADER;

\COPY hauskds FROM 'raw/Hauskoordinaten_bayernweit_Lite.csv' WITH CSV HEADER;



-- create a table to store unique cityname-plz pairs for pretesting the correctness of INPUT
DROP TABLE IF EXISTS ort_plz;

select distinct postonm,postplz
  into TABLE ort_plz
  from hauskds
  order by postonm;
  



-- use the mean ostwert and nordwert to store the coordinates of the street, in case the house number is not given.
CREATE TABLE strkds AS (
SELECT str, postplz, postonm, kreis, regbez, land, 
AVG(ostwert) as ostwert,
AVG(nordwert) as nordwert
FROM  hauskds
GROUP BY str, postplz, postonm, kreis, regbez, land
);


-- set default value of hnr/adz to 0 in street coordinates table
ALTER TABLE strkds
ADD COLUMN hnr integer default 0,
ADD COLUMN adz text default '';

-- insert strkds to hauskds
-- the order of columns must align with that in hauskds!!!
INSERT INTO hauskds
SELECT str, hnr, adz, postplz, postonm, kreis, regbez, land, ostwert,nordwert 
FROM strkds;

-- delete strkds 
DROP TABLE IF EXISTS strkds;

-- -- merge hauskds and strkds
-- CREATE TABLE hauskds_full AS (
-- SELECT * FROM hauskds 
--   NATURAL FULL OUTER JOIN strkds
--   ORDER BY str,hnr
-- );

-- -- delete sub tables
-- DROP TABLE hauskds,strkds;

-- rename hauskds_full to hauskds
-- ALTER TABLE hauskds_full RENAME TO hauskds;

-- -- Display the first rows of a data table
-- SELECT * FROM hauskds LIMIT 10;


-- add unique id for each row.
ALTER TABLE hauskds
ADD COLUMN id serial primary key;



-- set coordinate reference system code (EPSG ID): UTM32N on WGS84
\set CRS 32632

\set CRS_WGS84 4326


--  add a geom point column
--  add geom_wgs84 point column
--  add latitude and longitude columns

ALTER TABLE hauskds
ADD COLUMN geom GEOMETRY(Point, :CRS),
ADD COLUMN geom_wgs84 GEOMETRY(Point, :CRS_WGS84),
ADD COLUMN latitude double precision,
ADD COLUMN longitude double precision;



-- convert coordinates (ostwert,nordwert) into point
UPDATE hauskds
SET geom = ST_SetSRID(ST_MakePoint(ostwert, nordwert), :CRS);


-- reproject POINTS from UTM32N to WGS84,
UPDATE hauskds
SET geom_wgs84 = ST_Transform(geom, :CRS_WGS84);


-- store latitude and longitude of house coordinates
UPDATE hauskds
SET longitude = ST_X(geom_wgs84), 
latitude = ST_Y(geom_wgs84);



-- create index on data table based on spatial data
CREATE INDEX hauskds_gix
  ON hauskds 
  USING GIST(geom);




-- example: Querying for nearby points
-- SELECT *
-- FROM hauskds
-- WHERE ST_DWithin(
--   geom,
--   ST_SetSRID(ST_MakePoint(715493,5324334),:CRS),
--   500
-- );


-- -- list indices in data table
-- SELECT indexname, indexdef
-- FROM pg_indexes
-- WHERE tablename = 'hauskds';















