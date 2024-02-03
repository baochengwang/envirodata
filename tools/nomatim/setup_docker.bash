# https://github.com/mediagis/nominatim-docker/tree/master/4.3

docker run -it \
  -e PBF_URL=https://download.geofabrik.de/europe/germany/bayern-latest.osm.pbf \
  -e REPLICATION_URL=https://download.geofabrik.de/europe/germany/bayern-updates/ \
  -p 8080:8080 \
  --name nominatim \
  mediagis/nominatim:4.3

# http://localhost:8080/search.php?q=avenue%20pasteur

# nomatim pw qaIACxO6wMR3