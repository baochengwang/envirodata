# from https://github.com/mediagis/nominatim-docker/tree/master/4.4

docker run -it \
  -e PBF_URL=https://download.geofabrik.de/europe/germany-latest.osm.pbf \
  -e REPLICATION_URL=https://download.geofabrik.de/europe/germany-updates/ \
  -p 8080:8080 \
  --name nominatim \
  mediagis/nominatim:4.4

# nomatim pw for reference: qaIACxO6wMR3