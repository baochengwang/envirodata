# from https://github.com/mediagis/nominatim-docker/tree/master/4.5

# REPLICATION_URL omitted intentionally, breaks offline...
docker run -it \
  -e PBF_URL=https://download.geofabrik.de/europe/germany-latest.osm.pbf \
  -p 8080:8080 \
  --name nominatim \
  mediagis/nominatim:4.5

# nomatim pw for reference: qaIACxO6wMR3