# from https://github.com/mediagis/nominatim-docker/tree/master/4.5

# REPLICATION_URL omitted intentionally, breaks offline...
docker run -it --shm-size=1g \
  -e PBF_URL=https://download.geofabrik.de/europe/germany-latest.osm.pbf \
  -e FREEZE=true \
  -e IMPORT_WIKIPEDIA=true \
  -p 8080:8080 \
  -v nominatim-data:/var/lib/postgresql/16/main \
  --name nominatim \
  mediagis/nominatim:4.5

# nomatim pw for reference: qaIACxO6wMR3

#wget https://nominatim.org/data/wikimedia-importance.sql.gz
#docker cp wikimedia-importance.sql.gz nominatim:/app
#docker exec -it nominatim sudo mkdir /app/tokenizer
#docker exec -it nominatim sudo -u nominatim nominatim refresh --wiki-data --importance