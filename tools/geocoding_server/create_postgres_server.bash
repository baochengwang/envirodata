# first - run trim_hauskoordinaten.sh to create "hauskoordinaten.csv" in current folder

USER=envirodata
PASSWORD=envirodata
HOST=localhost

# extend official postgres image with postgis
cat > Dockerfile << EOF
FROM postgres

RUN apt-get update
RUN apt install postgis -y
EOF

docker build -t envirodata/postgis:latest .

# run local postgis server in docker container
docker run --name envirodata -e POSTGRES_PASSWORD=$PASSWORD -e POSTGRES_USER=$USER -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 -d envirodata/postgis:latest

psql -U $USER -p $PASSWORD -h localhost -f create_dbs.sql

# create and fill geocoding database
psql -U $USER -p $PASSWORD -h localhost -d geocoding -f setup_geocoder_db.sql

# enable fuzzy matching
psql -U $USER -p $PASSWORD -h localhost -d geocoding -f setup_fuzzy_matching.sql
