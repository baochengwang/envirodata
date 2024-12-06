docker network create -d bridge envirodata_network
docker network connect envirodata_network nominatim
docker network connect envirodata_network brightsky-web-1

docker run --rm --mount source=envirodata_cache,target=/cache --network envirodata_network mbees/envirodata:latest load_data config.yaml
