docker network create -d bridge envirodata_network
docker network connect envirodata_network nominatim
docker network connect envirodata_network brightsky-web-1
docker run -d --name envirodata --mount source=envirodata_cache,target=/cache --network envirodata_network -p 8000:8000 mbees/envirodata:latest run_server config.yaml
