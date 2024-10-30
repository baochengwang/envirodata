docker run -d --name envirodata --mount source=envirodata_cache,target=/cache --network envirodata_network -p 8000:8000 mbees/envirodata:latest run_server config.yaml
