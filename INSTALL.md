## Configuration

`envirodata` is a python package, and provided for use as Docker container. It is configured through one configuration file, `config.yaml`, in `WORKPATH`.

It relies on two external pieces of software which are also provided as Docker containers:

- [Nominatim](https://nominatim.org): geocoder based on OpenStreetMap data
- [BrightSky](https://brightsky.dev): data extraction from the German Weather Service (DWD)

## Setup and preparation

1. Install [docker](https://www.docker.com), e.g. follow [these instructions](https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository) for Ubuntu. On Linux, all `docker` will need to be `sudo'ed` unless you are root.

2. Clone envirodata repo (need username or deploy token - contact us):

   `git clone https://git.rz.uni-augsburg.de/mmbees-git/envirodata.git`

3. Go to the directory where you cloned the repository in.

   `cd envirodata`

4. Setup and start dockerized Nomatim geocoder:

   `bash deploy/00_setup_nomatim_docker.bash`

5. Setup and start BrightSky DWD data provider (can be done in parallel with 4.):

   `bash deploy/01_setup_brightsky_api.bash`

6. Build envirodata docker container (can be done in parallel with 4.):

   `bash deploy/02_build_envirodata_docker.bash`

## Data caching

7. Start an Envirodata container and load data (can be started once step 6 is done):

  `bash deploy/03_run_loader_container.bash`

This might well take several hours.

## Running the service

8. Start the envirodata container

  `bash deploy/05_start_server_container.bash`

The Envirodata service is now running in background. You can access the user interface in your browser at

> [localhost:8000](http://localhost:8000/)

## Documentation

[Sphinx](https://www.sphinx-doc.org/en/master/) API documentation can be created using `tools/build_docs.bash`, and then be found at `docs/_build/index.html`.
