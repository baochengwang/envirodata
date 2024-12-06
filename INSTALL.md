## Setup (administrator)

 1) Install [docker](https://www.docker.com), e.g. follow [these instructions](https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository) for Ubunutu. On Linux, all `docker` will need to be `sudo'ed` unless you are root.

 2) Clone envirodata repo (need username or deploy token - contact us):

    `git clone https://git.rz.uni-augsburg.de/mmbees-git/envirodata.git`

 3) Go to the directory where you cloned the repository in.

    `cd envirodata`

 4) Setup and start dockerized Nomatim geocoder:

   `bash deploy/00_setup_nomatim_docker.bash`

 5) Setup and start BrightSky DWD data provider:

   `bash deploy/01_setup_brightsky_api.bash`

 6) Build envirodata docker container:

   `bash deploy/02_build_envirodata_docker.bash`

## Data caching

Start an Envirodata container and load data

  `bash deploy/03_run_loader_container.bash`

This might well take several hours up to a day.

## Running the service

  `bash deploy/05_start_server_container.bash`

The Envirodata service now runs as a docker container. You can access the user interface in your browser at

[localhost:8000](http://localhost:8000/)

## Configuration

`envirodata` is configured through one configuration file, `config.yaml`, in `WORKPATH`.

