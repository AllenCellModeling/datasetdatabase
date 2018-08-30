#!/bin/sh

docker run --rm -it \
    -p 8888:8888 \
    -e DOCKER_USER="$USER" \
    datasetdatabase_build \
    bash -c "jupyter lab"
