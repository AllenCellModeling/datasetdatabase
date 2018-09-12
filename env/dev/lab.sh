#!/bin/sh

docker run --rm -it \
    -p 8080:8888 \
    -e DOCKER_USER="$USER" \
    -v "$(dirname $(dirname "$PWD"))":/active \
    datasetdatabase_dev \
    bash -c "jupyter lab"
