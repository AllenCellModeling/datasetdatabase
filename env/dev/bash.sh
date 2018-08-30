#!/bin/sh

docker run --rm -it \
    -p 8080:8080 \
    -e DOCKER_USER="$USER" \
    -v "$(dirname "$PWD")":/active \
    datasetdatabase_dev \
    bash
