#!/bin/sh

docker run --rm -it \
    -p 8080:8080 \
    -e DOCKER_USER="$USER" \
    datasetdatabase_build \
    bash -c "jupyter lab --port 8080"
