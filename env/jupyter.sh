#!/bin/sh

IMAGE="modeling_db"
MOUNTDIR="$(dirname "$PWD")"
ROUTEPORT="-p 8080:8888"

if [ "$1" = "-h" ]
then
    echo "connect and run the modeling_db docker image.

    help:
        -m          a full os path to a directory or file,
                    '.' for current directory,
                    empty for no mount

        -p          a port forward in the form of {local}:{virtual}
                    empty for no port forward

        -i          which docker image to use
                    empty for modeling_db image
    "

else
    if [ "$1" = "-p" ]
    then
        ROUTEPORT="-p $2:8888"
    elif [ "$1" = "-m" ]
    then
        MOUNTDIR="$2"
    elif [ "$1" = "-i" ]
    then
        IMAGE="$2"
    fi

    if [ "$3" = "-p" ]
    then
        ROUTEPORT="-p $4:8888"
    elif [ "$3" = "-m" ]
    then
        MOUNTDIR="$4"
    elif [ "$3" = "-i" ]
    then
        IMAGE="$4"
    fi

    if [ "$5" = "-p" ]
    then
        ROUTEPORT="-p $6:8888"
    elif [ "$5" = "-m" ]
    then
        MOUNTDIR="$6"
    elif [ "$5" = "-i" ]
    then
        IMAGE="$6"
    fi

    docker run --rm -it \
        -e "PASSWORD=password" \
        $ROUTEPORT \
        -v /Users/$USER/Documents/dbconnect:/database \
        -v $MOUNTDIR:/active \
        $IMAGE \
        bash -c "jupyter notebook"
fi
