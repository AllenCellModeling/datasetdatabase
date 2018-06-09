FROM ubuntu:18.04

RUN apt-get update && apt-get install -y \
    software-properties-common

RUN add-apt-repository universe

RUN apt-get update && apt-get install -y \
    git \
    curl \
    python3.6 \
    python3-pip \
    python3-dev

RUN pip3 install --upgrade pip

RUN pip3 install \
    ipython \
    ipython[all] \
    jupyter

RUN pip3 install \
    numpy \
    scipy \
    pandas \
    labkey \
    orator \
    pathlib \
    psycopg2-binary

# RUN echo "alias jupyter=jupyter notebook \
#            --ip=127.0.0.1 \
#            --allow-root" >> ~/.bashrc

WORKDIR /active

ENV in_docker=True

RUN echo "alias python=python3" >> ~/.bashrc
RUN echo "alias pip=pip3" >> ~/.bashrc
RUN echo "pip3 install -e /active" >> ~/.bashrc
RUN echo "clear" >> ~/.bashrc
