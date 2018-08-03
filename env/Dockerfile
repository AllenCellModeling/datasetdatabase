FROM jupyter/minimal-notebook

RUN pip install --upgrade pip

EXPOSE 8888

RUN pip install \
    jupyterlab \
    jupyterlab-launcher

RUN pip install \
    numpy \
    quilt \
    pandas \
    orator \
    pathlib \
    networkx \
    matplotlib \
    psycopg2-binary \
    python-Levenshtein

WORKDIR /active

ENV IN_DOCKER=True

RUN echo "pip install -e /active" >> ~/.bashrc
RUN echo "clear" >> ~/.bashrc
