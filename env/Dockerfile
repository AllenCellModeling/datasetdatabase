FROM jupyter/minimal-notebook

# update python
RUN conda install -c anaconda python=3.7

# update conda
RUN conda update --all

# upgrade pip
RUN pip install --upgrade pip

# expose for notebook/ lab
EXPOSE 8888

# install lab
RUN pip install \
    jupyterlab \
    jupyterlab-launcher

# requirements
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

# set dir
WORKDIR /active

# for checking
ENV IN_DOCKER=True

# attach setup script to bash
RUN echo "pip install -e /active" >> ~/.bashrc
RUN echo "clear" >> ~/.bashrc
