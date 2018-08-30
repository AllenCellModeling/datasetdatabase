FROM jupyter/minimal-notebook

# upgrade pip
RUN pip install --upgrade pip

# expose for notebook/ lab
EXPOSE 8888

# install lab
RUN pip install \
    jupyterlab \
    jupyterlab-launcher

# set dir
WORKDIR /active

# attach current head to bash activate
RUN echo \
    "pip install git+https://github.com/AllenCellModeling/datasetdatabase.git" \
    >> ~/.bashrc
