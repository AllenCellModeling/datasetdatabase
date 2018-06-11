FROM jupyter/minimal-notebook

RUN pip install --upgrade pip

EXPOSE 8888

RUN pip install \
    numpy \
    scipy \
    pandas \
    labkey \
    orator \
    pathlib \
    psycopg2-binary

WORKDIR /active

ENV in_docker=True

RUN echo "alias python=python3" >> ~/.bashrc
RUN echo "alias pip=pip3" >> ~/.bashrc
RUN echo "pip install -e /active" >> ~/.bashrc
RUN echo "clear" >> ~/.bashrc
