#!/usr/bin/env python

# installed
from typing import Union
import pathlib
import orator
import abc


class FMSInterface(abc.ABC):
    @abc.abstractmethod
    def __init__(self, **kwargs):
        return

    @property
    @abc.abstractmethod
    def table_name(self):
        return

    @abc.abstractmethod
    def create_File(self, schema: orator.Schema):
        return

    @abc.abstractmethod
    def get_or_create_file(self, filepath: Union[str, pathlib.Path], metadata: Union[dict, None] = None):
        return
