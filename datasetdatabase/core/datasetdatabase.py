#!/usr/bin/env python

# installed
from datetime import datetime
from typing import Union
import types

# self
from .databaseconfig import DatabaseConfig
from .datasetinfo import DatasetInfo
from .dataset import Dataset
from ..utils import checks


class DatasetDatabase(object):
    def __init__(self,
                 config: DatasetConfig,
                 build: bool = True,
                 user: Union[str, None] = None,
                 version: types.ModuleType = minimal,
                 fms: types.ModuleType = quiltfms,
                 fms_connection_options: Union[dict, None] = None,
                 recent_size: int = 5):

        self.config = config
