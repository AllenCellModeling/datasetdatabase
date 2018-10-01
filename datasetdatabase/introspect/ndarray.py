#!/usr/bin/env python

# installed
from multiprocessing.dummy import Pool
from typing import Dict, List, Union
from datetime import datetime
from functools import partial
import _pickle as pickle
import numpy as np
import hashlib
import orator
import types
import os

# self
from ..schema.filemanagers import FMSInterface
from ..utils import checks, tools, ProgressBar
from .introspector import Introspector


class DataFrameIntrospector(Introspector):
    """
    Numpy array introspector. Ingest an ndarray, create and validate
    Iota, etc.
    """

    def __init__(self, obj: np.ndarray):
        self._obj = obj


    @property
    def obj(self):
        return self._obj
