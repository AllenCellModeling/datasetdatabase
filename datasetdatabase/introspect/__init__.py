#!/usr/bin/env python

# installed
import pandas as pd

# self
from .introspector import Introspector
from .dictionary import DictionaryIntrospector
from .dataframe import DataFrameIntrospector
from .object import ObjectIntrospector

INTROSPECTOR_MAP = {
    pd.DataFrame: DataFrameIntrospector,
    dict: DictionaryIntrospector
}
