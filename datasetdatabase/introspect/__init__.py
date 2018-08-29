#!/usr/bin/env python

# installed
import pandas as pd

# self
from .introspector import Introspector
from .dataframe import DataFrameInspector

INTROSPECTOR_MAP = {
    pd.DataFrame: DataFrameInspector
}
