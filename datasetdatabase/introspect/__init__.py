#!/usr/bin/env python

# installed
import pandas as pd

# self
from .introspector import Introspector
from .dictionary import DictionaryIntrospector
from .dataframe import DataFrameIntrospector
from .object import ObjectIntrospector

# globals
DATAFRAME_MODULE = \
"datasetdatabase.introspect.dataframe.DataFrameIntrospector"
DICTIONARY_MODULE = \
"datasetdatabase.introspect.dictionary.DictionaryIntrospector"
OBJECT_MODULE = \
"datasetdatabase.introspect.object.ObjectIntrospector"

INTROSPECTOR_MAP = {
    OBJECT_MODULE: ObjectIntrospector,
    pd.DataFrame: DataFrameIntrospector,
    DATAFRAME_MODULE: DataFrameIntrospector,
    dict: DictionaryIntrospector,
    DICTIONARY_MODULE: DictionaryIntrospector
}
