#!/usr/bin/env python

# installed
import pandas as pd

# self
from .introspector import Introspector
from .dictionary import DictionaryIntrospector
from .dictionary import reconstruct as reconstruct_dictionary
from .dataframe import DataFrameIntrospector
from .dataframe import reconstruct as reconstruct_dataframe
from .object import ObjectIntrospector
from .object import reconstruct as reconstruct_object

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

RECONSTRUCTOR_MAP = {
    OBJECT_MODULE: reconstruct_object,
    DATAFRAME_MODULE: reconstruct_dataframe,
    DICTIONARY_MODULE: reconstruct_dictionary
}
