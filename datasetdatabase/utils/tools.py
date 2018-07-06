#!/usr/bin/env python

# installed
from typing import Union
import pyarrow as pa
import pandas as pd
import pathlib
import sys
import os

# self
from ..utils import checks

# disable prints
def block_print():
    sys.stdout = open(os.devnull, 'w')

# enable prints
def enable_print():
    sys.stdout = sys.__stdout__

def create_parquet_file(table: Union[pa.Table, pd.DataFrame],
                        path: Union[str, pathlib.Path, None] = None) \
                        -> pathlib.Path:
    # enforce types
    checks.check_types(table, [pa.Table, pd.DataFrame])
    checks.check_types(path, [str, pathlib.Path, type(None)])

    # convert table
    if isinstance(table, pd.DataFrame):
        table = pa.Table.from_pandas(table)

    # convert path
    if isinstance(path, type(None)):
        path = os.getcwd()
    if isinstance(path, str):
        path = pathlib.Path(path)

    if path.is_dir():
        path /= "custom_table.parquet"

    pa.parquet.write_table(table, path)

    return path
