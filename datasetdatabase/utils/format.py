#!/usr/bin/env python

# installed
from typing import Union
import pandas as pd
import types

# self
from ..utils import checks


def quick_cast(value, cast_type):
    try:
        if not isinstance(value, cast_type):
            return cast_type(value)
    except ValueError:
        raise ValueError("Could not cast:", value, "to:", cast_type)

    return value


def format_dataset(dataset: pd.DataFrame,
                   type_map: Union[dict, None] = None) -> pd.DataFrame:
    # enforce types
    checks.check_types(dataset, pd.DataFrame)
    checks.check_types(type_map, [dict, type(None)])

    # format data if type_map was passed
    if type_map is not None:
        for key, ctype in type_map.items():
            dataset[key] = dataset[key].apply(lambda v: quick_cast(v, ctype))

    return dataset


def format_path(fp: str, replace_items: dict) -> str:
    # enforce types
    checks.check_types(fp, str)
    checks.check_types(replace_path, dict)

    # keep replacing until no changes remain
    for cur, new in replace_items.items():
        fp = fp.replace(cur, new)

    return fp


def format_paths(dataset: pd.DataFrame,
                 filepath_columns: Union[str, list, None],
                 replace_paths: Union[dict, None]) -> pd.DataFrame:
    # enforce types
    checks.check_types(dataset, pd.DataFrame)
    checks.check_types(filepath_columns, [str, list, type(None)])
    checks.check_types(replace_paths, [dict, type(None)])

    # early return
    if filepath_columns is None or \
       replace_path is None:
        return dataset

    # convert types
    if isinstance(filepath_columns, str):
        filepath_columns = [filepath_columns]

    # apply changes
    dataset[filepath_columns] = dataset[filepath_columns] \
                .applymap(lambda fp: change_path(fp))

    return dataset
