#!/usr/bin/env python

# installed
from typing import Union
import pandas as pd
import numpy as np
import types

# self
from ..utils import checks

# globals
NATIVE_SUPPORT_TYPES = ["int", "float", "bool",
                        "list", "tuple", "dict", "set"]


def cast(value: str, value_type: str, **kwargs):
    # check types
    checks.check_types(value, str)
    checks.check_types(value_type, str)

    # prep the type string
    value_type = value_type.replace("<class '", "")[:-2]

    # cast
    if "numpy.ndarray" in value_type:
        value = value.replace("[", "")
        value = value.replace("]", "")
        result = np.fromstring(value, sep=" ", **kwargs)
    elif "str" in value_type:
        result = eval(value_type + "('{v}')".format(v=value))
    elif value_type in NATIVE_SUPPORT_TYPES:
        result = eval(value_type + "({v})".format(v=value))
    else:
        result = value

    return result


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

    err = "Dataset failed inherent cast as type map at:\n\tcol:{c}\n\trow:{r}"

    # format data if type_map was passed
    if type_map is not None:
        for i, row in dataset.iterrows():
            for key, value in dict(row).items():
                if key in type_map:
                    try:
                        dataset[key][i] = quick_cast(value, type_map[key])
                    except (ValueError, TypeError):
                        raise TypeError(err.format(c=key, r=i))

    return dataset


def format_path(fp: str, replace_items: dict) -> str:
    # enforce types
    checks.check_types(fp, str)
    checks.check_types(replace_items, dict)

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
       replace_paths is None:
        return dataset

    # convert types
    if isinstance(filepath_columns, str):
        filepath_columns = [filepath_columns]

    # apply changes
    dataset[filepath_columns] = dataset[filepath_columns] \
                .applymap(lambda fp: format_path(fp, replace_paths))

    return dataset


def convert_dataset_to_dataframe(dataset: pd.DataFrame,
                                 get_info_items: bool = False,
                                 **kwargs) -> pd.DataFrame:

    # enforce types
    checks.check_types(dataset, pd.DataFrame)
    checks.check_types(get_info_items, bool)

    # join each Iota row into a dataframe row
    rows = {}
    for i, row in dataset.iterrows():
        items = dict(row)

        # basic items
        group = {items["Key"]: cast(items["Value"],
                                    items["ValueType"],
                                    **kwargs)}

        # info items
        if get_info_items:
            group[(items["Key"] + "(Type)")] = items["ValueType"]
            group[(items["Key"] + "(IotaId)")] = items["IotaId"]
            group[(items["Key"] + "(SourceId)")] = items["SourceId"]
            group[(items["Key"] + "(SourceTypeId)")] = items["SourceTypeId"]

        # first item of row
        if items["GroupId"] not in rows:
            rows[items["GroupId"]] = group

        # additional items of already created row
        else:
            for key, item in group.items():
                rows[items["GroupId"]][key] = item

    # format dataframe
    rows = list(rows.values())
    rshp = "(Reshape)"
    reshape_cols = [c.replace(rshp, "") for c in rows[0].keys() if rshp in c]

    if len(reshape_cols) > 0:
        for row in rows:
            for key, val in row.items():
                if key in reshape_cols:
                    row[key] = np.reshape(val, row[key + rshp])

    reshape_cols = [c + rshp for c in reshape_cols]

    # return formatted
    return pd.DataFrame(rows).drop(columns=reshape_cols)
