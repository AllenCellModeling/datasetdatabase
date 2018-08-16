#!/usr/bin/env python

# installed
from contextlib import contextmanager
from datetime import datetime
from typing import Union
import pandas as pd
import numpy as np
import pathlib
import pickle
import time
import math
import sys
import ast
import os

# self
from ..utils import checks

# globals
BYTE_SIZES = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
ALLOWED_YES = ["y", "yes"]
ALLOWED_NO = ["n", "no"]


@contextmanager
def suppress_prints():
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout


def convert_size(size_bytes):
    """
    Convert bytes to a more readable size.
    Credit: https://stackoverflow.com/questions/5194057/better-way-to-convert-file-sizes-in-python#answer-14822210
    """

    if size_bytes == 0:
        return "0B"

    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, BYTE_SIZES[i])


def get_yes_no_input(message: str = "") -> bool:
    while True:
        try:
            answer = str(input(message))
        except ValueError:
            print("Sorry, I didn't understand that.")
            continue

        answer = answer.lower()
        if answer in (ALLOWED_YES + ALLOWED_NO):
            if answer in ALLOWED_YES:
                return True

            return False
        else:
            print("Your response must not be one of the following: {a}"\
                    .format(a=(ALLOWED_YES + ALLOWED_NO)))


def write_dataset_readme(ds_info: dict,
                         path: Union[str, pathlib.Path, None] = None) \
                         -> pathlib.Path:
    # enforce types
    checks.check_types(ds_info, dict)
    checks.check_types(path, [str, pathlib.Path, type(None)])

    # convert types
    if isinstance(path, str):
        path = pathlib.Path(path)

    # construct readme
    # name
    readme = ""
    readme += "# " + ds_info["Name"] + ":"
    readme += "\n\n"

    # description
    if ds_info["Description"] is not None:
        readme += "## Description:"
        readme += "\n"
        readme += ds_info["Description"]
        readme += "\n\n"

    if ds_info["FilepathColumns"] is not None:
        readme += "## Filepath Columns:"
        readme += "\n"
        readme += ds_info["FilepathColumns"]
        readme += "\n\n"

    # created
    readme += "## Created:"
    readme += "\n"
    readme += str(ds_info["Created"])
    readme += "\n\n"

    # source
    readme += "## Origin SourceId:"
    readme += "\n"
    readme += str(ds_info["SourceId"])
    readme += "\n\n"

    # create path
    if path is None:
        path = os.getcwd()
        path = pathlib.Path(path)
        path /= (str(ds_info["SourceId"]) + "_readme.md")

    # ensure dirs
    try:
        os.makedirs(path.parent)
    except FileExistsError:
        pass

    # dump
    with open(path, "w") as write_out:
        write_out.write(readme)

    return path


def parse_readme(filepath: Union[str, pathlib.Path]) -> dict:
    # enforce types
    checks.check_types(filepath, [str, pathlib.Path])
    checks.check_file_exists(filepath)

    # convert types
    filepath = pathlib.Path(filepath)

    # read contents
    with open(filepath, "r") as read_in:
        lines = read_in.readlines()

    # default contents
    info = {"Description": None,
            "FilepathColumns": None}

    # parse contents
    for i, line in enumerate(lines):
        if line.startswith("## "):
            if "## Description:" in line:
                info["Description"] = lines[i + 1].replace("\n", "")
            if "## Filepath Columns:" in line:
                cols = lines[i + 1].replace("\n", "")
                info["FilepathColumns"] = ast.literal_eval(cols)

    return info
