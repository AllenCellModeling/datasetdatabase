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


class ProgressBar(object):
    """
    Print a progress bar to the console.
    Credit: https://gist.github.com/vladignatyev/06860ec2040cb497f0f3

    Changes to convert from single function to class for better state
    management and easier update.
    """

    def __init__(self, max: int, bar_length: int = 60):
        self.current = 0
        self.max = max
        self.start = time.time()

        # to calc
        self.bar_length = bar_length
        self.update_bar_str()
        self.remaining_h = 0
        self.remaining_m = 0
        self.remaining_s = 0
        self.update_time_str()


    def increment(self, add: int = 1):
        self.current += add
        self.draw()


    def draw(self):
        # calc bar and percent
        self.update_bar_str()
        # calc completion
        self.update_time_str()

        # draw
        disp = "[{b}] {p}% {c} / {m} {t}\r".format(b=self.bar,
                                                   p=self.percent,
                                                   c=self.current,
                                                   m=self.max,
                                                   t=self.time)
        sys.stdout.write("[{b}] {p}% {c} / {m} {t}\r")
        sys.stdout.flush()


    def update_bar_str(self):
        complete = self.current / float(self.max)
        self.percent = round(100.0 * complete, 1)
        filled_length = int(round(self.bar_length * complete))

        # bar
        self.bar = "=" * filled_length
        self.bar += "-" * (self.bar_length - filled_len)


    def update_time_str(self):
        if self.current > 0:
            # elapsed time
            duration = time.time() - self.start

            # average time per object
            avg_time = duration / self.current
            avg_time = (avg_time * (self.max - self.current))

            self.remaining_m, self.remaining_s = divmod(avg_time, 60)
            self.remaining_h, self.remaining_m = divmod(m, 60)

        self.time = "~ {h}:{m}:{s} remaining".format(h=self.remaining_h,
                                                     m=self.remaining_m,
                                                     s=self.remaining_s)


def print_progress(count: int, start_time: float, total: int):
    """
    Print a progress bar to the console.
    Credit: https://gist.github.com/vladignatyev/06860ec2040cb497f0f3

    Minor changes to include expected completion time.
    """

    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    duration = time.time() - start_time
    completion = '~ 0:0:0 remaining'
    if count > 0:
        avg_time = duration / count
        avg_time = (avg_time * (total - count))
        m, s = divmod(avg_time, 60)
        h, m = divmod(m, 60)
        completion = ('~ %d:%02d:%02d remaining' % (h, m, s))

    sys.stdout.write('[%s] %s%s (%s%s%s) %s\r' %
                     (bar, percents, '%', count, '/', total, completion))
    sys.stdout.flush()


def create_pickle_file(table: pd.DataFrame,
                       path: Union[str, pathlib.Path, None] = None) \
                       -> pathlib.Path:
    # enforce types
    checks.check_types(table, pd.DataFrame)
    checks.check_types(path, [str, pathlib.Path, type(None)])

    # convert path
    if isinstance(path, type(None)):
        path = os.getcwd()
    if isinstance(path, str):
        path = pathlib.Path(path)

    # custom file
    if path.is_dir():
        path /= "custom_dataframe.pkl"

    # dump object
    table.to_pickle(path)

    return path

def create_npy_file(arr: np.ndarray,
                    path: Union[str, pathlib.Path, None] = None) \
                    -> pathlib.Path:
    # enforce types
    checks.check_types(arr, np.ndarray)
    checks.check_types(path, [str, pathlib.Path, type(None)])

    # convert path
    if isinstance(path, type(None)):
        path = os.getcwd()
    if isinstance(path, str):
        path = pathlib.Path(path)

    # custom file
    if path.is_dir():
        path /= "custom_arr.npy"

    # dump object
    np.save(path, arr)

    return path


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
