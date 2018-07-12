#!/usr/bin/env python

# installed
from contextlib import contextmanager
from typing import Union
import pandas as pd
import pathlib
import pickle
import time
import math
import sys
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
    with open(str(path), "wb") as write_out:
        pickle.dump(table, write_out, protocol=2)

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
