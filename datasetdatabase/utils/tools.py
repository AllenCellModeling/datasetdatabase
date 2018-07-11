#!/usr/bin/env python

# installed
from contextlib import contextmanager
from typing import Union
import pandas as pd
import pathlib
import pickle
import time
import sys
import os

# self
from ..utils import checks


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
