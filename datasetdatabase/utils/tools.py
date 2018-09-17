#!/usr/bin/env python

# installed
from orator.exceptions.query import QueryException
from contextlib import contextmanager
from typing import Union
import _pickle as pickle
import pathlib
import hashlib
import types
import math
import sys
import os

# self
from ..utils import checks

# globals
BYTE_SIZES = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
ALLOWED_YES = ["y", "yes"]
ALLOWED_NO = ["n", "no"]
TOO_MANY_RETURN_VALUES = "Too many values returned from query expecting {n}."


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


def write_pickle(obj: object, path: Union[str, pathlib.Path]) -> pathlib.Path:
    # enforce types
    checks.check_types(obj, object)
    checks.check_types(path, [str, pathlib.Path])

    # convert types
    path = pathlib.Path(path)

    # expand path
    path = path.expanduser()
    path = path.resolve()

    # write
    with open(path, "wb") as write_out:
        pickle.dump(obj, write_out)

    return path


def read_pickle(path: Union[str, pathlib.Path]) -> object:
    # enforce types
    checks.check_types(path, [str, pathlib.Path])

    # convert types
    path = pathlib.Path(path)

    with open(path, "rb") as read_in:
        obj = pickle.load(read_in)

    return obj


def get_file_hash(path: Union[str, pathlib.Path],
    alg: types.BuiltinMethodType = hashlib.md5) -> str:
    # enforce types
    checks.check_types(path, [str, pathlib.Path])
    checks.check_types(alg, types.BuiltinMethodType)

    # convert types
    path = pathlib.Path(path)

    # block size
    BLOCKSIZE = 65536

    # block read
    alg = alg()
    with open(path, "rb") as read_in:
        file_buffer = read_in.read(BLOCKSIZE)
        while len(file_buffer) > 0:
            alg.update(file_buffer)
            file_buffer = read_in.read(BLOCKSIZE)

    # get hash
    return alg.hexdigest()


def get_object_hash(obj: object,
    alg: types.BuiltinMethodType = hashlib.md5) -> str:
    # enforce types
    checks.check_types(obj, object)
    checks.check_types(alg, types.BuiltinMethodType)

    # hash
    return alg(pickle.dumps(obj)).hexdigest()


def quick_cast(value, cast_type, info=None):
    try:
        if not isinstance(value, cast_type):
            return cast_type(value)
    except ValueError:
        raise ValueError("Could not cast:", value,
                         "to:", cast_type,
                         "\ninfo:", info)

    return value


def _assert_value(f, val, err):
    # get pass fail
    assert f(val), err


def get_items_from_db_table(db, table, conditions):
    # construct orator table
    table = db.table(table)

    # expand multiple conditions
    if all(isinstance(cond, list) for cond in conditions):
        for cond in conditions:
            table = table.where(*cond)
    # expand single condition
    else:
        table = table.where(*conditions)

    # get table
    table = table.get()

    # format
    return [dict(item) for item in table]


def insert_to_db_table(db, table, items):
    # create conditions
    conditions = [[k, "=", v] for k, v in items.items() if k != "Created"]

    # check exists
    found_items = get_items_from_db_table(db, table, conditions)

    # not found
    if len(found_items) == 0:
        # handle multiprocessed inserts
        try:
            id = db.table(table)\
                .insert_get_id(items, sequence=(table + "Id"))
            id_condition = [table + "Id", "=", id]
            return get_items_from_db_table(db, table, id_condition)[0]
        except QueryException:
            return get_items_from_db_table(db, table, conditions)[0]

    # found
    if len(found_items) == 1:
        return found_items[0]

    # database structure error
    raise ValueError(TOO_MANY_RETURN_VALUES.format(n=1))
