#!/usr/bin/env python

# installed
from datetime import datetime
from typing import Union
import pathlib
import hashlib
import quilt
import yaml
import os

# self
from ...utils import checks
from ...utils import tools

# globals
STORAGE_USER = "dsdb_storage"


def set_storage_location(storage_path: Union[str, pathlib.Path, None] = None):
    # enforce types
    checks.check_types(storage_path, [str, pathlib.Path, type(None)])
    checks.check_file_exists(storage_path)

    # ensure string
    storage_path = str(storage_path)

    # update quilt store
    os.environ["QUILT_PRIMARY_PACKAGE_DIR"] = storage_path
    os.environ["QUILT_PACKAGE_DIRS"] = storage_path


def get_or_create_fileid(filepath: Union[str, pathlib.Path]) -> str:
    # enforce types
    checks.check_types(filepath, [str, pathlib.Path])
    checks.check_file_exists(filepath)

    # convert filepath
    filepath = pathlib.Path(filepath)

    # construct package_name
    package_name = _hash_bytestr_iter(
                                    _file_as_blockiter(open(filepath, "rb")),
                                    hashlib.md5(),
                                    True)
    package_name = "fms_" + filepath.suffix[1:] + "_" + package_name

    # check fileid exists
    quilt_store = quilt.tools.store.PackageStore()
    found_pkg = quilt_store.find_package(None, STORAGE_USER, package_name)

    # not found, build the package
    if found_pkg is None:
        with tools.suppress_prints():
            _build_file_as_package(filepath, package_name)

    return package_name


def get_readpath_from_fileid(fileid: str) -> pathlib.Path:
    # enforce types
    checks.check_types(fileid, str)

    # set full package name and load
    name = STORAGE_USER + "/" + fileid
    return quilt.load(name).load()


################################################################################
############################## PRIVATE FUNCTIONS ###############################
################################################################################


def _build_file_as_package(filepath: pathlib.Path, package_name: str):
    # enforce types
    checks.check_types(filepath, pathlib.Path)
    checks.check_file_exists(filepath)

    # construct manifest
    load = {}
    load["file"] = str(filepath)
    load["transform"] = "id"
    contents = {"load": load}
    node = {"contents": contents}

    # write temporary manifest
    temp_write_loc = pathlib.Path(os.getcwd())
    temp_write_loc /= "single_file.yml"
    with open(temp_write_loc, "w") as write_out:
        yaml.dump(node, write_out, default_flow_style=False)

    # create quilt node
    full_package_name = STORAGE_USER + "/" + package_name
    quilt.build(full_package_name, str(temp_write_loc))

    # remove the temp file
    os.remove(temp_write_loc)


# this hashing function is pulled from:
# https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file#answer-3431835
# addressing concerns:
# these hexdigests will only be used as unique file ids to detect if a file is
# new or not, no security issues
def _hash_bytestr_iter(bytesiter, hasher, ashexstr=False):
    for block in bytesiter:
        hasher.update(block)
    return (hasher.hexdigest() if ashexstr else hasher.digest())

def _file_as_blockiter(afile, blocksize=65536):
    with afile:
        block = afile.read(blocksize)
        while len(block) > 0:
            yield block
            block = afile.read(blocksize)
