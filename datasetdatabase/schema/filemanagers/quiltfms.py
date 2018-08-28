#!/usr/bin/env python

# installed
from datetime import datetime
from typing import Union
import importlib
import pathlib
import hashlib
import orator
import quilt
import yaml
import json
import os

# self
from .fmsinterface import FMSInterface
from ...utils import checks
from ...utils import tools

# globals
STORAGE_USER = "dsdb_storage"
CONNECTION_OPTIONS = {"user": STORAGE_USER, "storage_location": None}


class QuiltFMS(FMSInterface):

    def __init__(self,
                 connection_options: Union[dict, None] = None):

        # enforce types
        checks.check_types(connection_options, [dict, type(None)])

        # set or merge defaults with provided
        # if connection_options is None:
        #     self._connection_options = CONNECTION_OPTIONS
        # else:
        #     self._connection_options = {**CONNECTION_OPTIONS,
        #                                **connection_options}
        #
        # # update storage user
        # self.storage_user = self.connection_options["user"]
        # self.set_storage_location(self.connection_options["storage_location"])


    @property
    def table_name(self):
        return "File"


    def create_File(self, schema: orator.Schema):
        # enforce types
        checks.check_types(schema, orator.Schema)

        # create table
        if not schema.has_table("File"):
            with schema.create("File") as table:
                table.string("FileId")
                table.string("Filename")
                table.string("OriginalFilepath")
                table.string("FileType").nullable()
                table.string("ReadPath")
                table.string("MD5").unique()
                table.string("SHA256").unique()
                table.string("QuiltPackage").unique()
                table.string("Metadata").nullable()
                table.datetime("Created")

        # update FileSource and Algorithm relationships
        try:
            with schema.table("FileSource") as table:
                table.foreign("FileId") \
                     .references("FileId") \
                     .on("File")

            with schema.table("Algorithm") as table:
                table.foreign("FileId") \
                     .references("FileId") \
                     .on("File")
            
        except orator.exceptions.query.QueryException:
            pass


    def set_storage_location(self,
                             storage_location: Union[str, pathlib.Path, None]):
        # enforce types
        checks.check_types(storage_location,
                           [str, pathlib.Path, type(None)])

        # update quilt store
        if storage_location is not None:
            self.storage_location = storage_location
            checks.check_file_exists(self.storage_location)
            os.environ["QUILT_PRIMARY_PACKAGE_DIR"] = str(self.storage_location)
            os.environ["QUILT_PACKAGE_DIRS"] = str(self.storage_location)


    def get_hash(self, filepath: Union[str, pathlib.Path]) -> str:
        # enforce types
        checks.check_types(filepath, [str, pathlib.Path])
        checks.check_file_exists(filepath)

        # construct package_name
        hash = _hash_bytestr_iter(_file_as_blockiter(open(filepath, "rb")),
                                  hashlib.md5(),
                                  True)
        return hash


    def get_file(self,
                   filepath: Union[str, pathlib.Path, None] = None,
                   readpath: Union[str, pathlib.Path, None] = None,
                   hash: Union[str, None] = None) -> Union[str, None]:
        # enforce types
        checks.check_types(filepath, [str, pathlib.Path, type(None)])
        checks.check_types(readpath, [str, pathlib.Path, type(None)])
        checks.check_types(hash, [str, type(None)])

        # enforce at least one parameter given
        assert filepath is not None or \
               readpath is not None or \
               hash is not None, \
            "Provide filepath, an fms provided readpath, or a file hash."

        # try to find the fileid
        if hash is not None:
            found = self.dsdb.get_items_from_table("File",
                                                ["MD5", "=", hash])
        elif readpath is not None:
            found = self.dsdb.get_items_from_table("File",
                                                ["ReadPath", "=", readpath])
        else:
            hash = self.get_hash(filepath)
            found = self.dsdb.get_items_from_table("File",
                                                ["MD5", "=", hash])

        # try catch exists
        try:
            found = found[0]
        except IndexError:
            found = None

        return found


    def get_or_create_file(self,
                           filepath: Union[str, pathlib.Path],
                           metadata: Union[str, dict, None] = None) -> str:
        # enforce types
        checks.check_types(filepath, [str, pathlib.Path])
        checks.check_types(metadata, [str, dict, type(None)])

        # convert types
        filepath = pathlib.Path(filepath)
        if isinstance(metadata, dict):
            metadata = str(dict)

        # check file exists
        checks.check_file_exists(filepath)

        # check exists
        hash = self.get_hash(filepath)
        file_info = self.get_file(hash=hash)

        # return if found
        if file_info is not None:
            return file_info

        name = "fms_" + hash
        # create if not
        with tools.suppress_prints():
            pkg = self._build_file_as_package(filepath, name)

        # import string
        read_pkg = importlib.import_module(name="quilt.data." +
                                                self.storage_user +
                                                "." +
                                                name)

        file_info = self.dsdb._insert_to_table("File",
                                        {"OriginalFilepath": str(filepath),
                                         "Filetype": filepath.suffix\
                                                     .replace(".", ""),
                                         "ReadPath": read_pkg.load(),
                                         "MD5": hash,
                                         "QuiltPackage": pkg,
                                         "Metadata": metadata,
                                         "Created": datetime.now()})

        return file_info


    def _build_file_as_package(self,
                               filepath: Union[str, pathlib.Path],
                               package_name: str) -> str:
        # enforce types
        checks.check_types(filepath, [str, pathlib.Path])
        checks.check_types(package_name, str)
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
        full_package_name = self.storage_user + "/" + package_name
        quilt.build(full_package_name, str(temp_write_loc))

        # remove the temp file
        os.remove(temp_write_loc)

        return full_package_name
