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
import uuid
import sys
import os

# self
from .fmsinterface import FMSInterface
from ...utils import checks
from ...utils import tools

# globals
STORAGE_USER = "dsdb_storage"
CONNECTION_OPTIONS = {"user": STORAGE_USER, "storage_location": None}
STORAGE_LOCATION_IS_NOT_DIR = "Storage location must be an existing directory."


class QuiltFMS(FMSInterface):
    def __init__(self,
        connection_options: Union[dict, None] = None):

        # enforce types
        checks.check_types(connection_options, [dict, type(None)])

        # set or merge defaults with provided
        if connection_options is None:
            self._connection_options = CONNECTION_OPTIONS
        else:
            self._connection_options = {**CONNECTION_OPTIONS,
                                       **connection_options}

        # update storage user
        self.storage_user = self._connection_options["user"]
        self.set_storage_location(self._connection_options["storage_location"])


    @property
    def table_name(self):
        return "File"


    @property
    def storage_location(self):
        return self._storage_location


    def create_File(self, schema: orator.Schema):
        # enforce types
        checks.check_types(schema, orator.Schema)

        # create table
        if not schema.has_table("File"):
            with schema.create("File") as table:
                table.string("FileId")
                table.string("OriginalFilepath")
                table.string("Filetype").nullable()
                table.string("ReadPath")
                table.string("MD5").unique()
                table.string("SHA256").unique()
                table.string("Metadata").nullable()
                table.datetime("Created")


    def set_storage_location(self,
        storage_location: Union[str, pathlib.Path, None]):
        # enforce types
        checks.check_types(storage_location,
                           [str, pathlib.Path, type(None)])

        # update quilt store
        if storage_location is not None:
            # resolve path
            storage_location = pathlib.Path(storage_location)
            storage_location = storage_location.expanduser()
            storage_location = storage_location.resolve()
            assert storage_location.is_dir(), STORAGE_LOCATION_IS_NOT_DIR
            assert storage_location.exists(), STORAGE_LOCATION_IS_NOT_DIR
            self._storage_location = storage_location
            os.environ["QUILT_PRIMARY_PACKAGE_DIR"] = str(self.storage_location)
            os.environ["QUILT_PACKAGE_DIRS"] = str(self.storage_location)


    def get_file(self,
        db: orator.DatabaseManager,
        filepath: Union[str, pathlib.Path, None] = None,
        readpath: Union[str, pathlib.Path, None] = None,
        md5: Union[str, None] = None,
        sha256: Union[str, None] = None) -> Union[str, None]:
        # enforce types
        checks.check_types(db, orator.DatabaseManager)
        checks.check_types(filepath, [str, pathlib.Path, type(None)])
        checks.check_types(readpath, [str, pathlib.Path, type(None)])
        checks.check_types(md5, [str, type(None)])
        checks.check_types(sha256, [str, type(None)])

        # enforce at least one parameter given
        assert filepath is not None or \
               readpath is not None or \
               md5 is not None or \
               sha256 is not None, \
            "Provide filepath, an fms provided readpath, or a file hash."

        # try to find the fileid
        if md5 is not None:
            table = db.table("File").where("MD5", "=", md5).get()
        elif sha256 is not None:
            table = db.table("File").where("SHA256", "=", sha256).get()
        elif readpath is not None:
            table = db.table("File").where("ReadPath", "=", readpath).get()
        else:
            md5 = tools.get_file_hash(filepath)
            table = db.table("File").where("MD5", "=", md5).get()

        # try catch exists
        found = [dict(item) for item in table]
        try:
            found = found[0]
        except IndexError:
            found = None

        return found


    def get_or_create_file(self,
        db: orator.DatabaseManager,
        filepath: Union[str, pathlib.Path],
        metadata: Union[str, dict, None] = None) -> dict:

        # enforce types
        checks.check_types(db, orator.DatabaseManager)
        checks.check_types(filepath, [str, pathlib.Path])
        checks.check_types(metadata, [str, dict, type(None)])

        # convert types
        filepath = pathlib.Path(filepath)
        if isinstance(metadata, dict):
            metadata = str(dict)

        # check file exists
        checks.check_file_exists(filepath)

        # check exists
        md5 = tools.get_file_hash(filepath)
        sha256 = tools.get_file_hash(filepath, hashlib.sha256)
        file_info = self.get_file(db=db, md5=md5)

        # return if found
        if file_info is not None:
            return file_info

        name = "fms_" + md5

        # create if not
        with tools.suppress_prints():
            self._build_file_as_package(filepath, name)

        # import string
        read_pkg = importlib.import_module(name="quilt.data." +
                                                self.storage_user +
                                                "." +
                                                name)

        # file info dict
        file_info = {
            "FileId": str(uuid.uuid4()),
            "OriginalFilepath": str(filepath),
            "Filetype": filepath.suffix[1:],
            "ReadPath": read_pkg.load(),
            "MD5": md5,
            "SHA256": sha256,
            "Metadata": metadata,
            "Created": datetime.now()}

        # insert
        file_id = db.table("File").insert(file_info)

        return file_info


    def _build_file_as_package(self,
        filepath: Union[str, pathlib.Path],
        package_name: str) -> str:
        # enforce types
        checks.check_types(filepath, [str, pathlib.Path])
        checks.check_types(package_name, str)
        checks.check_file_exists(filepath)

        # convert types
        filepath = pathlib.Path(filepath)
        filepath = filepath.expanduser()
        filepath = filepath.resolve()

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
        print(full_package_name, file=sys.stderr)
        quilt.build(full_package_name, str(temp_write_loc))

        # remove the temp file
        os.remove(temp_write_loc)

        return full_package_name
