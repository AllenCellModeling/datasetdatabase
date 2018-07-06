#!/usr/bin/env python

# installed
from datetime import datetime
from typing import Union
import pandas as pd
import pathlib
import getpass
import orator
import types

# self
from ..schema import quiltfile
from ..schema import aicsfms
from ..schema import minimal
from ..utils import checks


class DatasetDatabase(object):
    """
    A loaded connection to a sqlite or postgresql database that can be used as
    a Dataset Database. This will handle the storage, versioning, tracking, and
    retrieval of datasets in any arbitrary form.
    """

    def __init__(self,
                 config: dict,
                 build: bool = True,
                 user: Union[str, None] = None,
                 version: types.ModuleType = minimal,
                 file_handler: types.ModuleType = quiltfile):
        """
        Create a DatasetDatabase connection.

        A DatasetDatabase is an object you will use to connect to and interact
        with, and additionally build, a sqlite or postgresql database that you
        would like to use as a Datase Database. This object will handle the
        storage, versioning, tracking, and retrieval of datasets in any
        arbitrary form.

        Example
        ==========
        ```
        >>> mngr = ConnectionManager("local")
        >>> DatasetDatabase(mngr["local"])
        ```

        Parameters
        ==========
        config: dict
            The database config dictionary that will be passed to an
            orator.DatabaseManager object. Recommended to use ConnectionManager
            to ensure the config is well constructed as well as provided
            additional functionality.

        build: bool
            Should the tables contained in the version module be constructed.

            Default: True (construct the tables on initialization)

        user: str, None
            A string username.

            Default: None (getpass.getuser())

        version: module
            Which dataset database version do you want to construct and
            interact with. Can pass your own modules here for extended versions.
            You module must contain a TABLES global variable, and functions:
            create_schema, drop_schema, add_basic_info, and get_tables.

            Default: datasetdatabase.schema.minimal

        Returns
        ==========
        self

        Errors
        ==========

        """

        # enforce types
        checks.check_types(config, dict)
        checks.check_types(build, bool)
        checks.check_types(user, [str, type(None)])
        checks.check_types(version, types.ModuleType)
        self.user = checks.check_user(user)

        # construct basic items
        self.database = orator.DatabaseManager(config)
        self.schema = orator.Schema(self.database)
        self.driver = config[list(config.keys())[0]]["driver"]
        self.tables = version.get_tables(self)

        # create tables
        if build:
            self.construct_tables(version)

        # recent since connection made
        self.recent = []

    def construct_tables(self, version: types.ModuleType = minimal):
        """
        Create the tables found in the version module.

        Parameters
        ==========
        version: module
            Which version of the dataset database you would like to construct.

            Default: datasetdatabase.schema.minimal

        Returns
        ==========

        Errors
        ==========

        """

        # enforce types
        checks.check_types(version, types.ModuleType)

        # construct tables
        version.create_schema(self)

        # add basic info
        version.add_basic_info(self)

        # update table map
        self.tables = version.get_tables(self)


    def create_dataset(self, iotas: list) -> dict:
        return {}

    def upload_dataset(self,
                       dataset: Union[str, pathlib.Path, pd.DataFrame],
                       name: Union[str, None] = None,
                       description: Union[str, None] = None,
                       type_map: Union[dict, None] = None,
                       validation_map: Union[dict, None] = None,
                       import_as_type_map: bool = False,
                       upload_files: bool = False,
                       filepath_columns: Union[str, list, None] = None,
                       replace_paths: dict = {"\\": "/"}) -> dict:

        # enforce types
        checks.check_types(dataset, [str, pathlib.Path, pd.DataFrame])
        checks.check_types(name, [str, type(None)])
        checks.check_types(description, [str, type(None)])
        checks.check_types(type_map, [dict, type(None)])
        checks.check_types(validation_map, [dict, type(None)])
        checks.check_types(import_as_type_map, bool)
        checks.check_types(upload_files, bool)
        checks.check_types(filepath_columns, [str, list, type(None)])
        checks.check_types(replace_paths, dict)

        # TODO:
        # complete ingest change
        # this counts as a Run
        # algorithm being this function of this git commit

        # convert types
        if isinstance(dataset, str):
            dataset = pathlib.Path(dataset)

        if isinstance(filepath_columns, str):
            filepath_columns = [filepath_columns]

        # check dataset name
        if name is None:
            if isinstance(dataset, pathlib.Path):
                name = str(dataset) + "@@" + str(datetime.now())
            else:
                name = user + "@@" + str(datetime.now())

        # read dataset
        if isinstance(dataset, pathlib.Path):
            dataset = pd.read_csv(dataset)

            # TODO:
            # handle fms upload and get guid

            # TODO:
            # get or create from table
            # id of File from SourceTypeTable

        else:
            pass
            # TODO:
            # get or create from table
            # id of Run from SourceType table

        # TODO:
        # create uploaded map
        # iter dataset, validate, cast, enforce, and add to db
        # if anything fails, rollback all created from upload map


        return


    def __getitem__(self, key):
        return


    def __str__(self):
        disp = (("-" * 31) + " DATASET DATABASE " + ("-" * 31))

        for name, tbl in self.tables.items():
            disp += ("\n" + ("-" * 80))

            disp += ("\n" + name + ":")
            if isinstance(tbl, orator.query.builder.QueryBuilder):
                disp += "\n\trows: {r}".format(r=tbl.count())
                disp += "\n\texample: {r}".format(r=tbl.first())
            else:
                disp += "\n\t{t}".format(t=type(tbl))

        disp += ("\n" + ("-" * 32) + " RECENTLY ADDED " + ("-" * 32))
        disp += ("\n" + ("-" * 80))
        for row in self.recent:
            disp += "\n{r}".format(r=row)

        return disp


    def __repr__(self):
        return str(self)
