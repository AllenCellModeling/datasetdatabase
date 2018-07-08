#!/usr/bin/env python

# installed
from datetime import datetime
from typing import Union
import pyarrow as pa
import pandas as pd
import pathlib
import inspect
import getpass
import orator
import types
import os

# self
from ..schema.filemanagers import quiltfms
from ..schema.filemanagers import aicsfms
from ..version import __version__
from ..schema import minimal
from ..utils import checks
from ..utils import tools


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
                 fms: types.ModuleType = quiltfms):
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
            Custom modules must contain a TABLES global variable, and functions:
            create_schema, drop_schema, add_basic_info, and get_tables.

            Default: datasetdatabase.schema.minimal

        fms: module
            How should files be properly stored, versioned, and retrieved.
            Can pass your own modules here for extended fms modules.
            Custom modules must contain functions: set_storage_location,
            get_or_create_fileid, and get_readpath_from_fileid.

            Default: datasetdatabase.schema.filemanagers.quiltfms

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
        checks.check_types(fms, types.ModuleType)
        self.user = checks.check_user(user)

        # construct basic items
        self.database = orator.DatabaseManager(config)
        self.schema = orator.Schema(self.database)
        self.driver = config[list(config.keys())[0]]["driver"]
        self.tables = version.get_tables(self)
        self.fms = fms

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


    def get_items_from_table(self,
                             table: str,
                             conditions: Union[list, None]) -> dict:
        # enforce types
        checks.check_types(table, str)
        checks.check_types(conditions, list)

        # construct orator table
        items = self.db.table(table)

        # was passed multiple conditions
        if all(isinstance(cond, list) for cond in conditions):
            for cond in conditions:
                items = items.where(cond)
        # was passed single condition
        else:
            items = items.where(conditions)

        # get items
        items = items.get()

        # format return
        if self.driver == "postgresql":
            items = [dict(r) for row in items]

        return items

    def insert_to_table(self, table: str, items: dict) \
        -> dict:

        # enforce types
        checks.check_types(table, str)
        checks.check_types(items, dict)

        # try catch ingest
        id = self.db.table(table).insert_get_id(items)
        info = get_items_from_table(table, [table + "Id", "=", id])[0]
        if self.driver == "postgresql":
            info = dict(info)

        return info



    def create_dataset(self, iotas: list) -> dict:
        return {}


    def _create_dataset(self, iotas: list) -> dict:
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

        # prep items for run ingest
        fname = inspect.currentframe().f_code.co_name
        alg_info = get_items_from_table("Algorithm",
                                        [["Name", "=", fname],
                                         ["Version", "=", __version__]])[0]
        user_info = get_items_from_table("User",
                                         ["Name", "=", self.user])[0]

        # items
        input_dataset_id = None
        alg_id = alg_info["AlgorithmId"]
        user_id = user_info["UserId"]
        name = "dataset ingestion"
        desc = None

        # begin run
        begin = datetime.now()

        ds_info = _upload_dataset(dataset,
                                  name,
                                  description,
                                  type_map,
                                  validation_map,
                                  import_as_type_map,
                                  upload_files,
                                  filepath_columns,
                                  replace_paths)

        output_dataset_id = ds_info["DatasetId"]

        # end run
        end = datetime.now()

        # process run
        run_info = insert_to_table(self,
                        input_dataset_id,
                        output_dataset_id,
                        alg_id,
                        user_id,
                        name,
                        desc,
                        begin,
                        end)

        return ds_info


    def _upload_dataset(self,
                        dataset: Union[str, pathlib.Path, pd.DataFrame],
                        name: Union[str, None] = None,
                        description: Union[str, None] = None,
                        type_map: Union[dict, None] = None,
                        validation_map: Union[dict, None] = None,
                        import_as_type_map: bool = False,
                        upload_files: bool = False,
                        filepath_columns: Union[str, list, None] = None,
                        replace_paths: dict = {"\\": "/"}) -> dict:

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

        # TODO:
        # the sourceid needs to be moved out to upload_dataset
        # input_dataset_id can be fms or run id
        # table refactor

        # get sourceid
        if isinstance(dataset, pd.DataFrame):
            ds_path = tools.create_parquet_file(dataset)
            sourceid = self.fms.get_or_create_fileid(ds_path)
            os.remove(ds_path)
        else:
            sourceid = self.fms.get_or_create_fileid(dataset)

        # read dataset
        if isinstance(dataset, pathlib.Path):
            dataset = pd.read_csv(dataset)

        # preprocess prep dataset
        if import_as_type_map:
            dataset = tools.format_dataset(dataset, type_map)
        dataset = tools.format_paths(dataset, filepath_columns, replace_paths)

        # check and validate dataset
        checks.validate_dataset(dataset, type_map, filepath_columns)
        checks.enforce_values(dataset, validation_map)

        # dataset name check and creation
        found_ds = get_items_from_table("Dataset", ["Name", "=", name])
        if len(found_ds) > 0:
            name += "@@" + str(datetime.now())

        # user add
        if len(get_items_from_table("User", ["Name", "=", self.user])) == 0:
            insert_to_table("User", {"Name": self.user,
                                    "Created": datetime.now()})

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
