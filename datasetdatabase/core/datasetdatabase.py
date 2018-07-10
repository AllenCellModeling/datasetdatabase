#!/usr/bin/env python

# installed
from datetime import datetime
from typing import Union
import pandas as pd
import numpy as np
import pathlib
import inspect
import getpass
import orator
import types
import time
import os

# self
from ..schema.filemanagers import quiltfms
from ..schema.filemanagers import aicsfms
from ..version import __version__
from ..schema import minimal
from ..utils import format
from ..utils import checks
from ..utils import tools

# globals
UNIX_REPLACE = {"\\": "/"}


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
                 fms: types.ModuleType = quiltfms,
                 cache_size: int = 10):
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

        cache_size: int
            How many items should be stored in the recently created list.

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
        checks.check_types(cache_size, int)
        self.user = checks.check_user(user)

        # construct basic items
        self.cache_size = cache_size
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
        items = self.database.table(table)

        # was passed multiple conditions
        if all(isinstance(cond, list) for cond in conditions):
            for cond in conditions:
                items = items.where(*cond)
        # was passed single condition
        else:
            items = items.where(*conditions)

        # get items
        items = items.get()

        # format return
        if self.driver == "postgresql":
            items = [dict(r) for row in items]

        return items


    def insert_to_table(self, table: str, items: dict) -> dict:
        # enforce types
        checks.check_types(table, str)
        checks.check_types(items, dict)

        # try catch ingest
        try:
            id = self.database.table(table).insert_get_id(items,
                sequence=(table + "Id"))
        except Exception as e:
            checks.check_ingest_error(e)

        # get row
        info = self.get_items_from_table(table, [table + "Id", "=", id])[0]

        # format row
        if self.driver == "postgresql":
            info = dict(info)

        # update recent
        self.recent.append(info)
        if len(self.recent) > self.cache_size:
            self.recent = self.recent[:self.cache_size]

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
                       value_validation_map: Union[dict, None] = None,
                       import_as_type_map: bool = False,
                       upload_files: bool = False,
                       filepath_columns: Union[str, list, None] = None,
                       replace_paths: Union[dict, None] = UNIX_REPLACE) -> dict:

        # enforce types
        checks.check_types(dataset, [str, pathlib.Path, pd.DataFrame])
        checks.check_types(name, [str, type(None)])
        checks.check_types(description, [str, type(None)])
        checks.check_types(type_map, [dict, type(None)])
        checks.check_types(value_validation_map, [dict, type(None)])
        checks.check_types(import_as_type_map, bool)
        checks.check_types(upload_files, bool)
        checks.check_types(filepath_columns, [str, list, type(None)])
        checks.check_types(replace_paths, dict)

        # convert types
        if isinstance(dataset, str):
            dataset = pathlib.Path(dataset)
        if isinstance(filepath_columns, str):
            filepath_columns = [filepath_columns]

        # get file_id
        if isinstance(dataset, pd.DataFrame):
            ds_path = tools.create_parquet_file(dataset)
            file_id = self.fms.get_or_create_fileid(ds_path)
            os.remove(ds_path)
        else:
            file_id = self.fms.get_or_create_fileid(dataset)

        # check dataset name
        if name is None:
            name = file_id

        # return if dataset already exists
        found_ds = self.get_items_from_table("Dataset", ["Name", "=", name])
        if len(found_ds) > 0:
            return found_ds[0]

        # read dataset
        if isinstance(dataset, pathlib.Path):
            dataset = pd.read_csv(dataset)

        # prep items for run ingest
        fname = inspect.currentframe().f_code.co_name
        alg_info = self.get_items_from_table("Algorithm",
                            [["Name", "=", fname],
                             ["Version", "=", __version__]])[0]
        source_type_info = self.get_items_from_table("SourceType",
                            ["Name", "=", "FileSource"])[0]

        # create or get user
        user_info = self.get_items_from_table("User", ["Name", "=", self.user])
        if len(user_info) == 0:
            user_info = self.insert_to_table("User",
                            {"Name": self.user,
                             "Created": datetime.now()})
        else:
            user_info = user_info[0]

        # create or get source
        source_info = self.get_items_from_table("Source",
                            ["Name", "=", file_id])
        if len(source_info) == 0:
            source_info = self.insert_to_table("Source",
                            {"SourceTypeId": source_type_info["SourceTypeId"],
                             "Name": file_id,
                             "Created": datetime.now()})
        else:
            source_info = source_info[0]

        # create or get file source
        file_source_info = self.get_items_from_table("FileSource",
                            ["FileId", "=", file_id])
        if len(file_source_info) == 0:
            file_source_info = self.insert_to_table("FileSource",
                            {"SourceId": source_info["SourceId"],
                             "FileId": file_id})
        else:
            file_source_info = file_source_info[0]

        # items
        input_dataset_id = None
        source_id = source_info["SourceId"]
        alg_id = alg_info["AlgorithmId"]
        user_id = user_info["UserId"]
        run_name = "dataset ingestion"
        desc = "{f} for {n}".format(f=fname, n=name)

        # begin run
        begin = datetime.now()

        # preprocess prep dataset
        if import_as_type_map:
            dataset = format.format_dataset(dataset, type_map)
        if filepath_columns is not None or \
           replace_paths is not None:
            dataset = format.format_paths(dataset,
                                          filepath_columns,
                                          replace_paths)

        if upload_files:
            # TODO:
            # get duplication size
            # verify duplication size w/ user
            # store objects
            pass

        # validate dataset
        checks.validate_dataset_types(dataset, type_map, filepath_columns)
        checks.validate_dataset_values(dataset, value_validation_map)

        # begin iota creation
        print("Creating Iota...")

        iota = {}
        current_i = 0
        start_time = time.time()
        total_upload = len(dataset)
        for groupid, row in dataset.iterrows():
            # update progress
            tools.print_progress(count=current_i,
                                 start_time=start_time,
                                 total=total_upload)

            # parse row
            r = dict(row)
            for key, value in r.items():
                if isinstance(value, str):
                    value = value.replace("\n", "")

                # basic items
                to_add = {"GroupId": groupid,
                          "Key": str(key),
                          "Value": str(value),
                          "ValueType": str(type(value)),
                          "Created": datetime.now()}

                # create extra ndarray iota
                if isinstance(value, np.ndarray):
                    arr_info = dict(to_add)
                    to_add["Value"] = np.array_str(value.flatten(),
                                                   precision=24)
                    arr_info["Key"] = str(key) + "(Reshape)"
                    arr_info["Value"] = str(value.shape)
                    arr_info["ValueType"] = str(type(value.shape))

                    iota_info = self.insert_to_table("Iota", arr_info)
                    iota[iota_info["IotaId"]] = arr_info

                iota_info = self.insert_to_table("Iota", to_add)
                iota[iota_info["IotaId"]] = to_add

            # update progress
            current_i += 1
            tools.print_progress(count=current_i,
                                 start_time=start_time,
                                 total=total_upload)

        # iota created, create junction items
        print("\nCreating Junction Items...")

        # create dataset
        dataset_info = self.insert_to_table("Dataset",
            {"Name": name,
             "Description": description,
             "SourceId": source_id,
             "Created": datetime.now()})
        output_dataset_id = dataset_info["DatasetId"]

        # start new progress bar
        current_i = 0
        start_time = time.time()
        total_upload = len(iota)
        for iota_id in iota.keys():
            # update progress
            tools.print_progress(count=current_i,
                                 start_time=start_time,
                                 total=total_upload)

            # insert junction item
            self.database.table("IotaDatasetJunction").insert(
                {"IotaId": iota_id, "DatasetId": output_dataset_id})

            # update progress
            current_i += 1
            tools.print_progress(count=current_i,
                                 start_time=start_time,
                                 total=total_upload)

        # end run
        end = datetime.now()

        # process run
        print("\n\n")

        print("RUN INFO:")
        print("-" * 80)
        print("In:\t", input_dataset_id)
        print("Out:\t", output_dataset_id)
        print("Alg:\t", alg_id)
        print("User:\t", user_id)
        print("Name:\t", run_name)
        print("Desc:\t", desc)
        print("Begin:\t", begin)
        print("End:\t", end)

        return dataset_info

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
