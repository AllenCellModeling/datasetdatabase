#!/usr/bin/env python

# installed
from datetime import datetime
from typing import Union
import pandas as pd
import numpy as np
import subprocess
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
                 cache_size: int = 5):
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
        self.schema_version = version
        self.driver = config[list(config.keys())[0]]["driver"]
        self.tables = version.get_tables(self)
        self.fms = fms

        # create tables
        if build:
            self.construct_tables(self.schema_version)

        # recent since connection made
        self.recent = {table: [] for table in self.tables}


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
        """
        Get all items from a table where conditions are met.

        Example
        ==========
        ```
        >>> get_items_from_table("User", ["UserId", "=", 1])
        {...}

        >>> get_items_from_table("User")
        [
         {...},
         {...},
         ...
         {...}
        ]
        ```

        Parameters
        ==========
        table: str
            Which table in the database to retrieve items from.
        conditions: list, None
            Which conditions to match items for. None for all items return.

        Returns
        ==========
        items: list
            Which items were found matching conditions given.

        Errors
        ==========

        """

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
        """
        Insert items into a table. If the item already exists given the unique
        contraints, it will return the found object.

        Example
        ==========
        ```
        >>> me = {"Name": "jacksonb", "Created", datetime.now()}
        >>> insert_to_table("User", me)
        {"UserId": 1, "Name": "jacksonb", "Description": None, "Created": ...}

        >>> insert_to_table("User", me)
        {"UserId": 1, "Name": "jacksonb", "Description": None, "Created": ...}

        ```

        Parameters
        ==========
        table: str
            Which table to insert into.
        items: dict
            Which items to insert.

        Returns
        ==========
        The row of data that was created or found.

        Errors
        ==========
        ValueError:
            Missing non-nullable values for the insert.

        """

        # enforce types
        checks.check_types(table, str)
        checks.check_types(items, dict)

        # create conditions
        conditions = []
        for k, v in items.items():
            if not isinstance(v, datetime):
                if v is not None:
                    conditions.append([k, "=", v])

        # try catch ingest
        try:
            id = self.database.table(table).insert_get_id(items,
                sequence=(table + "Id"))
            info = self.get_items_from_table(table, [table + "Id", "=", id])[0]
        except Exception as e:
            checks.check_ingest_error(e)
            info = self.get_items_from_table(table, conditions)

        # get row


        # format row
        if self.driver == "postgresql":
            info = dict(info)

        # update recent
        self.recent[table].append(info)
        if len(self.recent[table]) > self.cache_size:
            self.recent[table] = self.recent[table][1:]

        return info


    def create_dataset(self, iotas: list) -> dict:
        return {}


    def _create_dataset(self, iotas: list) -> dict:
        return {}


    def get_or_create_user(self,
                           name: Union[str, None] = None,
                           description: Union[str, None] = None) -> dict:

        # enforce types
        checks.check_types(name, [str, type(None)])
        checks.check_types(description, [str, type(None)])

        # get name
        if name is None:
            name = self.user

        # create or get user
        user_info = self.get_items_from_table("User", ["Name", "=", name])
        if len(user_info) == 0:
            user_info = self.insert_to_table("User",
                            {"Name": name,
                             "Description": description,
                             "Created": datetime.now()})
        else:
            user_info = user_info[0]

        return user_info


    def get_or_create_algorithm(self,
                                algorithm: types.MethodType,
                                name: Union[str, None],
                                description: Union[str, None],
                                version: Union[str, None]) -> dict:

        # enforce types
        checks.check_types(algorithm, types.MethodType)
        checks.check_types(name, [str, type(None)])
        checks.check_types(description, [str, type(None)])
        checks.check_types(version, [str, type(None)])

        # get name
        if name is None:
            standard_begin = "<module '"
            m_name = str(inspect.getmodule(algorithm))
            m_start = m_name.index(standard_begin) + len(standard_begin)
            m_name = m_name[m_start:]
            standard_split = "' from '"
            m_end = m_name.index(standard_split)
            m_name = m_name[:m_end]
            f_name = algorithm.__name__

            name = m_name + "." + f_name

        if version is None:
            git_dir = os.path.dirname(os.path.abspath(__file__))
            hash = subprocess.check_output(
                ["git", "rev-list", "-1", "HEAD", "./"], cwd=git_dir).strip()
            hash = hash.decode("utf-8")

            assert len(hash) > 0, "Algorithm version could not be determined."
            version = hash

        # get or create alg
        alg_info = self.get_items_from_table("Algorithm",
            [["Name", "=", name], ["Version", "=", version]])
        if len(alg_info) == 0:
            alg_info = self.insert_to_table("Algorithm",
            {"Name": name,
             "Description": description,
             "Version": version,
             "Created": datetime.now()})
        else:
            alg_info = alg_info[0]

        return alg_info


    def process_run(self,
                    algorithm: types.MethodType,
                    input_dataset_id: Union[int, None] = None,
                    input_dataset_name: Union[str, None] = None,
                    set_algorithm_name: Union[str, None] = None,
                    set_algorithm_description: Union[str, None] = None,
                    set_algorithm_version: Union[str, None] = None,
                    name: Union[str, None] = None,
                    description: Union[str, None] = None,
                    alg_parameters: dict = {}) -> dict:

        # enforce types
        checks.check_types(input_dataset_id, [int, type(None)])
        checks.check_types(input_dataset_name, [str, type(None)])
        checks.check_types(algorithm, types.MethodType)
        checks.check_types(set_algorithm_name, [str, type(None)])
        checks.check_types(set_algorithm_description, [str, type(None)])
        checks.check_types(set_algorithm_version, [str, type(None)])
        checks.check_types(name, [str, type(None)])
        checks.check_types(description, [str, type(None)])
        checks.check_types(alg_parameters, dict)

        # enforce that alg_parameters doesn't contain key input_dataset
        assert "input_dataset" not in alg_parameters, \
            "input_dataset is automatically added to alg parameters."

        # get dataset if provided
        input = None
        if input_dataset_id is not None:
            input = self.get_dataset(input_dataset_id)
        elif input_dataset_name is not None:
            input_dataset_id = self.get_items_from_table["Dataset",
                ["Name", "=", input_dataset_name]][0]["DatasetId"]
            input = self.get_dataset(name=input_dataset_name)

        # create alg info before run
        alg_info = self.get_or_create_algorithm(algorithm,
                                                set_algorithm_name,
                                                set_algorithm_description,
                                                set_algorithm_version)

        # update description
        if description is None:
            description = "{a} run for {u}".format(a=alg_info["Name"],
                                                   u=self.user)

        # create user info before run
        user_info = self.get_or_create_user(self.user)

        # begin time
        begin = datetime.now()

        # use the dataset passed
        alg_parameters["input_dataset"] = input
        output_dataset_info = algorithm(alg_parameters)

        # end time
        end = datetime.now()

        # insert run info
        run_info = self.insert_to_table("Run",
                                        {"AlgorithmId": alg_info["AlgorithmId"],
                                         "UserId": user_info["UserId"],
                                         "Name": name,
                                         "Description": description,
                                         "Begin": begin,
                                         "End": end})
        run_id = run_info["RunId"]

        # insert run input
        if input_dataset_id is not None:
            self.insert_to_table("RunInput",
                                 {"RunId": run_id,
                                  "DatasetId": input_dataset_id,
                                  "Created": datetime.now()})

        # insert run output
        self.insert_to_table("RunOutput",
                             {"RunId": run_id,
                              "DatasetId": output_dataset_info["DatasetId"],
                              "Created": datetime.now()})



        return output_dataset_info

    def upload_dataset(self,
                       dataset: Union[str, pathlib.Path, pd.DataFrame],
                       name: Union[str, None] = None,
                       description: Union[str, None] = None,
                       type_map: Union[dict, None] = None,
                       value_validation_map: Union[dict, None] = None,
                       import_as_type_map: bool = False,
                       store_files: bool = True,
                       filepath_columns: Union[str, list, None] = None,
                       replace_paths: Union[dict, None] = UNIX_REPLACE) -> dict:

        # enforce types
        checks.check_types(dataset, [str, pathlib.Path, pd.DataFrame])
        checks.check_types(name, [str, type(None)])
        checks.check_types(description, [str, type(None)])
        checks.check_types(type_map, [dict, type(None)])
        checks.check_types(value_validation_map, [dict, type(None)])
        checks.check_types(import_as_type_map, bool)
        checks.check_types(store_files, bool)
        checks.check_types(filepath_columns, [str, list, type(None)])
        checks.check_types(replace_paths, dict)

        # convert types
        if isinstance(dataset, str):
            dataset = pathlib.Path(dataset)
        if isinstance(filepath_columns, str):
            filepath_columns = [filepath_columns]

        # get file_id
        if isinstance(dataset, pd.DataFrame):
            ds_path = tools.create_pickle_file(dataset)
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

        # create the params object to be passed all the way to the private func
        params = {}
        params["dataset"] = dataset
        params["name"] = name
        params["description"] = description
        params["type_map"] = type_map
        params["value_validation_map"] = value_validation_map
        params["import_as_type_map"] = import_as_type_map
        params["store_files"] = store_files
        params["filepath_columns"] = filepath_columns
        params["replace_paths"] = replace_paths
        params["file_id"] = file_id

        # run upload
        ds_info = self.process_run(algorithm=self._upload_dataset,
                                   alg_parameters=params)

        return ds_info


    def _upload_dataset(self, params) -> dict:
        # unpack values
        dataset = params["dataset"]
        name = params["name"]
        description = params["description"]
        type_map = params["type_map"]
        value_validation_map = params["value_validation_map"]
        import_as_type_map = params["import_as_type_map"]
        store_files = params["store_files"]
        filepath_columns = params["filepath_columns"]
        replace_paths = params["replace_paths"]
        file_id = params["file_id"]

        # read dataset
        if isinstance(dataset, pathlib.Path):
            dataset = pd.read_csv(dataset)

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
        source_id = source_info["SourceId"]

        # create or get file source
        file_source_info = self.get_items_from_table("FileSource",
                            ["FileId", "=", file_id])
        if len(file_source_info) == 0:
            file_source_info = self.insert_to_table("FileSource",
                            {"SourceId": source_info["SourceId"],
                             "FileId": file_id})
        else:
            file_source_info = file_source_info[0]

        # begin checks
        print("Validating Dataset...")
        current_i = 0
        start_time = time.time()
        total_upload = (len(dataset) * len(dataset.columns) * 4)

        # update progress
        tools.print_progress(count=current_i,
                             start_time=start_time,
                             total=total_upload)

        # format types
        if import_as_type_map:
            dataset = format.format_dataset(dataset, type_map)

        current_i += (len(dataset) * len(dataset.columns))
        tools.print_progress(count=current_i,
                             start_time=start_time,
                             total=total_upload)

        # format paths
        if filepath_columns is not None or \
           replace_paths is not None:
            dataset = format.format_paths(dataset,
                                          filepath_columns,
                                          replace_paths)

        current_i += (len(dataset) * len(dataset.columns))
        tools.print_progress(count=current_i,
                             start_time=start_time,
                             total=total_upload)

        # store files
        if store_files:
            # TODO:
            # get duplication size
            # verify duplication size w/ user
            # store objects
            pass

        # validate types
        checks.validate_dataset_types(dataset, type_map, filepath_columns)
        current_i += (len(dataset) * len(dataset.columns))
        tools.print_progress(count=current_i,
                             start_time=start_time,
                             total=total_upload)

        # validate values
        checks.validate_dataset_values(dataset, value_validation_map)
        current_i += (len(dataset) * len(dataset.columns))
        tools.print_progress(count=current_i,
                             start_time=start_time,
                             total=total_upload)

        # begin iota creation
        print("\nCreating Iota...")

        iota = {}
        current_i = 0
        start_time = time.time()
        total_upload = (len(dataset) * len(dataset.columns))
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

        # create dataset
        dataset_info = self.insert_to_table("Dataset",
            {"Name": name,
             "Description": description,
             "SourceId": source_id,
             "Created": datetime.now()})
        output_dataset_id = dataset_info["DatasetId"]

        # iota created, create junction items
        print("\nCreating Junction Items...")

        # start new progress bar
        current_i = 0
        start_time = time.time()
        total_upload = len(iota)
        for iota_id in iota.keys():
            # update progress
            tools.print_progress(count=current_i,
                                 start_time=start_time,
                                 total=total_upload)

            junction_item = {
                "IotaId": iota_id,
                "DatasetId": output_dataset_id,
                "Created": datetime.now()}

            # insert junction item
            self.insert_to_table("IotaDatasetJunction", junction_item)

            # update progress
            current_i += 1
            tools.print_progress(count=current_i,
                                 start_time=start_time,
                                 total=total_upload)

        print("\nDataset upload complete!")

        return dataset_info

    def __getitem__(self, key):
        return


    def _deep_print(self):
        print(("-" * 31) + " DATASET DATABASE " + ("-" * 31))

        for table, items in self.recent.items():
            print("-" * 80)

            print(table + ":")
            print("rows: {r}".format(r=self.tables[table].count()))
            print("recent:")
            for r in self.recent[table]:
                print(r)


    def __str__(self):
        disp = "Recent Datasets:"
        disp += ("\n" + ("-" * 80))
        for row in self.recent["Dataset"]:
            disp += "\n{r}".format(r=row)

        return disp


    def __repr__(self):
        return str(self)
