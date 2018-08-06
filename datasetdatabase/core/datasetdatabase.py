#!/usr/bin/env python

# installed
from datetime import datetime
import Levenshtein as leven
from typing import Union
import pandas as pd
import numpy as np
import subprocess
import importlib
import itertools
import pathlib
import inspect
import getpass
import orator
import quilt
import types
import time
import yaml
import ast
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
DUPLICATE_DATA_CONFIRM = "Duplicate a maximum of {s} of data? (y / n) "
STANDARD_UPLOAD_PARAMS = {"name": None,
                          "description": None,
                          "type_map": None,
                          "value_validation_map": None,
                          "import_as_type_map": False,
                          "store_files": True,
                          "force_storage": False,
                          "filepath_columns": None,
                          "replace_paths": UNIX_REPLACE}
SEARCH_WEIGHTS = {"Name": 0.33,
                  "Description": 0.33,
                  "FilepathColumns": 0.33,
                  "Keys": 0.5,
                  "Values": 0.5}

# bad, but until someone tells me how to fix [671]:
#   dataset[col][i] = self.fms.get_readpath_from_fileid(f_id)
# i dont know what else to do
pd.options.mode.chained_assignment = None  # default='warn'

# TODO:
# documentation for:
#   export_to_quilt
#   import_from_quilt
#   _create_dataset
#
#   quilt fms
#
# form_dataset from iota ids


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
                 fms_connection_options: Union[dict, None] = None,
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
            get_or_create_file, and get_readpath_from_fileid.

            Default: datasetdatabase.schema.filemanagers.quiltfms

        fms_connection_options: dict, None
            Parameters to be passed to the fms connection module's setup
            function specified by the above fms parameter.

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

        # create tables
        if build:
            self.construct_tables(self.schema_version)

        # run fms module setup
        self.fms = fms.FMS(self, fms_connection_options, build)


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
                             conditions: list = []) -> dict:
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
        conditions: list
            Which conditions to match items for. Leave blank for all items.

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
        if self.driver == "postgres" or self.driver == "postgresql":
            items = [dict(row) for row in items]
        else:
            items = items.all()

        return items


    def _insert_to_table(self, table: str, items: dict) -> dict:
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
            info = self.get_items_from_table(table, conditions)[0]

        # format row
        if self.driver == "postgres" or self.driver == "postgresql":
            info = dict(info)

        return info


    def upload_files(self, filepaths: Union[str, list]) -> list:
        """
        Upload a single file or list of files to this dataset database's fms.

        Example
        ==========
        ```
        >>> t = "/path/to/real/file.png"
        >>> upload_files(t)
        {"FileId": 1, "OriginalFilepath": "/path/to/real/file.png", ...}

        >>> t = ["/path/to/real/file.png", "/path/to/nonexistant/file.png"]
        upload_files(t)
        FileNotFoundError: "/path/to/nonexistant/file.png" was not found.

        ```

        Parameters
        ==========
        filepaths: str, list
            Which filepath or list of filepaths, to upload to the fms.

        Returns
        ==========
        infos: list
            A list of the rows (as dictionaries) created from the upload.

        Errors
        ==========

        """

        # enforce types
        checks.check_types(filepaths, [str, list])

        # convert types
        if isinstance(filepaths, str):
            filepaths = [filepaths]

        # create file info list
        infos = []
        for fp in filepaths:
            infos.append(self.fms.get_or_create_file(fp))

        return infos


    def get_or_create_user(self,
                           name: Union[str, None] = None,
                           description: Union[str, None] = None) -> dict:

        """
        Short function that extends the insert_to_table function due to
        specific needs of the idea of a user in a database.

        Example
        ==========
        ```
        >>> me = "jacksonb"
        >>> get_or_create_user(me)
        {"UserId": 1, "Name": "jacksonb", "Description": None, "Created": ...}

        ```

        Parameters
        ==========
        name: str, None
            The name of the user to be added to the database, if None is
            provided, the dataset database user is added.
        description: str, None
            The description of the user to be added to the database.

        Returns
        ==========
        user_info: dict
            The row that was added or found for that user.

        Errors
        ==========

        """

        # enforce types
        checks.check_types(name, [str, type(None)])
        checks.check_types(description, [str, type(None)])

        # get name
        if name is None:
            name = self.user

        # enforce name
        checks.check_user(name)

        # create or get user
        user_info = self.get_items_from_table("User", ["Name", "=", name])
        if len(user_info) == 0:
            user_info = self._insert_to_table("User",
                            {"Name": name,
                             "Description": description,
                             "Created": datetime.now()})
        else:
            user_info = user_info[0]

        return user_info


    def get_or_create_algorithm(self,
                        algorithm: Union[types.MethodType, types.FunctionType],
                        name: Union[str, None],
                        description: Union[str, None],
                        version: Union[int, float, str, None]) -> dict:

        """
        Short function that extends the insert_to_table function due to
        specific needs of the idea of a algorithm in a database.

        Example
        ==========
        ```
        >>> def hello():
                print("hello")
        >>> get_or_create_algorithm(hello, version=0.1)
        {"AlgorithmId": 1, "Name": "hello", "Description": None, ...}

        ```

        Parameters
        ==========
        algorithm: method, function
            The method to be added to the database.
        name: str, None
            The forced name to store in the database.
            If none provided, it will attempt to retrieve the name by inspect.
        description: str, None
            The forced description to store in the database.
        version: int, float, str, None
            The forced version string, int, or float to store in the database.
            If none provided, it will attempt to retrieve the git commit hash
            of the methods directory.

        Returns
        ==========
        alg_info: dict
            The row that was added or found for that algorithm.

        Errors
        ==========
        AssertionError:
            No version passed and could not detect a git commit.

        """

        # enforce types
        checks.check_types(algorithm, [types.MethodType, types.FunctionType])
        checks.check_types(name, [str, type(None)])
        checks.check_types(description, [str, type(None)])
        checks.check_types(version, [int, float, str, type(None)])

        # get name
        if name is None:
            str_alg = str(algorithm)
            if "<bound method " in str_alg:
                standard_begin = "<module '"
                m_name = str(inspect.getmodule(algorithm))
                m_start = m_name.index(standard_begin) + len(standard_begin)
                m_name = m_name[m_start:]
                standard_split = "' from '"
                m_end = m_name.index(standard_split)
                m_name = m_name[:m_end]
                f_name = algorithm.__name__

                name = m_name + "." + f_name
            else:
                name = str_alg[len("<function ") : str_alg.index(" at ")]

        # handle dsdb algorithm by using the package version
        if "datasetdatabase.core.datasetdatabase" in name:
            version = __version__

        # attempt git hash check of repo
        if version is None:
            version = ""
            try:
                git_dir = os.path.dirname(os.path.abspath(__file__))
                version = subprocess.check_output(
                    ["git", "rev-list", "-1", "HEAD", "./"], cwd=git_dir)
                version = version.strip()
                version = version.decode("utf-8")
            except subprocess.CalledProcessError:
                pass

            assert len(version) > 0, \
            "Algorithm version could not be determined."

        # convert version to string
        version = str(version)

        # get or create alg
        alg_info = self.get_items_from_table("Algorithm",
            [["Name", "=", name], ["Version", "=", version]])
        if len(alg_info) == 0:
            alg_info = self._insert_to_table("Algorithm",
            {"Name": name,
             "Description": description,
             "Version": version,
             "Created": datetime.now()})
        else:
            alg_info = alg_info[0]

        return alg_info


    def get_dataset(self,
                    id: Union[int, None] = None,
                    name: Union[str, None] = None,
                    sourceid: Union[int, None] = None,
                    columns: Union[str, list, None] = None,
                    get_info_items: bool = False) \
                    -> Union[pd.DataFrame, KeyError]:
        """
        Using either a dataset id, name, or source id, return the formed and
        recasted dataset.

        Example
        ==========
        ```
        >>> get_dataset(1)
            |   files       |   structs     |   ...     |
        0   |   foo/1.png   |   myosin      |   ...     |
        1   |   foo/2.png   |   actin       |   ...     |
        ...

        >>> get_dataset(1, columns="files")
            |   files
        0   |   foo/1.png
        1   |   foo/2.png
        ...

        ```

        Parameters
        ==========
        id: int, None
            The dataset id as it is stored in the database.
        name: str, None
            The dataset name as it is stored in the database.
        sourceid: int, None
            The source id of the dataset as it is stored in the database.
        columns: str, list, None
            Which columns to return. None for all.
        get_info_items: bool
            Should the Iota items be returned and attached to the dataframe.

        Returns
        ==========
        df: pandas.DataFrame
            The desired dataset after reformation and casting.

        Errors
        ==========
        AssertionError:
            Must provide a dataset id, name, or source id for the query.

        """

        # enforce types
        checks.check_types(id, [int, type(None)])
        checks.check_types(name, [str, type(None)])
        checks.check_types(sourceid, [int, type(None)])
        checks.check_types(columns, [str, list, type(None)])
        checks.check_types(get_info_items, bool)

        # must provide id or name
        assert id is not None or name is not None or sourceid is not None, \
            "Must provide dataset id, name, or source id."

        # convert types
        if isinstance(columns, str):
            columns = [columns]

        # get merge contents
        ds = self.database.table("Dataset")

        if id is not None:
            ds = ds.where("Dataset.DatasetId", "=", id)
        elif name is not None:
            ds = ds.where("Dataset.Name", "=", name)
        else:
            ds = ds.where("Dataset.SourceId", "=", sourceid)

        # join
        ds = ds.join("IotaDatasetJunction",
                     "Dataset.DatasetId", "=", "IotaDatasetJunction.DatasetId")
        ds = ds.join("Iota",
                     "Iota.IotaId", "=", "IotaDatasetJunction.IotaId")

        # filter by key
        if isinstance(columns, list):
            or_where = database.query()
            for c in columns:
                or_where = or_where.where("Key", "=", c)

            ds = ds.or_where(or_where)

        # get
        ds = ds.get()

        # format
        if self.driver == "sqlite":
            ds = pd.DataFrame(ds.all())
        else:
            ds = pd.DataFrame([dict(r) for r in ds.all()])

        ds = format.convert_dataset_to_dataframe(ds,
                                                 get_info_items)

        return ds


    def search(self,
               seq: Union[str, list],
               sep: Union[str, None] = None,
               search_keys: bool = False,
               search_values: bool = False,
               all_combinations_search: bool = False) -> dict:

        # enforce types
        checks.check_types(seq, [str, list])
        checks.check_types(sep, [str, type(None)])
        checks.check_types(search_keys, bool)
        checks.check_types(search_values, bool)
        checks.check_types(all_combinations_search, bool)

        # convert
        if isinstance(seq, str):
            seq = seq.split(sep)

        # scores object
        scores = []

        # datasets
        datasets = self.get_items_from_table("Dataset")

        # get combinations
        if all_combinations_search:
            combs = []
            for i in range(len(seq)):
                combs += itertools.combinations(seq, i+1)

            seq = [list(c) for c in combs]

        # force structure on seq
        else:
            seq = [[w] for w in seq]

        # compute scores
        for ds in datasets:
            score = 0.0

            weight_name = SEARCH_WEIGHTS["Name"]
            weight_desc = SEARCH_WEIGHTS["Description"]
            weight_fpcols = SEARCH_WEIGHTS["FilepathColumns"]

            if ds["FilepathColumns"] is None:
                weight_desc *= 2

            if search_keys or search_values:
                match_all = self.database.table("Dataset")
                match_all = match_all.where("Dataset.DatasetId", "=",
                                            ds["DatasetId"])
                match_all = match_all.join("IotaDatasetJunction",
                    "Dataset.DatasetId", "=", "IotaDatasetJunction.DatasetId")
                match_all = match_all.join("Iota",
                    "Iota.IotaId", "=", "IotaDatasetJunction.IotaId")

                if search_keys:
                    keys = match_all.select("Iota.Key").distinct().get()

                    if self.driver == "postgres" or self.driver == "postgresql":
                        keys = [dict(row)["Key"] for row in keys]
                    else:
                        keys = [i["Key"] for i in keys]

                if search_values:
                    vals = match_all.select("Iota.Value").distinct().get()

                    if self.driver == "postgres" or self.driver == "postgresql":
                        vals = [dict(row)["Value"] for row in vals]
                    else:
                        vals = [i["Value"] for i in vals]

            for sub in seq:
                joined = " ".join(sub)

                if joined in ds["Name"]:
                    name_dist = leven.distance(ds["Name"], joined)
                    name_score = float(name_dist) / len(ds["Name"])
                    name_score = (1 - name_score)\
                                    * weight_name\
                                    * len(sub)
                else:
                    added_from_set = 1
                    for i, word in enumerate(sub):
                        if word in ds["Name"]:
                            added_from_set += 1
                        name_dist = leven.distance(ds["Name"], word)
                        name_score = float(name_dist) / len(ds["Name"])
                        name_score = (1 - name_score)\
                                        * weight_name\
                                        * (1 / len(sub))\
                                        * (added_from_set / len(sub))

                score += name_score

                desc_score = 0.0
                if ds["Description"] is not None:
                    desc = ds["Description"]

                    if joined in desc:
                        desc_dist = leven.distance(desc, joined)
                        desc_score = float(desc_dist) / len(desc)
                        desc_score = (1 - desc_score)\
                                        * weight_desc\
                                        * len(sub)
                    else:
                        added_from_set = 1
                        for i, word in enumerate(sub):
                            if word in desc:
                                added_from_set += 1
                            desc_dist = leven.distance(desc, word)
                            desc_score = float(desc_dist) / len(desc)
                            desc_score = (1 - desc_score)\
                                            * weight_desc\
                                            * (1 / len(sub))\
                                            * (added_from_set / len(sub))

                score += desc_score

                fpcol_score = 0.0
                if ds["FilepathColumns"] is not None:
                    fpcols = " ".join(ds["FilepathColumns"])
                    if joined in fpcols:
                        fpcol_dist = leven.distance(fpcols, joined)
                        fpcol_score = float(fpcol_dist) / len(fpcols)
                        fpcol_score = (1 - fpcol_score)\
                                        * weight_fpcols\
                                        * len(sub)
                    else:
                        added_from_set = 1
                        for i, word in enumerate(sub):
                            if word in ds["FilepathColumns"]:
                                added_from_set += 1
                            fpcol_dist = leven.distance(fpcols, word)
                            fpcol_score = float(fpcol_dist) / len(fpcols)
                            fpcol_score = (1 - fpcol_score)\
                                            * weight_fpcols\
                                            * (1 / len(sub))\
                                            * (added_from_set / len(sub))

                score += fpcol_score

                # key_score = 0.0
                # if search_keys:
                #     added_from_set = 1
                #     for i, word in enumerate(sub):
                #         if word in keys:
                #             added_from_set += 1
                #         key_dist = leven.distance(key, word)
                #         key_score = float(key_dist) / len(fpcols)
                #         key_score = (1 - key_score)\
                #                         * weight_fpcols\
                #                         * (1 / len(sub))\
                #                         * (added_from_set / len(sub))
                #
                # val_score = 0.0
                # if search_values:
                #     added_from_set = 1
                #     for i, word in enumerate(sub):
                #         if word in desc:
                #             added_from_set += 1
                #         val_dist = leven.distance(fpcols, word)
                #         val_score = float(val_dist) / len(fpcols)
                #         val_score = (1 - val_score)\
                #                         * weight_fpcols\
                #                         * (1 / len(sub))\
                #                         * (added_from_set / len(sub))

            scores.append({"score": score, "ds_info": ds})
            scores = sorted(scores, key=lambda x: x["score"], reverse=True)

        return scores


    def process_run(self,
                    algorithm: Union[types.MethodType, types.FunctionType],
                    input_dataset_id: Union[int, None] = None,
                    input_dataset_name: Union[str, None] = None,
                    set_algorithm_name: Union[str, None] = None,
                    set_algorithm_description: Union[str, None] = None,
                    set_algorithm_version: Union[str, None] = None,
                    name: Union[str, None] = None,
                    description: Union[str, None] = None,
                    alg_parameters: dict = {},
                    dataset_parameters: dict = STANDARD_UPLOAD_PARAMS) -> dict:

        """
        Process an dataset using an algorithm and provided parameters.

        Example
        ==========
        ```
        >>> def add_one(df, params):
                return df["column_i_care_about"] += 1
        >>> process_run(add_one, 1)
        {"DatasetId": 2, ...}

        >>> def conditional_add(df, params):
                return df[params["col"]] += params["add_by"]
        >>> run_params = {"col": "column_i_care_about", "add_by": 3}
        >>> process_run(conditional_add, 2, alg_parameters=run_params)
        {"DatasetId": 3, ...}

        ```

        Parameters
        ==========
        algorithm: method, function
            The method or function to run against a dataset. Your algorithm must
            return a pandas.DataFrame.
        input_dataset_id: int, None
            The dataset id as stored in the database.
        input_dataset_name: str, None
            The dataset name as stored in the database.
        set_algorithm_name: str, None
            What the method or function passed as algorithm should be given as a
            name.
        set_algorithm_description: str, None
            What the method or function passed as algorithm should be given as a
            description.
        set_algorithm_version: str, None
            What the method or function passed as algorithm should be given as a
            version.
        name: str, None
            The name of this run.
        description: str, None
            The description of this run.
        alg_parameters: dict
            Parameters to be passed to the algorithm provided.
        dataset_parameters: dict
            Parameters to be used during the upload of the returned
            pandas.DataFrame from your algorithm.

            Default: STANDARD_UPLOAD_PARAMS, if you only pass a single upload
            parameter the rest will be filled in from the default.

        """

        # enforce types
        checks.check_types(input_dataset_id, [int, type(None)])
        checks.check_types(input_dataset_name, [str, type(None)])
        checks.check_types(algorithm, [types.MethodType, types.FunctionType])
        checks.check_types(set_algorithm_name, [str, type(None)])
        checks.check_types(set_algorithm_description, [str, type(None)])
        checks.check_types(set_algorithm_version, [str, type(None)])
        checks.check_types(name, [str, type(None)])
        checks.check_types(description, [str, type(None)])
        checks.check_types(alg_parameters, dict)
        checks.check_types(dataset_parameters, dict)

        # get dataset if provided
        input = None
        if input_dataset_id is not None:
            input = self.get_dataset(input_dataset_id)
        elif input_dataset_name is not None:
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
        output_dataset = algorithm(input, alg_parameters)

        # check output
        checks.check_types(output_dataset, pd.DataFrame)

        # create iota from new dataset
        if dataset_parameters != STANDARD_UPLOAD_PARAMS:
            dataset_parameters = {**STANDARD_UPLOAD_PARAMS,
                                  **dataset_parameters}

        dataset_parameters["dataset"] = output_dataset

        # create source
        source_info = self._insert_to_table("Source",
                        {"Created": datetime.now()})
        dataset_parameters["source_info"] = source_info
        if "source_type" not in dataset_parameters:
            dataset_parameters["source_type"] = "RunSource"
        if "source_type_id" not in dataset_parameters:
            dataset_parameters["source_type_id"] = None

        output_dataset_info = self._create_dataset(dataset_parameters)

        # end time
        end = datetime.now()

        # insert run info
        run_info = self._insert_to_table("Run",
                                        {"AlgorithmId": alg_info["AlgorithmId"],
                                         "UserId": user_info["UserId"],
                                         "Name": name,
                                         "Description": description,
                                         "AlgorithmParameters":
                                            str(alg_parameters),
                                         "Begin": begin,
                                         "End": end})

        # insert run input
        if input_dataset_id is not None:
            self._insert_to_table("RunInput",
                                 {"RunId": run_info["RunId"],
                                  "DatasetId": input_dataset_id,
                                  "Created": datetime.now()})

        # insert run output
        self._insert_to_table("RunOutput",
                             {"RunId": run_info["RunId"],
                              "DatasetId": output_dataset_info["DatasetId"],
                              "Created": datetime.now()})

        # insert run source
        if dataset_parameters["source_type"] in ["RunSource"]:
            self._insert_to_table("RunSource",
                                 {"SourceId": source_info["SourceId"],
                                  "RunId": run_info["RunId"],
                                  "Created": datetime.now()})

        return output_dataset_info


    def form_dataset(self, iotas: list) -> dict:
        return {}


    def upload_dataset(self,
        dataset: Union[str, pathlib.Path, pd.DataFrame],
        name: Union[str, None] = STANDARD_UPLOAD_PARAMS["name"],
        description: Union[str, None] = STANDARD_UPLOAD_PARAMS["description"],
        type_map: Union[dict, None] = STANDARD_UPLOAD_PARAMS["type_map"],
        value_validation_map: Union[dict, None] = \
            STANDARD_UPLOAD_PARAMS["value_validation_map"],
        import_as_type_map: bool = STANDARD_UPLOAD_PARAMS["import_as_type_map"],
        store_files: bool = STANDARD_UPLOAD_PARAMS["store_files"],
        force_storage: bool = STANDARD_UPLOAD_PARAMS["force_storage"],
        filepath_columns: Union[str, list, None] =\
            STANDARD_UPLOAD_PARAMS["filepath_columns"],
        replace_paths: Union[dict, None] =\
            STANDARD_UPLOAD_PARAMS["replace_paths"]) -> dict:

        """
        Upload a dataframe like dataset to the database. If the dataset already
        exists in the database it is not doubly uploaded.

        Example
        ==========
        ```
        >>> rows = [{"ints": 0, "strs": "foo1"},
                    {"ints": 1, "strs": "foo2"}]
        >>> df = pd.DataFrame(rows)
        >>> upload_dataset(df)
        {"DatasetId": 1, ...}

        >>> upload_dataset(df)
        {"DatasetId": 1, ...}

        ```

        Parameters
        ==========
        dataset: str, pathlib.Path, pandas.DataFrame
            The dataset to be uploaded. Strings and pathlib.Paths will be opened
            using pandas.
        name: str, None
            The name the dataset should be given.
        description: str, None
            The description the dataset should be given.
        type_map: dict, None
            A dictionary of columns and what type is allowed in that column.
            Ex: {"ints": int}
        value_validation_map: dict, None
            A dictionary of columns and a checking function that validates the
            values of that column.
            Ex: {"ints": lambda x: return x >= 4}
        import_as_type_map: bool
            Before validation occurs, attempt to cast the values contained in
            the type_map columns to their mapped types.
        store_files: bool
            Should files be stored/ uploaded to a file management system.
        force_storage: bool
            Should storage of files be forced.

            Default: False, before storing files, size of upload will be
            calculated and user will be asked to approve the upload.
        filepath_columns: str, list, None
            Which columns contain filepaths.
        replace_paths: dict, None
            A dictionary with key and value mapping how paths should be
            manipulated.

            Ex: {"\\": "/"}, None for no path manipulation.

        Returns
        ==========
        ds_info: dict
            The database row that stores the information regarding the ingested
            dataset.

        Errors
        ==========

        """

        # enforce types
        checks.check_types(dataset, [str, pathlib.Path, pd.DataFrame])
        checks.check_types(name, [str, type(None)])
        checks.check_types(description, [str, type(None)])
        checks.check_types(type_map, [dict, type(None)])
        checks.check_types(value_validation_map, [dict, type(None)])
        checks.check_types(import_as_type_map, bool)
        checks.check_types(store_files, bool)
        checks.check_types(force_storage, bool)
        checks.check_types(filepath_columns, [str, list, type(None)])
        checks.check_types(replace_paths, dict)

        # convert types
        if isinstance(dataset, str):
            dataset = pathlib.Path(dataset)
        if isinstance(filepath_columns, str):
            filepath_columns = [filepath_columns]

        # get file_info
        if isinstance(dataset, pd.DataFrame):
            dataset = dataset.copy()
            ds_path = tools.create_pickle_file(dataset)
            hash = self.fms.get_hash(ds_path)
            file_info = self.fms.get_file(filepath=ds_path, hash=hash)
        else:
            hash = self.fms.get_hash(dataset)
            file_info = self.fms.get_file(filepath=dataset, hash=hash)

        # return if exists
        if file_info is not None:
            try:
                found_ds = self.database.table("Dataset") \
                 .join("Source", "Dataset.SourceId", "=", "Source.SourceId") \
                 .join("FileSource",
                       "FileSource.SourceId", "=", "Source.SourceId") \
                 .where("FileId", "=", file_info["FileId"]).get()[0]

                found_ds = self.get_items_from_table("Dataset",
                                ["DatasetId", "=", found_ds["DatasetId"]])[0]

                # remove the created pickle
                if isinstance(dataset, pd.DataFrame):
                    os.remove(ds_path)

                return found_ds
            except IndexError:
                pass

        # create if not
        if isinstance(dataset, pd.DataFrame):
            file_info = self.fms.get_or_create_file(ds_path)
            os.remove(ds_path)
        else:
            file_info = self.fms.get_or_create_file(dataset)

        # check dataset name
        if name is None:
            name = "hash_" + file_info["MD5"]

        # create the params object to be passed all the way to the private func
        params = {}
        params["dataset"] = dataset
        params["name"] = name
        params["description"] = description
        params["type_map"] = type_map
        params["value_validation_map"] = value_validation_map
        params["import_as_type_map"] = import_as_type_map
        params["store_files"] = store_files
        params["force_storage"] = force_storage
        params["filepath_columns"] = filepath_columns
        params["replace_paths"] = replace_paths
        params["source_type"] = "FileSource"
        params["source_type_id"] = file_info["FileId"]

        # run upload
        ds_info = self.process_run(algorithm=self._upload_dataset,
                                   alg_parameters=params,
                                   dataset_parameters=params)

        return ds_info


    def export_to_quilt(self,
                        id: Union[int, None] = None,
                        name: Union[str, None] = None,
                        sourceid: Union[int, None] = None,
                        columns: Union[str, list, None] = None,
                        filepath_columns: Union[str, list, None] = None,
                        get_info_items: bool = False,
                        quilt_user: str = "dsdb",
                        quilt_package_name: Union[str, None] = None) -> str:

        # enforce types
        checks.check_types(id, [int, type(None)])
        checks.check_types(name, [str, type(None)])
        checks.check_types(sourceid, [int, type(None)])
        checks.check_types(columns, [str, list, type(None)])
        checks.check_types(filepath_columns, [str, list, type(None)])
        checks.check_types(get_info_items, bool)
        checks.check_types(quilt_user, str)
        checks.check_types(quilt_package_name, [str, type(None)])

        # must provide id or name
        assert id is not None or name is not None or sourceid is not None, \
            "Must provide dataset id, name, or source id."

        # get info
        if id is not None:
            ds_info = self.get_items_from_table("Dataset",
                                                ["DatasetId", "=", id])
        elif name is not None:
            ds_info = self.get_items_from_table("Dataset",
                                                ["Name", "=", name])
        else:
            ds_info = self.get_items_from_table("Dataset",
                                                ["SourceId", "=", sourceid])

        # first item from return
        ds_info = ds_info[0]

        # process or get filepath columns
        if isinstance(filepath_columns, str):
            filepath_columns = [filepath_columns]
        if filepath_columns is None:
            if ds_info["FilepathColumns"] is None:
                filepath_columns = []
            else:
                filepath_columns = ast.literal_eval(ds_info["FilepathColumns"])

        # get dataset
        dataset = self.get_dataset(id, name, sourceid, columns, get_info_items)

        # create file ids if needed
        # generate the map of file packages to be added
        quilt_FMS = quiltfms.FMS(self)
        packages = {}
        for col in filepath_columns:
            for i, item in enumerate(dataset[col]):
                # ensure quilt package creation of each file needed for upload
                file_info = quilt_FMS.get_or_create_file(item)
                file_node_name = (file_info["Filetype"] + "_" +
                                  str(file_info["FileId"]))
                packages[file_node_name] = file_info["QuiltPackage"]
                dataset[col][i] = file_node_name

        # dataset has been parsed
        ds_path = tools.create_pickle_file(dataset)
        file_info = quilt_FMS.get_or_create_file(ds_path)

        # create readme
        readme_path = tools.write_dataset_readme(ds_info)

        # construct manifest
        contents = {}
        contents["README"] = {"file": str(readme_path), "transform": "id"}
        contents["data"] = {"file": file_info["ReadPath"], "transform": "id"}
        send_files = {}
        for f_id, full_pkg_name in packages.items():
            send_files[f_id] = {"package": full_pkg_name}
        contents["files"] = send_files
        node = {"contents": contents}

        # write temporary manifest
        temp_write_loc = pathlib.Path(os.getcwd())
        temp_write_loc /= "single_file.yml"
        with open(temp_write_loc, "w") as write_out:
            yaml.dump(node, write_out, default_flow_style=False)

        # create quilt node
        if quilt_package_name is not None:
            full_pkg_name = quilt_user + "/" + quilt_package_name
        else:
            full_pkg_name = quilt_user + "/" + ds_info["Name"].replace(" ", "_")

        with tools.suppress_prints():
            quilt.build(full_pkg_name, str(temp_write_loc))

        # remove files
        os.remove(temp_write_loc)
        os.remove(readme_path)
        os.remove(ds_path)

        return full_pkg_name


    def import_from_quilt(self, package_name: str) \
        -> Union[dict, ModuleNotFoundError]:
        # enforce types
        checks.check_types(package_name, str)

        # split package_name
        if "/" in package_name:
            package_split = package_name.split("/")
            user = package_split[0]
            package = package_split[1]
        else:
            user = "dsdb"
            package = package_name

        # attempt import
        full_import = "quilt.data." + user + "." + package
        try:
            package = importlib.import_module(full_import)
        except ModuleNotFoundError:
            try:
                with tools.suppress_prints():
                    quilt.install(user + "/" + package)
                package = importlib.import_module(full_import)
            except HTTPResponseException:
                err = "Quilt package, {p}, not found...".format(p=full_import)
                raise ModuleNotFoundError(err)

        # run hash check of dataset
        hash = self.fms.get_hash(package.data())
        file_info = self.fms.get_file(filepath=package.data(), hash=hash)

        # return if exists
        if file_info is not None:
            try:
                found_ds = self.database.table("Dataset") \
                 .join("Source", "Dataset.SourceId", "=", "Source.SourceId") \
                 .join("FileSource",
                       "FileSource.SourceId", "=", "Source.SourceId") \
                 .where("FileId", "=", file_info["FileId"]).get()[0]

                found_ds = self.get_items_from_table("Dataset",
                                ["DatasetId", "=", found_ds["DatasetId"]])[0]
                return found_ds
            except IndexError:
                pass

        # get basic dataset info
        ds_info = tools.parse_readme(package.README())

        # update the filepath columns with real filepaths
        dataset = pd.read_pickle(package.data())
        if ds_info["FilepathColumns"] is not None:
            for col in ds_info["FilepathColumns"]:
                for i, item in enumerate(dataset[col]):
                    fp = getattr(package.files, dataset[col][i]).load()
                    dataset[col][i] = fp

        # create the params object to be passed all they way to the private func
        params = {}
        params["dataset"] = dataset
        params["name"] = package_name
        params["description"] = ds_info["Description"]
        params["type_map"] = None
        params["value_validation_map"] = None
        params["import_as_type_map"] = False
        params["store_files"] = True
        params["force_storage"] = True
        params["filepath_columns"] = ds_info["FilepathColumns"]
        params["replace_paths"] = UNIX_REPLACE
        params["source_type"] = "QuiltSource"
        params["source_type_id"] = package_name

        # run upload
        ds_info = self.process_run(algorithm=self._upload_dataset,
                                   alg_parameters=params,
                                   dataset_parameters=params)

        return ds_info


    def _find_dataset_connections(self,
                                  current: dict,
                                  connections: list = [],
                                  depth: int = 0,
                                  max_depth: Union[int, None] = None) \
                                  -> list:

        # use dataset id
        if "DatasetId" in current:
            ds_id = current["DatasetId"]
        elif "Dataset.DatasetId" in current:
            ds_id = current["Dataset.DatasetId"]
        else:
            return connections

        if max_depth is None or depth <= max_depth:
            try:
                forward_run_ids = self.database.table("Run")\
                .join("RunInput", "Run.RunId", "=", "RunInput.RunId")\
                .join("Dataset",
                      "RunInput.DatasetId", "=", "Dataset.DatasetId")\
                .where("Dataset.DatasetId", "=", current["DatasetId"])\
                .select("Run.RunId")\
                .get()

                for r in forward_run_ids:
                    forward_ds = self.database.table("Run")\
                    .join("RunOutput", "Run.RunId", "=", "RunOutput.RunId")\
                    .join("Dataset",
                          "RunOutput.DatasetId", "=", "Dataset.DatasetId")\
                    .where("Run.RunId", "=", r["RunId"])\
                    .limit(1)\
                    .get()

                    forward_alg = self.database.table("Run")\
                    .join("Algorithm",
                          "Run.AlgorithmId", "=", "Algorithm.AlgorithmId")\
                    .where("Run.RunId", "=", r["RunId"])\
                    .limit(1)\
                    .get()

                    if self.driver == "postgres" or self.driver == "postgresql":
                        forward_ds = [dict(r) for r in forward_ds]
                        forward_alg = [dict(r) for r in forward_alg]

                    forward_ds = forward_ds[0]
                    forward_alg = forward_alg[0]

                    forward = {"from": current["Name"],
                               "to": forward_ds["Name"],
                               "alg": forward_alg["Name"]}

                    if forward not in connections:
                        connections.append(forward)

                        connection = self._find_dataset_connections(forward_ds,
                                                                    connections,
                                                                    depth + 1,
                                                                    max_depth)
            except orator.exceptions.query.QueryException:
                pass

            try:
                backward_run_ids = self.database.table("Run")\
                .join("RunOutput", "Run.RunId", "=", "RunOuput.RunId")\
                .join("Dataset",
                      "RunOutput.DatasetId", "=", "Dataset.DatasetId")\
                .where("Dataset.DatasetId", "=", current["DatasetId"])\
                .select("Run.RunId")\
                .get()

                for r in backward_run_ids:
                    backward_ds = self.database.table("Run")\
                    .join("RunInput", "Run.RunId", "=", "RunInput.RunId")\
                    .join("Dataset",
                          "RunInput.DatasetId", "=", "Dataset.DatasetId")\
                    .where("Run.RunId", "=", r["RunId"])\
                    .limit(1)\
                    .get()

                    backward_alg = self.database.table("Run")\
                    .join("Algorithm",
                          "Run.AlgorithmId", "=", "Algorithm.AlgorithmId")\
                    .where("Run.RunId", "=", r["RunId"])\
                    .limit(1)\
                    .get()

                    if self.driver == "postgres" or self.driver == "postgresql":
                        backward_ds = [dict(r) for r in backward_ds]
                        backward_alg = [dict(r) for r in backward_alg]

                    backward_ds = backward_ds[0]
                    backward_alg = backward_alg[0]

                    backward = {"from": current["Name"],
                                "to": backward_ds["Name"],
                                "alg": backward_alg["Name"]}

                    if backward not in connections:
                        connections.append(backward)

                        connection = self._find_dataset_connections(backward_ds,
                                                                    connections,
                                                                    depth - 1,
                                                                    max_depth)
            except orator.exceptions.query.QueryException:
                pass

        return connections


    def get_dataset_connections(self,
                                id: Union[int, None] = None,
                                name: Union[str, None] = None,
                                sourceid: Union[int, None] = None,
                                max_distance: Union[int, None] = None) \
                                -> pd.DataFrame:

        # enforce types
        checks.check_types(id, [int, type(None)])
        checks.check_types(name, [str, type(None)])
        checks.check_types(sourceid, [int, type(None)])
        checks.check_types(max_distance, [int, type(None)])

        # must provide id or name
        assert id is not None or name is not None or sourceid is not None, \
            "Must provide dataset id, name, or source id."

        # get ds_info_target
        if id is not None:
            origin = self.get_items_from_table("Dataset",
                                               ["DatasetId", "=", id])
        elif name is not None:
            origin = self.get_items_from_table("Dataset",
                                               ["Name", "=", name])
        else:
            origin = self.get_items_from_table("Dataset",
                                               ["SourceId", "=", sourceid])

        # exists
        assert len(origin) > 0, \
        "Dataset with that name, id, or source does not exist."

        origin = origin[0]
        # recursive find
        connections = self._find_dataset_connections(origin)

        connections = pd.DataFrame(connections)

        return connections


    def display_dataset_graph(self,
                              id: Union[int, None] = None,
                              name: Union[str, None] = None,
                              sourceid: Union[int, None] = None,
                              max_distance: Union[int, None] = None) \
                              -> pd.DataFrame:

        # import now due to display functions
        import matplotlib.pyplot as plt
        import networkx as nx

        connections = self.get_dataset_connections(id,
                                                   name,
                                                   sourceid,
                                                   max_distance)


        G = nx.from_pandas_edgelist(connections,
                                     "from",
                                     "to",
                                     create_using=nx.DiGraph())
        pos = nx.layout.spring_layout(G)
        nx.draw(G, pos, with_labels=True, width=2, arrowsize=20)
        plt.show()


    def _upload_dataset(self, input, params: dict) -> pd.DataFrame:
        # just return
        return params["dataset"]


    def _create_iota(self,
                     row: pd.Series,
                     start_time: float,
                     total_upload: int,
                     id_obj: dict):

        # current row
        groupid = row.name

        key_index = 0
        index = (groupid + 1) * key_index

        # update progress
        tools.print_progress(count=index,
                             start_time=start_time,
                             total=total_upload)

        # parse row
        r = dict(row)
        for key, value in r.items():
            if isinstance(value, str):
                value = value.replace("\n", "")

            # basic items
            to_add = {"GroupId": int(groupid),
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

                iota_info = self._insert_to_table("Iota", arr_info)
                id_obj[iota_info["IotaId"]] = arr_info

            iota_info = self._insert_to_table("Iota", to_add)
            id_obj[iota_info["IotaId"]] = to_add

            key_index += 1
            index = (groupid + 1) * key_index
            # update progress
            tools.print_progress(count=index,
                                 start_time=start_time,
                                 total=total_upload)


    def _create_dataset(self, params) -> dict:
        # unpack values
        dataset = params["dataset"]
        name = params["name"]
        description = params["description"]
        type_map = params["type_map"]
        value_validation_map = params["value_validation_map"]
        import_as_type_map = params["import_as_type_map"]
        store_files = params["store_files"]
        force_storage = params["force_storage"]
        filepath_columns = params["filepath_columns"]
        replace_paths = params["replace_paths"]
        source_type = params["source_type"]
        source_type_id = params["source_type_id"]
        source_info = params["source_info"]

        # read dataset
        if isinstance(dataset, pathlib.Path):
            dataset = pd.read_csv(dataset)

        # update name
        if name is None:
            dataset = dataset.copy()
            ds_path = tools.create_pickle_file(dataset)
            name = "hash_" + self.fms.get_hash(ds_path)
            os.remove(ds_path)

        # convert types
        if isinstance(filepath_columns, str):
            filepath_columns = [filepath_columns]

        # create or get user
        user_info = self.get_items_from_table("User", ["Name", "=", self.user])
        if len(user_info) == 0:
            user_info = self._insert_to_table("User",
                            {"Name": self.user,
                             "Created": datetime.now()})
        else:
            user_info = user_info[0]

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

        # store files
        if filepath_columns is not None and store_files:
            if not force_storage:
                total_size = 0
                for i, row in dataset[filepath_columns].iterrows():
                    for item in row:
                        total_size += os.path.getsize(item)
                total_size = tools.convert_size(total_size)

                approval = tools.get_yes_no_input(
                    DUPLICATE_DATA_CONFIRM.format(s=total_size))

                if not approval:
                    print("Stopping upload...")
                    return None

            for col in filepath_columns:
                for i, item in enumerate(dataset[col]):
                    file_info = self.fms.get_or_create_file(item)
                    dataset[col][i] = file_info["ReadPath"]

        now = datetime.now()

        # create or get file source
        if source_type == "FileSource":
            source_type_info = self._insert_to_table("FileSource",
                            {"FileId": source_type_id,
                             "SourceId": source_info["SourceId"],
                             "Created": now})
        # get or create quilt source
        elif source_type == "QuiltSource":
            source_type_info = self._insert_to_table("QuiltSource",
                            {"PackageString": source_type_id,
                             "SourceId": source_info["SourceId"],
                             "Created": now})
        # handle invalid source type
        elif source_type != "RunSource":
            raise ValueError("Invalid source type passed to create dataset.")

        # begin iota creation
        print("\nCreating Iota...")

        iota = {}
        start_time = time.time()
        total_upload = (len(dataset) * len(dataset.columns))
        dataset.apply(lambda row: self._create_iota(row,
                                                    start_time,
                                                    total_upload,
                                                    iota), axis=1)

        # create dataset
        dataset_info = self._insert_to_table("Dataset",
            {"Name": name,
             "Description": description,
             "SourceId": source_info["SourceId"],
             "FilepathColumns": str(filepath_columns),
             "Created": datetime.now()})
        output_dataset_id = dataset_info["DatasetId"]

        # iota created, create junction items
        print("\nCreating Junction Items...")

        # start new progress bar
        current_i = 0
        start_time = time.time()
        total_upload = len(iota)
        for iota_id in iota:
            # update progress
            tools.print_progress(count=current_i,
                                 start_time=start_time,
                                 total=total_upload)

            junction_item = {
                "IotaId": iota_id,
                "DatasetId": output_dataset_id,
                "Created": datetime.now()}

            # insert junction item
            self._insert_to_table("IotaDatasetJunction", junction_item)

            # update progress
            current_i += 1
            tools.print_progress(count=current_i,
                                 start_time=start_time,
                                 total=total_upload)

        print("\nDataset upload complete!")

        return dataset_info


    def __getitem__(self, key):
        if isinstance(key, str):
            return self.database.table(str)

        raise KeyError("Table or item in table is not present in database")


    def _deep_print(self):
        print(("-" * 31) + " DATASET DATABASE " + ("-" * 31))

        for name, table in self.tables.items():
            print("-" * 80)
            try:
                print("Recent " + name + ":")
                if name == "Run":
                    recent = table\
                                  .order_by("End", "desc")\
                                  .limit(self.cache_size)\
                                  .get()
                else:
                    recent = table\
                                  .order_by("Created", "desc")\
                                  .limit(self.cache_size)\
                                  .get()

                if self.driver == "postgres" or self.driver == "postgresql":
                    recent = [dict(row) for row in recent]

                for item in recent:
                    print(item)

            except (orator.exceptions.query.QueryException, KeyError) as e:
                print("!package or schema not up to date!")


    def __str__(self):
        disp = "Recent Datasets:"
        disp += ("\n" + ("-" * 80))
        recent = self.tables["Dataset"]\
                     .order_by("Created", "desc")\
                     .limit(self.cache_size)\
                     .get()

        if self.driver == "postgres" or self.driver == "postgresql":
            recent = [dict(row) for row in recent]

        for item in recent:
            disp += ("\n" + str(item))

        return disp


    def __repr__(self):
        return str(self)
