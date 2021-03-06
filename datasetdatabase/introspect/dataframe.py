#!/usr/bin/env python

# installed
from multiprocessing.dummy import Pool
from typing import Dict, List, Union
from datetime import datetime
from functools import partial
import _pickle as pickle
import pandas as pd
import hashlib
import orator
import types
import os

# self
from ..schema.filemanagers import FMSInterface
from ..utils import checks, tools, ProgressBar
from .introspector import Introspector


class DataFrameIntrospector(Introspector):
    """
    Pandas DataFrame introspector. Ingest a DataFrame, validate, store files,
    deconstruct, reconstruct, and package.

    #### Example
    ```
    >>> DataFrameIntrospector(data)
    <class datasetdatabase.introspect.dataframe.DataFrameIntrospector>

    ```


    #### Parameters
    ##### obj: pd.DataFrame
    The dataframe you want to validate and potentially store in a dataset
    database.


    #### Returns
    ##### self


    #### Errors

    """

    def __init__(self, obj: pd.DataFrame):
        # enforce types
        checks.check_types(obj, pd.DataFrame)

        # store obj and ensure index
        self._obj = obj.reset_index(drop=True)

        # enforce column order
        columns = sorted(self.obj.columns)
        self._obj = self.obj.reindex(columns, axis=1)

        # set validated state
        self._validated = {"types": {k: False for k in self.obj.columns},
                           "values": {k: False for k in self.obj.columns},
                           "files": False}
        self.filepath_columns = None

    @property
    def obj(self):
        return self._obj

    @property
    def validated(self):
        return self._validated

    def get_object_hash(self, alg: types.BuiltinMethodType = hashlib.md5):
        """
        Get a unique and reproducible hash from the dataframe. DataFrame
        hashing can be a bit tricky so this function is rather memory intensive
        because it copies the bytes of every key-value pair in the dataframe
        but this is in my opinion the best way to ensure a reproducible hash of
        a dataset.

        #### Example
        ```
        >>> df_introspector.get_object_hash()
        asdf123asd3fhas2423dfhjkasd8f92178hb5sdf

        >>> df_introspector.get_object_hash(hashlib.sha256)
        8hkasdr823hklasdf7832balkjsdf73lkasdjhf73blkakljs892hksdf9

        ```


        #### Parameters
        ##### alg: types.BuiltinMethodType = hashlib.md5
        A hashing algorithm provided by hashlib.


        #### Returns
        ##### hash: str
        The hexdigest of the object hash.


        #### Errors

        """

        # create array
        barray = []

        # fill array with byte values of every key-value pair
        for i, row in self.obj.iterrows():
            for key, val in row.items():
                barray.append(pickle.dumps({key: val}))

        # return hexdigest of array
        return tools.get_object_hash(barray, alg=alg)

    def _format_dataset(self, type_map=None):
        # enforce types
        checks.check_types(type_map, [dict, type(None)])

        # format data if type_map was passed
        if type_map is not None:
            bar = ProgressBar(len(self.obj) * len(type_map))
            for key, ctype in type_map.items():
                self.obj[key] = self.obj[key].apply(lambda v: bar.apply_and_update(
                    tools.quick_cast, value=v, cast_type=ctype, info="column: " + key))

    def coerce_types_using_map(self, type_map: Union[Dict[str, type], None] = None):
        # enforce types
        checks.check_types(type_map, [dict, type(None)])

        # run casts
        if type_map is not None:
            print("Casting values to type map...")
            self._format_dataset(type_map)

        # updated validated
        self._validated["values"] = {**self._validated["values"],
                                     **{k: False for k in type_map}}
        self._validated["types"] = {**self._validated["types"],
                                    **{k: False for k in type_map}}

    def _validate_dataset_types(self, type_map=None):
        # enforce types
        checks.check_types(type_map, [dict, type(None)])

        # standard error
        err = "Dataset failed type check at:\n\tcolumn: {c}"

        # enforce data types
        if type_map is not None:
            bar = ProgressBar(len(self.obj) * len(type_map))
            for key, ctype in type_map.items():
                err = err.format(c=key)
                self.obj[key].apply(lambda x: bar.apply_and_update(checks.check_types, var=x, allowed=ctype, err=err))

    def enforce_types_using_map(self, type_map: Union[Dict[str, type], None] = None):
        # enforce types
        checks.check_types(type_map, [dict, type(None)])

        # run type checks
        if type_map is not None:
            print("Checking dataset value types...")
            self._validate_dataset_types(type_map)

        # types have been validated
        self._validated["types"] = {**self._validated["types"],
                                    **{k: True for k in type_map}}

    def _validate_dataset_values(self, validation_map=None):
        # enforce types
        checks.check_types(validation_map, [dict, type(None)])

        # standard error
        err = "Dataset failed data check at:\n\tcolumn: {c}"

        # enforce all dataset values to meet the lambda requirements
        if validation_map is not None:
            bar = ProgressBar(len(self.obj) * len(validation_map))
            for key, func in validation_map.items():
                self.obj[key].apply(lambda x: bar.apply_and_update(tools._assert_value, f=func, val=x, err=err.format(c=key)))

    def enforce_values_using_map(
        self,
        value_validation_map: Union[Dict[str, types.FunctionType], None] = None
    ):
        # enforce types
        checks.check_types(value_validation_map, [dict, type(None)])

        # run value checks
        if value_validation_map is not None:
            print("Checking dataset values...")
            self._validate_dataset_values(value_validation_map)

        # update validated
        self._validated["values"] = {**self._validated["values"],
                                     **{k: True for k in value_validation_map}}

    def _validate_dataset_files(self):
        err = "Dataset file not found at:\n\tcolumn: {c}"

        if self.filepath_columns is not None:
            bar = ProgressBar(len(self.obj) * len(self.filepath_columns))
            for key in self.filepath_columns:
                err = err.format(c=key)
                self.obj[key].apply(
                    lambda x: bar.apply_and_update(
                        checks.check_file_exists, f=x, err=err))

    def enforce_files_exist_from_columns(self, filepath_columns: Union[str, List[str], None] = None):
        # enforce types
        checks.check_types(filepath_columns, [str, list, type(None)])

        # convert types
        if isinstance(filepath_columns, str):
            filepath_columns = [filepath_columns]

        # update filepath columns
        if self.filepath_columns is None:
            self.filepath_columns = filepath_columns
        else:
            self.filepath_columns += list(set(filepath_columns) -
                                          set(self.filepath_columns))

        # run exists
        if self.filepath_columns is not None:
            print("Checking files exist...")
            self._validate_dataset_files()

            if not isinstance(self.validated, bool):
                # update validated
                self._validated["files"] = {
                    k: True for k in self.filepath_columns}

        # no columns passed
        else:
            if not isinstance(self.validated, bool):
                # update validated
                self._validated["files"] = False

    def store_files(
        self,
        dataset: "Dataset",
        db: orator.DatabaseManager,
        fms: FMSInterface,
        filepath_columns: Union[str, List[str], None] = None
    ):

        # enforce types
        checks.check_types(db, orator.DatabaseManager)
        checks.check_types(fms, FMSInterface)
        checks.check_types(filepath_columns, [str, list, type(None)])

        # enforce exists
        self.enforce_files_exist_from_columns(filepath_columns)

        # update filepaths to storage files
        if self.filepath_columns is not None:
            print("Creating FMS stored files...")
            bar = ProgressBar(len(self.obj) * len(self.filepath_columns))
            for key in self.filepath_columns:
                self.obj[key] = self.obj[key].apply(
                    lambda x: bar.apply_and_update(
                        fms.get_or_create_file, db=db, filepath=x)["ReadPath"])

        return self.obj

    def validate(
        self,
        type_validation_map: Union[Dict[str, type], None] = None,
        value_validation_map: Union[Dict[str, types.FunctionType], None] = None,
        filepath_columns: Union[List[str], str, None] = None,
        import_as_type_map: bool = False
    ):
        # coerce
        if import_as_type_map:
            self.coerce_types_using_map(type_validation_map)

        # validate
        if type_validation_map is not None:
            self.enforce_types_using_map(type_validation_map)
        if value_validation_map is not None:
            self.enforce_values_using_map(value_validation_map)
        if filepath_columns is not None:
            self.enforce_files_exist_from_columns(filepath_columns)

    def deconstruct(self, db: orator.DatabaseManager, ds_info: "DatasetInfo", fms: FMSInterface):
        # create bar
        bar = ProgressBar(len(self.obj))

        # begin teardown
        print("Tearing down object...")

        # create func
        func = partial(_deconstruct_Group,
                       database=db,
                       ds_info=ds_info,
                       progress_bar=bar)

        # get safe thread count
        if "DSDB_PROCESS_LIMIT" in os.environ:
            n_threads = int(os.environ["DSDB_PROCESS_LIMIT"])
        else:
            n_threads = os.cpu_count()

        # insert row labels
        indices = pd.Series(range(len(self.obj)))
        rows = self.obj.assign(__DSDB_GROUP_LABEL__=indices)

        # pre build rows
        rows = rows.to_dict("records")

        # create pool
        with Pool(n_threads) as pool:
            # map pool
            pool.map(func, rows)

    def package(self):
        package = {}
        package["data"] = self.obj
        package["files"] = None
        return package


def _deconstruct_Group(row, database, ds_info, progress_bar):
    # all iota are created at the same time
    created = datetime.utcnow()

    # get and remove label
    label = str(row.pop("__DSDB_GROUP_LABEL__"))

    # create iota list
    iota = []

    # generate iota
    for k, v in row.items():
        # create iota
        i = {"Key": k,
             "Value": pickle.dumps(v),
             "Created": created}

        # insert iota
        iota.append(tools.insert_to_db_table(database, "Iota", i))

    # create hash target
    to_hash = [i["IotaId"] for i in iota]

    # create group
    group = {"MD5": tools.get_object_hash(to_hash),
             "Created": created}

    # insert group
    group = tools.insert_to_db_table(database, "Group", group)

    # create group_dataset
    group_dataset = {"GroupId": group["GroupId"],
                     "DatasetId": ds_info.id,
                     "Label": label,
                     "Created": created}

    # insert group_dataset
    group_dataset = tools.insert_to_db_table(
        database, "GroupDataset", group_dataset)

    # generate iota_group joins
    for i in iota:
        # create iota_group
        iota_group = {"IotaId": i["IotaId"],
                      "GroupId": group["GroupId"],
                      "Created": created}

        # insert iota_group
        iota_group = tools.insert_to_db_table(
            database, "IotaGroup", iota_group)

    # update progress
    progress_bar.increment()


def reconstruct(
    db: orator.DatabaseManager,
    ds_info: "DatasetInfo",
    fms: FMSInterface
) -> pd.DataFrame:

    # get all iota that match
    data = [
        dict(r) for r in db.table("Iota")
        .join("IotaGroup", "IotaGroup.IotaId", "=", "Iota.IotaId")
        .join("GroupDataset", "GroupDataset.GroupId", "=", "IotaGroup.GroupId")
        .join("Dataset", "GroupDataset.DatasetId", "=", "Dataset.DatasetId")
        .where("Dataset.DatasetId", "=", ds_info.id)
        .get()
    ]

    # create dictionary of iota with key being their group label
    groups = {}
    for iota in data:
        label = int(iota["Label"])
        if label not in groups:
            groups[label] = {}

        groups[label][iota["Key"]] = pickle.loads(iota["Value"])

    # we know that dataframe labels are actually just their index value
    # so we can append these rows in order by simply looping through a range of their length and getting each one
    rows = []
    for i in range(len(groups)):
        rows.append(groups[i])

    # return frame
    return pd.DataFrame(rows)
