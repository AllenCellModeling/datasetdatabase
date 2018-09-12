#!/usr/bin/env python

# installed
from typing import Dict, List, Union
from datetime import datetime
import _pickle as pickle
import pandas as pd
import orator
import types
import uuid

# self
from ..schema.filemanagers import FMSInterface
from ..utils import checks, tools, ProgressBar
from .introspector import Introspector


class DataFrameIntrospector(Introspector):
    """
    Pandas DataFrame introspector. Ingest a DataFrame, create and validate
    Iota, etc.
    """

    def __init__(self, obj: pd.DataFrame):
        # enforce types
        checks.check_types(obj, pd.DataFrame)

        self._obj = obj
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


    def _format_dataset(self, type_map = None):
        # enforce types
        checks.check_types(type_map, [dict, type(None)])

        # format data if type_map was passed
        if type_map is not None:
            bar = ProgressBar(len(self.obj) * len(type_map))
            for key, ctype in type_map.items():
                self.obj[key] = self.obj[key].apply(
                    lambda v: bar.apply_and_update(tools.quick_cast,
                            value=v, cast_type=ctype, info="column: " + key))


    def coerce_types_using_map(self,
        type_map: Union[Dict[str, type], None] = None):
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


    def _validate_dataset_types(self, type_map = None):
        # enforce types
        checks.check_types(type_map, [dict, type(None)])

        # standard error
        err = "Dataset failed type check at:\n\tcolumn: {c}"

        # enforce data types
        if type_map is not None:
            bar = ProgressBar(len(self.obj) * len(type_map))
            for key, ctype in type_map.items():
                err = err.format(c=key)
                self.obj[key].apply(
                lambda x: bar.apply_and_update(checks.check_types,
                                               var=x, allowed=ctype, err=err))


    def enforce_types_using_map(self,
        type_map: Union[Dict[str, type], None] = None):
        # enforce types
        checks.check_types(type_map, [dict, type(None)])

        # run type checks
        if type_map is not None:
            print("Checking dataset value types...")
            self._validate_dataset_types(type_map)

        # types have been validated
        self._validated["types"] = {**self._validated["types"],
                                    **{k: True for k in type_map}}


    def _validate_dataset_values(self, validation_map = None):
        # enforce types
        checks.check_types(validation_map, [dict, type(None)])

        # standard error
        err = "Dataset failed data check at:\n\tcolumn: {c}"

        # enforce all dataset values to meet the lambda requirements
        if validation_map is not None:
            bar = ProgressBar(len(self.obj) * len(validation_map))
            for key, func in validation_map.items():
                self.obj[key].apply(
                lambda x: bar.apply_and_update(tools._assert_value,
                                        f=func, val=x, err=err.format(c=key)))


    def enforce_values_using_map(self,
        value_validation_map: Union[Dict[str, types.FunctionType], None] \
            = None):
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


    def enforce_files_exist_from_columns(self,
        filepath_columns: Union[str, List[str], None] = None):
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

            # update validated
            self._validated["files"] = {k: True for k in self.filepath_columns}

        # no columns passed
        else:
            self._validated["files"] = False


    def store_files(self,
        dataset: "Dataset",
        db: orator.DatabaseManager,
        fms: FMSInterface,
        filepath_columns: Union[str, List[str], None] = None):

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


    def validate(self,
        type_validation_map: Union[Dict[str, type], None] = None,
        value_validation_map: Union[Dict[str, types.FunctionType], None] = None,
        filepath_columns: Union[List[str], str, None] = None,
        import_as_type_map: bool = False):
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


    def _create_Group(self, db, ds_info, row):
        # all iota are created at the same time
        created = datetime.now()

        # create group
        group = {"Label": str(uuid.uuid4()),
                 "Created": created}

        # insert group
        group = tools.insert_to_db_table(db, "Group", group)

        # create group_dataset
        group_dataset = {"GroupId": group["GroupId"],
                         "DatasetId": ds_info.id,
                         "Created": created}

        # insert group_dataset
        group_dataset = tools.insert_to_db_table(
            db, "GroupDataset", group_dataset)

        # generate iota and iota_group
        for k, v in row.items():
            # create iota
            iota = {"Key": k,
                    "Value": pickle.dumps(v),
                    "Created": created}

            # insert iota
            iota = tools.insert_to_db_table(db, "Iota", iota)

            # create iota_group
            iota_group = {"IotaId": iota["IotaId"],
                          "GroupId": group["GroupId"],
                          "Created": created}
            # insert iota_group
            iota_group = tools.insert_to_db_table(db, "IotaGroup", iota_group)


    def deconstruct(self, db: orator.DatabaseManager, ds_info: "DatasetInfo"):
        # create bar
        bar = ProgressBar(len(self.obj) * len(self.obj.columns),
            increment = len(self.obj.columns))

        # begin teardown
        print("Tearing down object...")
        self.obj.apply(lambda row: bar.apply_and_update(self._create_Group,
                    db=db, ds_info=ds_info, row=row), axis=1)


    def package(self):
        package = {}
        package["data"] = self.obj
        package["files"] = None
        return package


def reconstruct(db: orator.DatabaseManager,
    ds_info: "DatasetInfo") -> pd.DataFrame:
    # create group_datasets
    group_datasets = tools.get_items_from_db_table(
        db, "GroupDataset", ["DatasetId", "=", ds_info.id])

    # create rows
    rows = []

    # create groups
    print("Reconstructing dataset...")
    bar = ProgressBar(len(group_datasets))

    for group_dataset in group_datasets:
        # create iota_groups
        iota_groups = tools.get_items_from_db_table(
            db, "IotaGroup", ["GroupId", "=", group_dataset["GroupId"]])

        # create group
        group = {}
        for iota_group in iota_groups:
            # create iota
            iota = tools.get_items_from_db_table(
                db, "Iota", ["IotaId", "=", iota_group["IotaId"]])[0]

            # read value and attach to group
            group[iota["Key"]] = pickle.loads(iota["Value"])


        # attach completed group to rows
        rows.append(group)

        # update progress
        bar.increment()

    # return dataframe
    return pd.DataFrame(rows)
