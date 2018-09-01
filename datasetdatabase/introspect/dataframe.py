#!/usr/bin/env python

# installed
from typing import Dict, List, Union
from datetime import datetime
import pandas as pd
import pickle
import types

# self
from ..utils import checks, tools, ProgressBar
from .introspector import Introspector


class DataFrameIntrospector(Introspector):
    """
    Pandas DataFrame introspector. Ingest a DataFrame, create and validate
    Iota, etc.
    """

    def __init__(self, obj: pd.DataFrame):
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

        # get columns
        if filepath_columns is None:
            if self.filepath_columns is not None:
                filepath_columns = self.filepath_columns
        # update filepath columns
        else:
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


    def store_files(self,
        filepath_columns: Union[str, List[str], None] = None):

        # enforce types
        checks.check_types(filepath_columns, [str, list, type(None)])

        # update filepath columns
        if self.filepath_columns is None:
            self.filepath_columns = filepath_columns
        else:
            self.filepath_columns += list(set(filepath_columns) -
                                          set(self.filepath_columns))

        self.enforce_files_exist_from_columns()

        # schedule file conversion
        print("file storage not implemented yet... :/")


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


    def _create_Group(self, row, storage):
        # create group
        created = datetime.now()
        group = {"GroupId": row.name,
                 "Label": row.name,
                 "Created": created}

        # add group
        if group not in storage["Group"]:
            storage["Group"].append(group)

        # find group
        group_id = storage["Group"].index(group)

        # generate iota and iota_group
        for k, v in row.items():
            # create iota
            iota = {"Key": k,
                    "Value": pickle.dumps(v),
                    "Created": created}

            # add iota
            if iota not in storage["Iota"]:
                storage["Iota"].append(iota)

            # find iota
            iota_id = storage["Iota"].index(iota)

            # create iota_group
            iota_group = {"IotaId": iota_id,
                          "GroupId": group_id,
                          "Created": created}

            # add iota_group
            storage["IotaGroup"].append(iota_group)


    def deconstruct(self) -> Dict[str, List[Dict[str, object]]]:
        storage = {}
        storage["Iota"] = []
        storage["Group"] = []
        storage["IotaGroup"] = []

        bar = ProgressBar(len(self.obj) * len(self.obj.columns),
                            increment = len(self.obj.columns))
        self.obj.apply(lambda row: bar.apply_and_update(self._create_Group,
                    row=row, storage=storage), axis=1)
        return storage


    def reconstruct(self, items: Dict[str, Dict[str, object]]) -> object:
        rows = [{} for i in range(len(items["Group"]))]
        for iota_group in items["IotaGroup"]:
            iota = items["Iota"][iota_group["IotaId"]]
            group = items["Group"][iota_group["GroupId"]]
            rows[group["Label"]] = {**rows[group["Label"]],
                                **{iota["Key"]: pickle.loads(iota["Value"])}}

        self._obj = pd.DataFrame(rows)
        return self.obj


    def package(self):
        package = {}
        package["data"] = self.obj
        package["files"] = None
        return package


    def validate_key(self,
        key: str,
        func: Union[types.ModuleType, types.FunctionType]) -> bool:

        return func(self.obj[key])
