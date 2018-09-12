#!/usr/bin/env python

# installed
from typing import Dict, List, Union
from datetime import datetime
import orator
import pickle
import types
import uuid

# self
from ..schema.filemanagers import FMSInterface
from ..utils import checks, tools, ProgressBar
from .introspector import Introspector


class DictionaryIntrospector(Introspector):
    """
    General dictionary introspector. Ingest a dictionary, create and validate,
    Iota, etc.
    """

    def __init__(self, obj: dict):
        # enforce types
        checks.check_types(obj, dict)

        self._obj = obj
        self._validated = {k: False for k in self.obj}


    @property
    def obj(self):
        return self._obj


    @property
    def validated(self):
        return self._validated


    def validate(self,
        item_validation_map: Union[None,
            Dict[str, Union[types.ModuleType, types.FunctionType]]] = None):

        # enforce types
        checks.check_types(item_validation_map, [dict, type(None)])

        # validate
        if item_validation_map is not None:
            new_validations = {k: self.validate_key(k, item_validation_map[k])
                                for k in item_validation_map}
            self._validated = {**self._validated, **new_validations}


    def deconstruct(self, db, ds_info):
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

        for k, v in self.obj.items():
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


    def store_files(self,
        dataset: "Dataset",
        db: orator.DatabaseManager,
        fms: FMSInterface,
        keys: Union[str, List[str], None] = None):
        # enforce types
        checks.check_types(db, orator.DatabaseManager)
        checks.check_types(fms, FMSInterface)
        checks.check_types(keys, [str, list, type(None)])

        # convert types
        if isinstance(keys, str):
            keys = [keys]

        # run store
        if isinstance(keys, list):
            for key in keys:
                # check exists
                checks.check_file_exists(self.obj[key])

                # store
                self._obj[key] = \
                    fms.get_or_create_file(
                        db=db, filepath=self.obj[key])["ReadPath"]
                self._validated[key] = True

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


def reconstruct(items: Dict[str, Dict[str, object]]) -> dict:
    obj = {i["Key"]: pickle.loads(i["Value"])
                    for i in items["Iota"]}
    return obj
