#!/usr/bin/env python

# installed
from typing import Dict, List, Union
from datetime import datetime
import pickle
import types
import uuid

# self
from ..utils import checks, ProgressBar
from .introspector import Introspector


class DictionaryIntrospector(Introspector):
    """
    General dictionary introspector. Ingest a dictionary, create and validate,
    Iota, etc.
    """

    def __init__(self, obj: dict):
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


    def deconstruct(self) -> Dict[str, List[Dict[str, object]]]:
        storage = {}

        # all iota are created at the same time
        created = datetime.now()

        storage["Iota"] = [{"Key": k,
                            "Value": pickle.dumps(v),
                            "Created": created}
                            for k, v in self.obj.items()]
        storage["Group"] = [{"Label": str(uuid.uuid4()),
                             "Created": created}]
        storage["IotaGroup"] = [{"IotaId": i,
                                 "GroupId": 0,
                                 "Created": created}
                                 for i in range(len(storage["Iota"]))]
        return storage


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
