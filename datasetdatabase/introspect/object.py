#!/usr/bin/env python

# installed
from typing import Dict, List, Union
from datetime import datetime
import pickle
import types
import uuid

# self
from .introspector import Introspector
from ..utils import checks


class ObjectIntrospector(Introspector):
    """
    General object introspector. Using a single all attribute validation map,
    you can ingest, validate, and deconstruct, an object. There is limited
    functionality due to how generalizable this Introspector needs to be.
    """

    def __init__(self, obj: object):
        self._obj = obj
        self._validated = False


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
            for i, f in item_validation_map.items():
                if not self.validate_attribute(i, f):
                    raise ValueError("Attribute failed validation: {a}"
                                    .format(a=i))

            self._validated = True


    def deconstruct(self) -> Dict[str, List[Dict[str, object]]]:
        storage = {}

        # all iota are created at the same time
        created = datetime.now()

        storage["Iota"] = [{"Key": "obj",
                            "Value": pickle.dumps(self.obj),
                            "Created": created}]
        storage["Group"] = [{"Label": str(uuid.uuid4()),
                             "Created": created}]
        storage["IotaGroup"] = [{"IotaId": 0,
                                 "GroupId": 0,
                                 "Created": created}]
        return storage


    def package(self):
        package = {}
        package["data"] = self.obj
        package["files"] = None
        return package


    def validate_attribute(self,
        item: str,
        func: Union[types.ModuleType, types.FunctionType]) -> bool:

        return func(getattr(self.obj, item))


def reconstruct(items: Dict[str, Dict[str, object]]) -> object:
    obj = pickle.loads(items["Iota"][0]["Value"])
    return obj
