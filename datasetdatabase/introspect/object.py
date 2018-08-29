#!/usr/bin/env python

# installed
from typing import Dict, List, Union, Tuple
from datetime import datetime
import pickle
import types

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

        # quick end
        if item_validation_map is None:
            self._validated = True
            return

        # validate
        if all(self.validate_attribute(i, f) for i, f
                in item_validation_map.items()):
            self._validated = True
            return


    def deconstruct(self) -> Dict[str, List[Dict[str, object]]]:
        storage = {}
        storage["Iota"] = [{"Key": "obj",
                            "Value": pickle.dumps(self.obj),
                            "Created": datetime.now()}]
        storage["Group"] = [{"Created": datetime.now()}]
        storage["IotaGroup"] = [{"IotaId": 0,
                                 "GroupId": 0,
                                 "Created": datetime.now()}]
        return storage


    def reconstruct(self, items: Dict[str, Dict[str, str]]) -> object:
        return {}


    def package(self):
        return {}


    def validate_attribute(self,
        item: str,
        func: Union[types.ModuleType, types.FunctionType]) -> bool:

        return func(getattr(self.obj, item))
