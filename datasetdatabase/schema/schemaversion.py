#!/usr/bin/env python

# installed
from typing import Union, Dict, List
import types

# self
from ..utils import checks


class SchemaVersion(object):
    def __init__(self,
        name: str,
        tables: Dict[str, types.ModuleType],
        version: Union[str, float, List[int]]):

        # enforce types
        checks.check_types(name, str)
        checks.check_types(tables, dict)
        checks.check_types(version, [str, float, list])

        # store attributes
        self._name = name
        self._tables = tables

        if isinstance(version, list):
            version = ".".join(version)

        self._version = str(version)


    @property
    def name(self):
        return self._name


    @property
    def tables(self):
        return self._tables


    @property
    def version(self):
        return self._version
