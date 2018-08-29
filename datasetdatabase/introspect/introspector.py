#!/usr/bin/env python

# installed
from typing import Dict, List
import abc

class Introspector(abc.ABC):
    def __init__(self, obj: object):
        self._obj = obj
        self._validated = False


    @property
    def obj(self):
        return self._obj


    @property
    @abc.abstractmethod
    def validated(self):
        return self._validated


    @abc.abstractmethod
    def deconstruct(self) -> Dict[str, List[int]]:
        return


    @abc.abstractmethod
    def reconstruct(self, items_map: Dict[str, Dict[str, str]]) -> object:
        return
