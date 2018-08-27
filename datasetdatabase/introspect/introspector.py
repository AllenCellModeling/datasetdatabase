#!/usr/bin/env python

# installed
from typing import Dict, List
import abc

class Introspector(abc.ABC):
    def __init__(self, obj: object):
        self._obj = obj

    @property
    def obj(self):
        return self._obj

    @abc.abstractmethod
    def parse(self) -> Dict[str, List[int]]:
        return
