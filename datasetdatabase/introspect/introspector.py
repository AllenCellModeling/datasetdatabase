#!/usr/bin/env python

# installed
from typing import Dict, List
import abc

class Introspector(abc.ABC):
    """
    Introspectors are a class of object that help a Dataset object initialize,
    validate, deconstruct, and reconstruct objects. This is the abstract base
    class for all others to be built off of. In short a custom Introspector
    needs to have an init, a validated property, as well as a validate
    function, deconstruct and reconstruct functions, and finally a package
    build function that allows for your 'Dataset' object to be fully packaged
    with all it's dependancies with Quilt.

    For a more defined example of an Introspector look at the
    DataFrameInspector.
    """
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
    def validate(self, **kwargs):
        """
        Validate the object using passed **kwargs.
        """
        self._validated = True


    @abc.abstractmethod
    def deconstruct(self) -> Dict[str, List[Dict[str, object]]]:
        """
        Generate a dictionary of lists of Iota, Group, and IotaGroup objects.

        As the Introspector object does not have access to the DatasetDatabase
        connection, IotaIds and GroupIds should be their index in their
        respective lists when creating the IotaGroup list.
        """
        storage = {}
        storage["Iota"] = [{}]
        storage["Group"] = [{}]
        storage["IotaGroup"] = [{}]
        return storage


    @abc.abstractmethod
    def reconstruct(self, items: Dict[str, Dict[str, object]]) -> object:
        """
        Given dictionary of lists of Iota, Group, and IotaGroup objects,
        reconstruct to the base object.
        """
        return self.obj


    @abc.abstractmethod
    def package(self, items: Dict[str, Dict[str, object]]) -> Dict[str, object]:
        """
        Because these are incredibly arbitrary objects, there is not default
        way of inferring a package standard between them. Due to this it is
        recommended that if you want to share data external of this database.
        You will need to write a packaging function that returns both "data"
        and "files" attributes. If there are no supporting files, return None.
        """
        package = {}
        package["data"] = self.obj
        package["files"] = None
        return package
