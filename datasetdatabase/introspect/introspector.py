#!/usr/bin/env python

# installed
from typing import Dict
import orator
import abc


class Introspector(abc.ABC):
    """
    Introspectors are a class of object that help a Dataset object initialize,
    validate, deconstruct, reconstruct, and package objects. This is the
    abstract base class for all others to be built off of. In short a custom
    Introspector needs to have an init, a validated property, as well as a
    validate function, deconstruct and reconstruct functions, and finally a
    package build function that allows for your 'Dataset' object to be fully
    packaged with all it's dependancies with Quilt.

    For a more defined example of an Introspector look at the
    DataFrameIntrospector.
    """

    def __init__(self, obj: object):
        self._obj = obj
        self._validated = False

    @property
    def obj(self):
        """
        The obj property must return the underlying object in the way you would
        like the user to view it.
        """

        return self._obj

    @property
    @abc.abstractmethod
    def validated(self):
        """
        The validated property must return validation information so that the
        user can determine what has and has not been validated.
        """

        return self._validated

    def get_object_hash(self):
        """
        Generate a hash for the object that can be reproduced given a
        reconstruction from binary of the object. Sometimes reconstructions
        from binary fail due to having different memory optimizations than the
        original.
        """
        return hash(self.obj)

    @abc.abstractmethod
    def validate(self, **kwargs):
        """
        Validate the object using passed **kwargs.
        """
        self._validated = True

    @abc.abstractmethod
    def deconstruct(self, db: orator.DatabaseManager, ds_info: "DatasetInfo", fms: "FMSInterface"):
        """
        Generate and insert all Iota, Group, IotaGroup, and GroupDataset items
        to the database passed using the ds_info passed when needed on things
        like GroupDataset joins.
        """
        return None

    @abc.abstractmethod
    def package(self, items: Dict[str, Dict[str, object]]):
        """
        Because these are incredibly arbitrary objects, there is no default
        way of inferring a package standard between them. Due to this it is
        recommended that if you want to share data externally from this
        database. You will need to write a packaging function that returns both
        "data" and "files" attributes. If there are no supporting files, set
        the files key to None.
        """
        package = {}
        package["data"] = self.obj
        package["files"] = None
        return package


@abc.abstractmethod
def reconstruct(db: orator.DatabaseManager, ds_info: "DatasetInfo", fms: "FMSInterface") -> object:
    """
    Do the reverse operation of the deconstruct and when given a database link
    and a dataset info, reconstruct the object and return it.
    """
    obj = {}
    return obj
