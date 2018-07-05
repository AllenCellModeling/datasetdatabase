#!/usr/bin/env python

# installed
import pandas as pd
import getpass
import orator
import types

# self
from ..utils import checks
from ..schema import minimal


class DatasetDatabase(object):
    """

    """

    def __init__(self,
                 config: dict,
                 build: bool = True,
                 user: Union[str, None] = None,
                 version: types.ModuleType = minimal):

        # enforce types
        checks.check_types(config, dict)
        checks.check_types(build, bool)
        checks.check_types(user, [str, type(None)])
        checks.check_types(version, types.ModuleType)

        # handle user
        if user is None:
            user = getpass.getuser()
        if user in ["jovyan", "root", "admin"]:
            raise ValueError("Could not validate user. Please specify.")

        self.user = user

        # construct basic items
        self.database = orator.DatabaseManager(config)
        self.schema = orator.Schema(self.database)
        self.driver = config[list(config.keys())[0]]["driver"]
        self.tables = version.get_tables(self)

        # create tables
        if build:
            version.create_schema(self)
            version.add_basic_info(self)
            self.tables = version.get_tables(self)

        # recent since connection made
        self.recent = []


    def ingest(self):
        return


    def __getitem__(self, key):
        return


    def __str__(self):
        disp = (("-" * 31) + " DATASET DATABASE " + ("-" * 31))

        for name, tbl in self.tables.items():
            disp += ("\n" + ("-" * 80))

            disp += ("\n" + name + ":")
            if isinstance(tbl, orator.query.builder.QueryBuilder):
                disp += "\n\trows: {r}".format(r=tbl.count())
                disp += "\n\texample: {r}".format(r=tbl.first())
            else:
                disp += "\n\t{t}".format(t=type(tbl))

        disp += ("\n" + ("-" * 32) + " RECENTLY ADDED " + ("-" * 32))
        disp += ("\n" + ("-" * 80))
        for row in self.recent:
            disp += "\n{r}".format(r=row)

        return disp


    def __repr__(self):
        return str(self)
