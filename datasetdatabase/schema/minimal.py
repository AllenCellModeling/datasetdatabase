#!/usr/bin/env python

# installed
from datetime import datetime
import getpass

# self
from ..version import __version__
from ..schema import tables
from ..utils import checks

# globals
# CREATION ORDER OF TABLES MATTERS
TABLES = {"User": tables.create_User,
          "Iota": tables.create_Iota,
          "Group": tables.create_Group,
          "Source": tables.create_Source,
          "FileSource": tables.create_FileSource,
          "QuiltSource": tables.create_QuiltSource,
          "Dataset": tables.create_Dataset,
          "IotaGroupJunction": tables.create_IotaGroupJunction,
          "GroupDatasetJunction": tables.create_GroupDatasetJunction,
          "Algorithm": tables.create_Algorithm,
          "Run": tables.create_Run,
          "RunInput": tables.create_RunInput,
          "RunOutput": tables.create_RunOutput,
          "RunSource": tables.create_RunSource}


class SchemaVersion(object):

    def __init__(self, dsdb):
        self.dsdb = dsdb
        self.tables = TABLES


    def create_schema(self):
        for tbl, func in TABLES.items():
            func(self.dsdb.schema)
            self.dsdb.tables[tbl] = self.dsdb.database.table(tbl)


    def drop_schema(self):
        drop_order = list(TABLES.keys())
        drop_order.reverse()

        for tbl in drop_order:
            self.dsdb.schema.drop_if_exists(tbl)
            try:
                self.dsdb.tables.pop(tbl)
            except KeyError:
                pass

        self.dsdb.fms.handle_drop_schema()


    def add_basic_info(self):
        return None


    def get_tables(self):
        if self.dsdb.driver == "sqlite":
            names = self.dsdb.database.table("sqlite_master") \
                               .select("name") \
                               .where("type", "=", "table").get()
            names = [t["name"] for t in names if t["name"] != "sqlite_sequence"]
        else:
            names = self.dsdb.database.table("pg_catalog.pg_tables") \
                               .select("tablename") \
                               .where("schemaname", "=", "public").get()

            names = [dict(t)["tablename"] for t in names]
            names = [n for n in names if n in TABLES]

        return {n: self.dsdb.database.table(n) for n in names}
