#!/usr/bin/env python

# installed
from datetime import datetime
import getpass
import orator

# self
from ..schema import tables
from ..utils import checks

# globals
# CREATION ORDER OF TABLES MATTERS
TABLES = {"SourceType": tables.create_SourceType,
          "User": tables.create_User,
          "Iota": tables.create_Iota,
          "Dataset": tables.create_Dataset,
          "IotaDatasetJunction": tables.create_IotaDatasetJunction,
          "Algorithm": tables.create_Algorithm,
          "Run": tables.create_Run}


def create_schema(db):
    for tbl, func in TABLES.items():
        func(db.schema)
        db.tables[tbl] = db.database.table(tbl)


def drop_schema(db):
    drop_order = list(TABLES.keys()).reverse()

    for tbl in drop_order:
        db.schema.drop_if_exists(tbl)
        del db.tables[tbl]

def add_basic_info(db):
    # add SourceType information
    try:
        db.database.table("SourceType").insert([
            {"Type": "File",
             "Description": "SourceId is a File GUID if connected to an FMS.",
             "Created": datetime.now()},
            {"Type": "Run",
             "Description": "SourceId is a RunId.",
             "Created": datetime.now()}
        ])
    except Exception as e:
        checks.check_ingest_error(e)

    # add User information
    try:
        db.database.table("User").insert([
            {"Name": getpass.getuser(),
             "Description": "admin",
             "Created": datetime.now()}
        ])
    except Exception as e:
        checks.check_ingest_error(e)

def get_tables(db):
    if db.driver == "sqlite":
        names = db.database.table("sqlite_master") \
                           .select("name") \
                           .where("type", "=", "table").get()
        names = [t["name"] for t in names if t["name"] != "sqlite_sequence"]
    else:
        names = db.database.table("pg_catalog.pg_tables") \
                           .select("name") \
                           .where("schemaname", "=", "public").get()
        names = [dict(t)["name"] for t in names]

    return {n: db.database.table(n) for n in names}
