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
          "SourceType": tables.create_SourceType,
          "Source": tables.create_Source,
          "FileSource": tables.create_FileSource,
          "Dataset": tables.create_Dataset,
          "IotaDatasetJunction": tables.create_IotaDatasetJunction,
          "Algorithm": tables.create_Algorithm,
          "Run": tables.create_Run,
          "RunInput": tables.create_RunInput,
          "RunOutput": tables.create_RunOutput,
          "RunSource": tables.create_RunSource}


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
    now = datetime.now()

    # add SourceType
    try:
        db.database.table("SourceType").insert([
            {"Name": "FileSource",
             "Description": "Id attached should be read using FMS get.",
             "Created": now},
            {"Name": "RunSource",
             "Description": "Id attached should be read using dataset get.",
             "Created": now}
        ])
    except Exception as e:
        checks.check_ingest_error(e)

    # add dsdb Algorithms
    # try:
    #     db.database.table("Algorithm").insert([
    #         {"Name": "upload_dataset",
    #          "Version": __version__,
    #          "Description": "DatasetDatabase ingest function",
    #          "Created": now},
    #         {"Name": "create_dataset",
    #          "Version": __version__,
    #          "Description": "DatasetDatabase create function",
    #          "Created": now}
    #     ])
    # except Exception as e:
    #     checks.check_ingest_error(e)


def get_tables(db):
    if db.driver == "sqlite":
        names = db.database.table("sqlite_master") \
                           .select("name") \
                           .where("type", "=", "table").get()
        names = [t["name"] for t in names if t["name"] != "sqlite_sequence"]
    else:
        names = db.database.table("pg_catalog.pg_tables") \
                           .select("tablename") \
                           .where("schemaname", "=", "public").get()

        names = [dict(t)["tablename"] for t in names]
        names = [n for n in names if n in TABLES]

    return {n: db.database.table(n) for n in names}
