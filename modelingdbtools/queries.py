# installed
from orator.exceptions.query import QueryException
from orator import DatabaseManager
from datetime import datetime
import pandas as pd


# self
from .utils import checks
from .utils import handles

def display_all_tables(db):
    checks.check_types(db, DatabaseManager)

    all_tables = get_db_tables(db)
    print(all_tables)

    driver = handles.get_db_driver(db)

    if driver == "sqlite":
        all_tables = get_tables(db, all_tables["tbl_name"])
    else:
        all_tables = get_tables(db, all_tables["tablename"])

    for table, df in all_tables.items():
        print("-" * 80)
        print(table)
        print("-" * 20)
        print(df.head())

def get_db_tables(db):
    checks.check_types(db, DatabaseManager)

    driver = handles.get_db_driver(db)

    if driver == "sqlite":
        w = "WHERE type='table'"
        tables = db.select("SELECT * FROM sqlite_master {w}".format(w=w))
        tables = [t for t in tables if t["name"] != "sqlite_sequence"]
    else:
        w = "WHERE schemaname='public'"
        tables = db.select("SELECT * FROM pg_catalog.pg_tables {w}".format(w=w))
        tables = [dict(t) for t in tables]

    tables = pd.DataFrame(tables)

    return tables

def get_tables(db, get):
    checks.check_types(db, DatabaseManager)
    checks.check_types(get, [str, list, pd.Series])

    if not isinstance(get, list):
        get = list(get)

    tables = {}
    for table in get:
        rows = db.select("SELECT * FROM {t}".format(t=table))
        tables[table] = pd.DataFrame(rows)

    return tables

def add_filler_data(db):
    checks.check_types(db, DatabaseManager)

    try:
        db.table("SourceType").insert([
            {"Type": "File",
             "Description": "Use aics.FMS.File id to find source"},
            {"Type": "Dataset",
             "Description": "Use aics.Modeling.Dataset id to find source"}
        ])
    except QueryException:
        pass

    try:
        db.table("User").insert([
            {"Name": "jacksonb",
             "Description": "admin",
             "Created": datetime.now()},
            {"Name": "gregj",
             "Description": "scrub, noob, etc",
             "Created": datetime.now()}
        ])
    except QueryException as e:
        pass

    try:
        db.table("Iota").insert([
            {"SourceId": 1,
             "SourceTypeId": 1,
             "GroupId": 0,
             "Created": datetime.now(),
             "Key": "test",
             "Value": "value",
             "Parser": "str"},
            {"SourceId": 1,
             "SourceTypeId": 1,
             "GroupId": 0,
             "Created": datetime.now(),
             "Key": "hello",
             "Value": "world",
             "Parser": "str"},
            {"SourceId": 1,
             "SourceTypeId": 1,
             "GroupId": 0,
             "Created": datetime.now(),
             "Key": "test",
             "Value": "value",
             "Parser": "str"},
            {"SourceId": 1,
             "SourceTypeId": 1,
             "GroupId": 0,
             "Created": datetime.now(),
             "Key": "123",
             "Value": "456",
             "Parser": "int"},
            {"SourceId": 1,
             "SourceTypeId": 2,
             "GroupId": 0,
             "Created": datetime.now(),
             "Key": "is_test",
             "Value": "True",
             "Parser": "bool"},
            {"SourceId": 1,
             "SourceTypeId": 2,
             "GroupId": 0,
             "Created": datetime.now(),
             "Key": "13579",
             "Value": "24680",
             "Parser": "int"},
            {"SourceId": 1,
             "SourceTypeId": 2,
             "GroupId": 1,
             "Created": datetime.now(),
             "Key": "is_test",
             "Value": "True",
             "Parser": "bool"},
            {"SourceId": 1,
             "SourceTypeId": 2,
             "GroupId": 1,
             "Created": datetime.now(),
             "Key": "33333",
             "Value": "66666",
             "Parser": "int"},
            {"SourceId": 1,
             "SourceTypeId": 2,
             "GroupId": 0,
             "Created": datetime.now(),
             "Key": "is_output",
             "Value": "True",
             "Parser": "bool"}
        ])
    except QueryException:
        pass

    try:
        db.table("Dataset").insert([
            {"Name": "test_input_dataset",
             "Description": "Created by queries.add_filler_data"},
            {"Name": "test_output_dataset",
             "Description": "Created by queries.add_filler_data"}
        ])
    except QueryException:
        pass

    try:
        db.table("IotaDatasetJunction").insert([
            {"IotaId": 5,
             "DatasetId": 1},
            {"IotaId": 6,
             "DatasetId": 1},
            {"IotaId": 7,
             "DatasetId": 1},
            {"IotaId": 8,
             "DatasetId": 1},
            {"IotaId": 9,
             "DatasetId": 2}
        ])
    except QueryException:
        pass

    try:
        db.table("Run").insert([
            {"InputDatasetId": 1,
             "OutputDatasetId": 2,
             "AlgorithmId": 1,
             "UserId": 1,
             "Name": "Test Run",
             "Description": "Created by queries.add_filler_data",
             "Begin": datetime.now(),
             "End": datetime.now()}
        ])
    except QueryException:
        pass

def get_dataset(db, datasetname=None, sourceid=None, sourcetype=None):
    checks.check_types(db, DatabaseManager)
    checks.check_types(datasetname, [str, type(None)])
    checks.check_types(sourceid, [int, type(None)])
    checks.check_types(sourcetype, [str, type(None)])

    if all([datasetname, sourceid, sourcetype]) is None:
        raise ValueError("Must provide either a dataset name or a sourceid and \
        sourcetype")

    if datasetname is not None:
        ds = db.table("IotaDatasetJunction") \
               .join("Iota",
                    "IotaDatasetJunction.IotaId", "=", "Iota.IotaId") \
               .join("Dataset",
                    "IotaDatasetJunction.DatasetId", "=", "Dataset.DatasetId") \
               .join("SourceType",
                    "Iota.SourceTypeId", "=", "SourceType.SourceTypeId") \
               .where("Dataset.Name", "=", datasetname) \
               .get()

    df = pd.DataFrame(ds.all())

    # TODO:
    # write df to dict func
    # convert dict to new df

    print(df)
