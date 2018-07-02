# installed
from orator.exceptions.query import QueryException
from orator import DatabaseManager
from datetime import datetime
import pandas as pd
import pathlib
import os

# self
from ..utils import checks
from ..utils import handles
from ..query import *

def display_all_tables(database):
    """
    Display all the tables present in the provided database.

    Example
    ==========
    ```
    >>> db = orator.DatabaseManager(config)
    >>> display_all_tables(db)
    ...

    ```

    Parameters
    ==========
    database: orator.DatabaseManager
        A constructed orator DatabaseManager to retrieve, format and display
        tables from.

    Returns
    ==========

    Errors
    ==========

    """

    # check types
    checks.check_types(database, DatabaseManager)

    # get all database tables
    all_tables = get_database_schema_tables(database)
    print("All tables")
    print("-" * 40)
    print(all_tables)

    # find driver to navigate proper key
    driver = handles.get_database_driver(database)

    # use proper key to get tables
    if driver == "sqlite":
        all_tables = get_tables(database, all_tables["tbl_name"])
    else:
        all_tables = get_tables(database, all_tables["tablename"])

    # display all tables
    for table, df in all_tables.items():
        print("-" * 80)
        print(table, "| rows:", len(df))
        print("-" * 40)
        print(df.head())

def get_database_schema_tables(database):
    """
    Get all database tables info and schema defintions.

    Example
    ==========
    ```
    >>> db = orator.DatabaseManager(config)
    >>> get_database_tables(db)
    tbl_name | ... | ... |
    ...
    ...

    ```

    Parameters
    ==========
    database: orator.DatabaseManager
        Which database you would like to get schema tables from.

    Returns
    ==========
    schema_tables: pd.DataFrame
        A pandas DataFrame of the provided database's table information.

    Errors
    ==========

    """

    # check types
    checks.check_types(database, DatabaseManager)

    # find driver to navigate proper key
    driver = handles.get_database_driver(database)

    # use proper key to execute database tables query
    query = "SELECT * FROM "
    if driver == "sqlite":
        where = "sqlite_master WHERE type='table'"
        tables = database.select((query + "{w}").format(w=where))
        tables = [t for t in tables if t["name"] != "sqlite_sequence"]
    else:
        where = "pg_catalog.pg_tables WHERE schemaname='public'"
        tables = database.select((query + "{w}").format(w=where))
        tables = [dict(t) for t in tables]

    # return found
    return pd.DataFrame(tables)

def store_all_database_tables(database, storage="/database/backups/"):
    """
    Store all database tables as csv files.

    Example
    ==========
    ```
    >>> db = orator.DatabaseManager(config)
    >>> store_all_database_tables(db)
    Stored: /database/backups/2018-06-10 06:54:28.786915/User.csv
    ...
    Stored: /database/backups/2018-06-10 06:54:28.786915/Run.csv

    >> store_all_database_tables(db, "/path/to/storage/")
    Stored: /path/to/storage/2018-06-10 06:54:28.786915/User.csv
    ...
    Stored: /path/to/storage/2018-06-10 06:54:28.786915/Run.csv

    ```

    Parameters
    ==========
    database: orator.DatabaseManager
        Which database you would like to store all tables from.
    storage: str, pathlib.Path
        Where you would like to store all tables to.

    Returns
    ==========
    storage: pathlib.Path
        The true path of where the backup was stored.

    Errors
    ==========

    """

    # check types
    checks.check_types(database, DatabaseManager)
    checks.check_types(storage, [str, pathlib.Path])

    # convert and make storage
    storage = pathlib.Path(storage)
    storage /= str(datetime.now())
    os.makedirs(storage)

    # find driver to navigate proper key
    driver = handles.get_database_driver(database)

    # get all database tables
    if driver == "sqlite":
        tables = list(get_database_schema_tables(database)["tbl_name"])
    else:
        tables = list(get_database_schema_tables(database)["tablename"])

    tables = get_tables(database, tables)

    # store each table
    for name, table in tables.items():
        table_store = storage / (name + ".csv")
        table.to_csv(table_store, index=False)
        print("Stored:", table_store)

    # return true storage path
    return storage

def add_user(database, user):
    # check types
    checks.check_types(database, DatabaseManager)
    checks.check_types(user, str)

    try:
        database.table("User").insert({
            "Name": user,
            "Created": datetime.now()
        })
    except QueryException:
        pass
