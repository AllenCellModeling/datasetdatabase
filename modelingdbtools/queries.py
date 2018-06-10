# installed
from orator.exceptions.query import QueryException
from orator import DatabaseManager
from datetime import datetime
import pandas as pd
import pathlib
import os

# self
from .utils import checks
from .utils import handles

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

def get_tables(database, get):
    """
    Get tables from the provided database.

    Example
    ==========
    ```
    >>> db = orator.DatabaseManager(config)
    >>> get_tables(db, "User")
    {"User": pd.DataFrame}

    >>> get_tables(db, ["User", "Run"])
    {"User": pd.DataFrame, "Run": pd.DataFrame}

    >>> get_tables(db, pd.Series("User", "Run"))
    {"User": pd.DataFrame, "Run": pd.DataFrame}

    ```

    Parameters
    ==========
    database: orator.DatabaseManager
        Which database you would like to get tables from.
    get: str, list, pd.Series
        Which table(s) you would like to retrieve.

    Returns
    ==========
    tables: dict
        A dictionary of table names as keys and pd.DataFrames as values.

    Errors
    ==========

    """

    # check types
    checks.check_types(database, DatabaseManager)
    checks.check_types(get, [str, list, pd.Series])

    # convert to list
    get = list(get)

    # get all tables and label them in dict
    tables = {}
    for table in get:
        rows = database.select("SELECT * FROM {t}".format(t=table))
        tables[table] = pd.DataFrame(rows)

    # return tables
    return tables

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

    # get all database tables
    tables = list(get_database_schema_tables(database)["tbl_name"])
    tables = get_tables(database, tables)

    # store each table
    for name, table in tables.items():
        table_store = storage / (name + ".csv")
        table.to_csv(table_store, index=False)
        print("Stored:", table_store)

    # return true storage path
    return storage

def get_dataset(database,
                datasetname=None,
                sourceid=None,
                sourcetype=None,
                get_info_items=False):
    """
    Get, format and return a dataset using either a dataset name or
    a source id and source type.

    Example
    ==========
    ```
    >>> db = orator.DatabaseManager(config)
    >>> get_dataset(db, "test_input_dataset")
    pd.DataFrame ...

    >>> get_dataset(db, "test_input_dataset", get_info_items=True)
    pd.DataFrame ...

    >>> get_dataset(db, sourceid=1, sourcetype="File")
    pd.DataFrame ...

    ```

    Parameters
    ==========
    database: orator.DatabaseManager
        Which database you would like to use to retrive the dataset from.
    datasetname: str, None
        Which dataset you want to retrieve.
    sourceid: int, None
        Which dataset you want to retrieve based off of the source id and type.
    sourcetype: str, None
        Which dataset you want to retrieve based off of the source id and type.
    get_info_items: bool
        Should the returned pandas DataFrame contain columns indicating each
        found key value pairings IotaId, SourceId, and SourceTypeId.

    Returns
    ==========
    dataset: pd.DataFrame
        The found and formatted dataset created by merging Iota by their
        specified GroupIds.

    Errors
    ==========

    """

    # check types
    checks.check_types(database, DatabaseManager)
    checks.check_types(datasetname, [str, type(None)])
    checks.check_types(sourceid, [int, type(None)])
    checks.check_types(sourcetype, [str, type(None)])
    checks.check_types(get_info_items, bool)

    # must provide either a datasetname or a sourceid and sourcetype
    if all([datasetname, sourceid, sourcetype]) is None:
        raise ValueError("Must provide either a dataset name or a sourceid and \
        sourcetype")

    # TODO:
    # handle sourceid and sourcetype joining

    # handle datasetname provided
    if datasetname is not None:
        ds = database.table("IotaDatasetJunction") \
               .join("Iota",
                    "IotaDatasetJunction.IotaId", "=", "Iota.IotaId") \
               .join("Dataset",
                    "IotaDatasetJunction.DatasetId", "=", "Dataset.DatasetId") \
               .join("SourceType",
                    "Iota.SourceTypeId", "=", "SourceType.SourceTypeId") \
               .where("Dataset.Name", "=", datasetname) \
               .get()

    # return the formatted dataset
    return convert_dataset_to_dataframe(ps.DataFrame(ds.all()), get_info_items)

def convert_dataset_to_dataframe(dataset, get_info_items=False):
    """
    Convert a dataframe of joined Iota, Dataset, and IotaDatasetJunction values
    into a formatted standard dataset dataframe.

    Example
    ==========
    ```
    >>> db = orator.DatabaseManager(config)
    >>> unformatted = db.table("IotaDatasetJunction") \
            .join("Iota",
                 "IotaDatasetJunction.IotaId", "=", "Iota.IotaId") \
            .join("Dataset",
                 "IotaDatasetJunction.DatasetId", "=", "Dataset.DatasetId") \
            .join("SourceType",
                 "Iota.SourceTypeId", "=", "SourceType.SourceTypeId") \
            .where("Dataset.Name", "=", datasetname) \
            .get()
    >>> convert_dataset_to_dataframe(unformatted)
    pd.DataFrame ...

    >> convert_dataset_to_dataframe(unformatted, True)
    pd.DataFrame ..

    ```

    Parameters
    ==========
    dataset: pd.DataFrame
        The unformatted dataframe created from the Iota, Dataset,
        IotaDatasetJunction join.
    get_info_items: bool
        Should the returned pandas DataFrame contain columns indicating each
        found key value pairings IotaId, SourceId, and SourceTypeId.

    Returns
    ==========
    dataset: pd.DataFrame
        The formatted and merged dataset merged based off of Iota "GroupId"
        values.

    Errors
    ==========

    """

    # check types
    checks.check_types(dataset, pd.DataFrame)
    checks.check_types(get_info_items, bool)

    # join each Iota row into a dataframe row
    rows = {}
    for i, row in dataset.iterrows():
        items = dict(row)

        # basic items
        group = {items["Key"]: items["Value"],
                 (items["Key"] + "(Parser)"): items["Parser"]}

        # info items
        if get_info_items:
            group[(items["Key"] + "(IotaId)")] = items["IotaId"]
            group[(items["Key"] + "(SourceId)")] = items["SourceId"]
            group[(items["Key"] + "(SourceTypeId)")] = items["SourceTypeId"]

        # first item of row
        if items["GroupId"] not in rows:
            rows[items["GroupId"]] = group

        # additional items of already created row
        else:
            for key, item in group.items():
                rows[items["GroupId"]][key] = item

    # return dataframe
    return pd.DataFrame(list(rows.values()))
