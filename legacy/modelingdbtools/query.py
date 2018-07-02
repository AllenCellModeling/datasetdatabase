# installed
from orator.exceptions.query import QueryException
from orator import DatabaseManager
import pandas as pd
import numpy as np

# self
from .utils import checks
from .utils import handles

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

    # TODO:
    # handle get only first n rows, etc using param and string format

    # check types
    checks.check_types(database, DatabaseManager)
    checks.check_types(get, [str, list, pd.Series])

    # convert to list
    get = list(get)

    # get database driver
    driver = handles.get_database_driver(database)

    # get all tables and label them in dict
    tables = {}
    for table in get:

        # handle tables with quotes for upper case names
        query = """SELECT * FROM "{t}" """
        rows = database.select(query.format(t=table))

        # handle different return
        if driver == "sqlite":
            tables[table] = pd.DataFrame(rows)
        else:
            tables[table] = pd.DataFrame([i.copy() for i in rows])

    # return tables
    return tables

def get_dataset(database,
                id=None,
                name=None,
                sourceid=None,
                sourcetype=None,
                columns=None,
                get_info_items=False):
    """
    Get, format and return a dataset using either a dataset name or
    a source id and source type.

    Example
    ==========
    ```
    >>> db = orator.DatabaseManager(config)
    >>> get_dataset(db, name="test_input_dataset")
    pd.DataFrame ...

    >>> get_dataset(db, name="test_input_dataset", get_info_items=True)
    pd.DataFrame ...

    >>> get_dataset(db, sourceid=1, sourcetype="File")
    pd.DataFrame ...

    ```

    Parameters
    ==========
    database: orator.DatabaseManager
        Which database you would like to use to retrive the dataset from.
    id: int, numpy.int64, None
        Which dataset you want to retrieve.
    name: str, None
        Which dataset you want to retrieve.
    sourceid: int, numpy.int64, None
        Which dataset you want to retrieve based off of the source id and type.
    sourcetype: str, None
        Which dataset you want to retrieve based off of the source id and type.
    columns: str, list, None
        Which columns (keys) you want to retrieve from the dataset.
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
    checks.check_types(id, [int, np.int64, type(None)])
    checks.check_types(name, [str, type(None)])
    checks.check_types(sourceid, [int, np.int64, type(None)])
    checks.check_types(sourcetype, [str, type(None)])
    checks.check_types(columns, [str, list, type(None)])
    checks.check_types(get_info_items, bool)

    # must provide either a name or a sourceid and sourcetype
    if all([id, name, sourceid, sourcetype]) is None:
        raise ValueError("Must provide either a dataset id or name, or a \
        sourceid and sourcetype")

    # get merge contents
    ds = database.table("Iota")

    # handle column filtering
    if isinstance(columns, str):
        columns = [columns]

    if isinstance(columns, list):
        or_where = database.query()
        for c in columns:
            or_where = or_where.where('Key', '=', c)

        ds = ds.or_where(or_where)

    ds = ds.join("IotaDatasetJunction",
                 "Iota.IotaId", "=", "IotaDatasetJunction.IotaId")
    ds = ds.join("Dataset",
                 "IotaDatasetJunction.DatasetId", "=", "Dataset.DatasetId")
    ds = ds.join("SourceType",
                 "Iota.SourceTypeId", "=", "SourceType.SourceTypeId")

    # handle id provided
    if id is not None:
        ds = ds.where("Dataset.DatasetId", "=", int(id))

    # handle name provided
    elif name is not None:
        ds = ds.where("Dataset.Name", "=", name)

    # handle sourceid and type
    else:
        ds = ds.where("Iota.SourceId", "=", int(sourceid))
        ds = ds.where("SourceType.SourceType", "=", sourcetype)

    # get
    ds = ds.get()

    try:
        ds = convert_dataset_to_dataframe(pd.DataFrame(ds.all()), get_info_items)
    except:
        pass
    # return the formatted dataset
    #return convert_dataset_to_dataframe(pd.DataFrame(ds.all()), get_info_items)
    return ds

def convert_dataset_to_dataframe(dataset, get_info_items=False, **kwargs):
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
            .where("Dataset.Name", "=", "test_dataset") \
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
        group = {items["Key"]: handles.cast(items["Value"],
                                            items["ValueType"],
                                            **kwargs)}

        # info items
        if get_info_items:
            group[(items["Key"] + "(Type)")] = items["ValueType"]
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

    # format dataframe
    rows = list(rows.values())
    rshp = "(Reshape)"
    reshape_cols = [c.replace(rshp, "") for c in rows[0].keys() if rshp in c]

    if len(reshape_cols) > 0:
        for row in rows:
            for key, val in row.items():
                if key in reshape_cols:
                    row[key] = np.reshape(val, row[key + rshp])

    reshape_cols = [c + rshp for c in reshape_cols]

    # return formatted
    return pd.DataFrame(rows).drop(columns=reshape_cols)

def get_items_in_table(database, table, items={}):
    # check types
    checks.check_types(database, DatabaseManager)
    checks.check_types(table, str)
    checks.check_types(items, dict)

    # filter down by items
    table = database.table(table)
    for key, val in items.items():
        table = table.where(key, "=", val)

    # return get
    return table.get()
