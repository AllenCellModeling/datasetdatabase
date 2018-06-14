# installed
from orator import DatabaseManager
from datetime import datetime
from getpass import getuser
import numpy as np
import pathlib
import os

# self
from ..utils import checks

def handle_local_connection():
    """
    Setup a pathlib Path to handle which local database to look for.
    (Naive and assumes local is stored in default locations)

    Example
    ==========
    ```
    >>> handle_local_connection()
    PosixPath('/Users/jacksonb/Documents/dbconnect')

    >>> os.environ["in_docker"] = True
    >>> handle_local_connection()
    PosixPath('/database')

    ```

    Parameters
    ==========

    Returns
    ==========
    local_lookup: pathlib.Path
        The naively determined database config, store, and backups lookup path.

    Errors
    ==========

    """

    # check if docker
    if "in_docker" in os.environ:
        return pathlib.Path("/database")

    # not docker
    local_lookup = pathlib.Path("/Users")
    local_lookup /= getuser()
    local_lookup /= "Documents"
    local_lookup /= "dbconnect"

    return local_lookup

def get_database_driver(database):
    """
    Get the driver of the passed DatabaseManager object.

    Example
    ==========
    ```
    >>> db = orator.DatabaseManager(sqlite_config)
    >>> get_db_driver(db)
    sqlite

    ```

    Parameters
    ==========
    database: orator.DatabaseManager
        Which database to find the driver for.

    Returns
    ==========
    driver: str
        The driver for the passed database.

    Errors
    ==========

    """

    # check types
    checks.check_types(database, DatabaseManager)

    # return the driver of the passed database config
    return database._config[list(database._config.keys())[0]]["driver"]

def cast(value, value_type, **kwargs):
    # check types
    checks.check_types(value, str)
    checks.check_types(value_type, str)

    # easy supported natives
    native_types = ["int", "float", "bool",
                    "list", "tuple", "dict", "set"]

    # prep the type string
    value_type = value_type.replace("<class '", "")[:-2]

    # cast
    if "numpy.ndarray" in value_type:
        value = value.replace("[", "")
        value = value.replace("]", "")
        result = np.fromstring(value, sep=" ", **kwargs)
    elif "str" in value_type:
        result = eval(value_type + "('{v}')".format(v=value))
    elif value_type in native_types:
        result = eval(value_type + "({v})".format(v=value))
    else:
        result = value

    return result
