#!/usr/bin/env python

# installed
from typing import Union
import pathlib
import getpass
import types
import copy
import json
import os

# self
from .datasetdatabase import DatasetDatabase
from ..core import connections
from ..utils import checks

# globals
CONNECTION_ALREADY_EXISTS = """

A connection with that link already exists.
Found connection: {f}
"""


class ConnectionManager(object):
    """
    A connection manager is an object you will create before you connect to any
    dataset database. It will store, and manage the creation, backup,
    restoration, migration, and teardown of different databases in it's
    connection map.
    """


    def __init__(self,
                 config: Union[str, dict, pathlib.Path, None] = None,
                 user: Union[str, None] = None,
                 **kwargs):
        """
        Create a ConnectionManager.

        A connection manager is an object you will create before you connect to
        any dataset database. It will store, and manage the creation, backup,
        restoration, migration, and teardown of different databases in it's
        connection map.

        Example
        ==========
        ```
        >>> ConnectionManager(LOCAL)
        >>> ConnectionManager(LOCAL)
        local:
            driver: sqlite
            database: /{cwd}/local_database/local.db

        ```

        Parameters
        ==========
        config: str, dict, pathlib.Path, None
            OS string path, or pathlib.Path to a config file, or a custom
            dictionary object for database connections. Optionally, use
            dsdb.LOCAL to construct a local database in your current
            working directory.

            Default: None (Initialize an empty connections map)

        user: str, None
            Name of the user creating and interacting with database connections.

            Default: None (attempt to get user)

        **kwargs:
            All other keyword arguments to be passed to the
            ConnectionManager.add_connections() function.

        Returns
        ==========
        self

        Errors
        ==========

        """

        # enforce types
        checks.check_types(config, [str, dict, pathlib.Path, type(None)])
        checks.check_types(user, [str, type(None)])
        self.user = checks.check_user(user)

        # create connections
        self.connections = {}

        # handle connections passed
        if config is not None:
            self.add_connections(config, **kwargs)


    def add_connections(self,
                        config: Union[str, dict, pathlib.Path],
                        override_existing: bool = False,
                        local_store: Union[str, pathlib.Path, None] \
                            = None):
        """
        Add all connections present in a config JSON file. Or create a local
        database instance using dsdb.LOCAL.

        Example
        ==========
        ```
        >>> mngr = ConnectionManager()
        >>> mngr.add_connections(LOCAL)

        >>> config = "/foo/bar/config.json"
        >>> mngr.add_connections(config)

        >>> config = {"prod": {...}}
        >>> mngr.add_connections(config)

        ```

        Parameters
        ==========
        config: str, dict, pathlib.Path
            OS string path, or pathlib.Path to a config file, or a custom
            dictionary object for database connections. Optionally, use
            dsdb.LOCAL to construct a local database in your current
            working directory.

        override_existing: bool
            On the chance that a database already exists in the connections map
            with the same name of one trying to be added, the current database
            link will be overriden.

            Default: False, (Do not override)

        local_store: str, pathlib.Path, None
            Provide a custom storage path for the local dataset database
            instance to use.

        Returns
        ==========

        Errors
        ==========
        AssertionError:
            A database connection, name, or link either was not provided
            properly or already exists.

        """

        # enforce types
        checks.check_types(config, [str, dict, pathlib.Path])
        checks.check_types(override_existing, bool)
        checks.check_types(local_store, [str, pathlib.Path, type(None)])

        # handle string
        if isinstance(config, str):
            # check for local
            if config == connections.LOCAL:
                if not override_existing:
                    assert config not in self.connections, \
                        "A connection with that name already exists."

                if local_store is None:
                    storage = os.getcwd()
                    storage = pathlib.Path(storage)
                    storage /= "local_database"
                    storage /= "local.db"
                else:
                    storage = pathlib.Path(local_store)

                # ensure the folders exist
                try:
                    os.makedirs(storage.parent)
                except FileExistsError:
                    pass

                conn = {config: {
                            "driver": "sqlite",
                            "database": str(storage)}
                        }

                # add local
                self.connections[config] = conn
                return

            else:
                # convert to path
                config = pathlib.Path(config)

        # handle path
        if isinstance(config, pathlib.Path):
            checks.check_file_exists(config)

            with open(config, "r") as read_in:
                config = json.load(read_in)

        # check config
        for name, cnfg in config.items():
            assert "driver" in cnfg, "Connection requires a driver."
            assert "database" in cnfg, "Connection requires a link."
            checks.check_types(cnfg["driver"],
                               str,
                               "Database driver must be a string")
            checks.check_types(cnfg["database"],
                               [str, pathlib.Path],
                               "Database link must be a string")

            #
            if not override_existing:
                for n, c in self.connections.items():
                    assert name != n, \
                        "A connection with that name already exists."
                    if isinstance(c[n]["database"], str):
                        assert c[n]["database"] != str(cnfg["database"]), \
                            CONNECTION_ALREADY_EXISTS.format(f=c)
                    elif isinstance(c[n]["database"], pathlib.Path):
                        assert c[n]["database"] != \
                            pathlib.Path(cnfg["database"]), \
                            CONNECTION_ALREADY_EXISTS.format(f=c)

        # add config
        for name, cnfg in config.items():
            self.connections[name] = {name: cnfg}


    def rename_connection(self, current: str, new: str):
        """
        Rename a dataset database connection.

        Example
        ==========
        ```
        >>> mngr = ConnectionManager(dsdb.LOCAL)
        >>> mngr.rename_connection(dsdb.LOCAL, "new_name")

        ```

        Parameters
        ==========
        current: str
            The current name of the connection you would like to rename.

        new: str
            The new name of the connection you would like to rename.

        Returns
        ==========

        Errors
        ==========
        AssertionError:
            A connection with the name provided in parameter "current", does not
            exist.

            A connection with the name provided in parameter "new", already
            exists.

        """

        # enforce types
        checks.check_types(current, str)
        checks.check_types(new, str)

        # enforce connection names
        assert current in self.connections, \
            "A connection with that name does not exist."
        assert new not in self.connections, \
            "A connection with that name already exists."

        # copy config portion and rename
        config = copy.deepcopy(self.connections[current][current])
        self.connections.pop(current)
        self.connections[new] = {new: config}


    def backup(self):
        return


    def connect(self, name: str, **kwargs) -> DatasetDatabase:
        """
        Connect to a dataset database.

        Example
        ==========
        ```
        >>> mngr = ConnectionManager(dsdb.LOCAL)
        >>> mngr.connect(dsdb.LOCAL)
        <datasetdatabase.core.datasetdatabase.DatasetDatabase at 01x345678>

        >>> mngr.connect("missing")
        AssertionError: "A connection with that name does not exist."

        ```

        Parameters
        ==========
        name: str
            Which dataset database to connect to.

        **kwargs:
            All other keyword arguments get passed to DatasetDatabase
            initialization.

        Returns
        ==========
        db: datasetdatabase.core.datasetdatabase.DatasetDatabase
            The initialized DatasetDatabase from the connection desired.

        Errors
        ==========
        AssertionError:
            A connection with that name does not exist.

        """

        # enforce types
        checks.check_types(name, str)

        # key must exist
        assert name in self.connections, \
        "A connection with that name does not exist."

        # return a dataset database object
        return DatasetDatabase(self.connections[name], user=self.user, **kwargs)


    def migrate(self):
        return


    def restore(self):
        return


    def teardown(self):
        return


    def __getitem__(self, key):
        return self.connections[key]


    def __str__(self):
        disp = ""

        for name, config in self.connections.items():
            disp += "{n}:\n\tdriver: {d}\n\tlink: {l}\n".format(n=name,
                                                  d=config[name]["driver"],
                                                  l=config[name]["database"])

        return disp[:-1]


    def __repr__(self):
        return str(self)
