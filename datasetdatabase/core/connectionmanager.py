#!/usr/bin/env python

# installed
from typing import Union
import pathlib
import copy
import json
import os

# self
from .datasetdatabase import DatasetDatabase
from ..utils import checks
from ..utils import types

# globals
CONNECTION_ALREADY_EXISTS = """

A connection with that link already exists.
Found connection: {f}
"""

class ConnectionManager(object):
    """

    """

    def __init__(self,
                 configs: Union[str, dict, pathlib.Path, types.NONE] = None,
                 local_store: Union[str, pathlib.Path, types.NONE] = None):

        # enforce types
        checks.check_types(configs, [str, dict, pathlib.Path, types.NONE])
        checks.check_types(local_store, [str, pathlib.Path, types.NONE])

        # create connections
        self.connections = {}

        # handle connections passed
        if configs is not None:
            self.add_connections(configs, local_store=local_store)


    def add_connections(self,
                        config: Union[str, dict, pathlib.Path],
                        override_existing: bool = False,
                        local_store: Union[str, pathlib.Path, types.NONE] \
                            = None):
        """
        Add all connections present in a config JSON file. Or create a local
        database instance using string "local".

        Example
        ==========
        ```
        >>> mngr = ConnectionManager()
        >>> mngr.add_connections("local")

        >>> config = "/foo/bar/config.json"
        >>> mngr.add_connections(config)

        >>> config = {"prod": {...}}
        >>> mngr.add_connections(config)

        ```

        Parameters
        ==========
        config: str, dict, pathlib.Path
            OS string path, or pathlib.Path to a config file, or a custom
            dictionary object for database connections. Optionally, provide the
            string "local" to construct a local database in your current working directory.

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
        KeyError:
            A database link with that name already exists.

        """

        # enforce types
        checks.check_types(config, [str, dict, pathlib.Path])
        checks.check_types(override_existing, bool)
        checks.check_types(local_store, [str, pathlib.Path, types.NONE])

        # handle string
        if isinstance(config, str):
            # check for local
            if "local" == config:
                if not override_existing:
                    assert "local" not in self.connections, \
                        "A connection with that name already exists."

                if local_store is None:
                    storage = os.getcwd()
                    storage = pathlib.Path(storage)
                    storage /= "local_database"
                    storage /= "local.db"
                else:
                    storage = pathlib.Path(local_store)

                conn = {"local": {
                            "driver": "sqlite",
                            "database": storage}
                        }

                # add local
                self.connections["local"] = conn
                return

            else:
                # convert to path
                config = pathlib.Path(config)

        # handle path
        if isinstance(config, pathlib.Path):
            checks.check_file_exists(config)

            with open(config, "r") as read_in:
                config = json.load(config)

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
        >>> mngr = ConnectionManager("local")
        >>> mngr.rename_connection("local", "new_name")

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
            "Connection with that name does not exist."
        assert new not in self.connections, \
            "Connection with that name already exists."

        # copy config portion and rename
        config = copy.deepcopy(self.connections[current][current])
        del self.connections[current]
        self.connections[new] = {new: config}


    def backup(self):
        return


    def connect(self):


        return


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

        disp = disp[:-1]

        return disp


    def __repr__(self):
        return str(self)
