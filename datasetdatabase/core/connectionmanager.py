#!/usr/bin/env python

# installed
from typing import Union
import pathlib
import json
import os

# self
from ..utils import checks
from ..utils import types

class ConnectionManager(object):
    """

    """

    def __init__(self):
        self.connections = {}


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

        Returns
        ==========

        Errors
        ==========
        KeyError:
            A database link with that name already exists.

        """

        # enforce types
        checks.check_types(config, [str, dict, pathlib.Path])
        checks.check_types(override_existing, bool)

        # handle string
        if isinstance(config, str):
            # check for local
            if config == "local":
                storage = os.getcwd()
                storage = pathlib.Path(storage)
                storage = storage / "local_database"
                os.makedirs(storage)

                conn = {"local": {
                            "driver": "sqlite",
                            "database": storage / "local.db"}
                        }

                # add local
                self.connections["local"] = conn

            else:
                # convert to path
                config = pathlib.Path(config)

        # handle path
        if isinstance(config, pathlib.Path):
            checks.check_file_exists(config)


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

    def __str__(self):
        return ""
