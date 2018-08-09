#!/usr/bin/env python

# installed
from typing import Union
import pathlib
import json

# self
from ..utils import checks

# globals
REQUIRED_CONFIG_ITEMS = ("driver", "database")
MISSING_REQUIRED_ITEMS = "Config must contain {i}".format(REQUIRED_CONFIG_ITEMS)
MALFORMED_LOCAL_LINK = "Local databases must have suffix '.db'"

class DatabaseConfig(object):
    def __init__(self,
                 config: Union[str, pathlib.Path, dict]):
        # enforce types
        checks.check_types(config, [str, pathlib.Path, dict])

        # convert types
        if isinstance(config, str):
            config = pathlib.Path(config)
        if isinstance(config, pathlib.Path):
            with open(config, "r") as read_in:
                config = json.load(read_in)

        # enforce minimum
        valid_config = all(key in REQUIRED_CONFIG_ITEMS for key in config)
        assert valid_config, MISSING_REQUIRED_ITEMS

        # import config
        for key, item in config.items():
            self.__setattr__(key, item)

        # convert database link and enforce exists
        if self.driver = "sqlite":
            self.database = pathlib.Path(self.database)
            assert t.suffix == ".db", MALFORMED_LOCAL_LINK
            if not t.exists():
                t.mkdir(parents=True)


    def __str__(self):
        return str(self.__dict__)


    def __repr__(self):
        return str(self)
