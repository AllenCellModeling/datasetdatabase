#!/usr/bin/env python

# installed
import pathlib

# self
from .core import DatabaseConfig

# globals
LOCAL = {"LOCAL": {
            "driver": "sqlite",
            "database": pathlib.Path("./local.db")}
        }
LOCAL = DatabaseConfig(LOCAL)
