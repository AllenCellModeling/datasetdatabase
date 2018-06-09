# installed
from orator import DatabaseManager
from getpass import getuser
import pathlib
import os

# self
from ..utils import checks

def handle_local_connection():
    if "in_docker" in os.environ:
        local_lookup = pathlib.Path("/database")
    else:
        local_lookup = pathlib.Path("/Users")
        local_lookup /= getuser()
        local_lookup /= "Documents"
        local_lookup /= "dbconnect"

    return local_lookup

def get_db_driver(db):
    checks.check_types(db, DatabaseManager)

    return db._config[list(db._config.keys())[0]]["driver"]
