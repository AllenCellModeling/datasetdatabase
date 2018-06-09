# installed
from orator import DatabaseManager
import pandas as pd

# self
from .utils import checks
from .utils import handles

def display_all_tables(db):
    checks.check_types(db, DatabaseManager)

    all_tables = get_db_tables(db)
    print(all_tables)

def get_db_tables(db):
    checks.check_types(db, DatabaseManager)

    driver = handles.get_db_driver(db)

    if driver == "sqlite":
        w = "WHERE type='table'"
        tables = db.select("SELECT * FROM sqlite_master {w}".format(w=w))
        tables = [t for t in tables if t["name"] != "sqlite_sequence"]
    else:
        w = "WHERE schemaname='public'"
        tables = db.select("SELECT * FROM pg_catalog.pg_tables {w}".format(w=w))
        tables = [dict(t) for t in tables]

    tables = pd.DataFrame(tables)

    return tables

def get_tables(db, get):
    checks.check_types(db, DatabaseManager)
    checks.check_types(get, [str, list])

    if isinstance(get, str):
        get = [get]

    tables = {}
    for table in get:
        t = db.select("SELECT * FROM {t}".format(t=table))
        print(dict(t))
