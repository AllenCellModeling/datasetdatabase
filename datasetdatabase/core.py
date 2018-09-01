#!/usr/bin/env python

# installed
from typing import Union, Dict, List
from datetime import datetime
import pandas as pd
import pathlib
import hashlib
import orator
import pickle
import types
import json

# self
from .introspect import Introspector, INTROSPECTOR_MAP, ObjectIntrospector
from .schema import FMSInterface
from .schema import SchemaVersion
from .utils import checks, tools

from .version import VERSION

# globals
REQUIRED_CONFIG_ITEMS = ("driver", "database")
MISSING_REQUIRED_ITEMS = "Config must have {i}.".format(i=REQUIRED_CONFIG_ITEMS)
MALFORMED_LOCAL_LINK = "Local databases must have suffix '.db'"
TOO_MANY_CONFIGS = "Config must be only for a single database link."

FILEPATH_COL_SEP = ","
INVALID_DS_INFO = "This set of attributes could not be found in the linked db."

UNIX_REPLACE = {"\\": "/"}
GENERIC_TYPES = Union[bytes, str, int, float, None, datetime]
DATASET_EXTENSION = ".dataset"
MISSING_INIT = "Must provide either a pandas DataFrame or a DatasetInfo object."
EDITING_STORED_DATASET = "Datasets connected to DatasetInfo are immutable."
TOO_MANY_RETURN_VALUES = "Too many values returned from query expecting {n}."

EXTENSION_MAP = {".csv": pd.read_csv,
                 ".dataset": tools.read_pickle}
UNKNOWN_EXTENSION = "Unsure how to read dataset from the passed path.\n\t{p}"


class DatabaseConfig(object):
    def __init__(self,
        config: Union[str, pathlib.Path, Dict[str, str]],
        name: Union[str, None] = None):
        # enforce types
        checks.check_types(name, [str, type(None)])
        checks.check_types(config, [str, pathlib.Path, dict])

        # convert types
        if isinstance(config, str):
            config = pathlib.Path(config)
        if isinstance(config, pathlib.Path):
            with open(config, "r") as read_in:
                config = json.load(read_in)

        # enforce config minimum attributes
        valid_config = all(k in config for k in REQUIRED_CONFIG_ITEMS)
        assert valid_config, MISSING_REQUIRED_ITEMS

        # passed enforcement
        self._config = config

        # assign name
        if name is None:
                self.name = pathlib.Path(self.config["database"])\
                            .with_suffix("")\
                            .name


    def __iter__(self):
        yield self.name, self.config


    @property
    def config(self):
        return self._config


    def __str__(self):
        return str(dict(self))


    def __repr__(self):
        return str(self)


class DatabaseConstructor(object):
    def __init__(self,
        config: Union[DatabaseConfig, None],
        schema: Union[SchemaVersion, None] = None,
        fms: Union[FMSInterface, None] = None):

        # enforce types
        checks.check_types(config, [DatabaseConfig, type(None)])
        checks.check_types(schema, [SchemaVersion, type(None)])
        checks.check_types(fms, [FMSInterface, type(None)])

        if config is None:
            config = LOCAL

        # store attributes
        self._config = config
        self._schema = schema
        self._fms = fms
        self._tables = []
        self._db = orator.DatabaseManager(dict(self.config))


    @property
    def config(self):
        return self._config


    @property
    def schema(self):
        # None passed, load default
        if self._schema is None:
            from .schema.minimal import MINIMAL
            self._schema = MINIMAL

        return self._schema


    @property
    def fms(self):
        # None passed, load default
        if self._fms is None:
            from .schema.filemanagers.quiltfms import QuiltFMS
            self._fms = QuiltFMS()

        return self._fms


    @property
    def tables(self):
        return self._tables


    @property
    def db(self):
        return self._db

    @property
    def orator_schema(self):
        return self._orator_schema


    def prepare_connection(self):
        # convert database link and enforce exists
        if self.config.config["driver"] == "sqlite":
            link = self.config.config["database"]
            link = pathlib.Path(link)
            assert link.suffix == ".db", MALFORMED_LOCAL_LINK
            if not link.exists():
                link.parent.mkdir(parents=True, exist_ok=True)


    def create_schema(self):
        # create all tables in version
        for tbl, func in self.schema.tables.items():
            func(self.orator_schema)
            if tbl not in self.tables:
                self._tables.append(tbl)

        # create file table from fms module
        if self.fms.table_name not in self.tables:
            self.fms.create_File(self.orator_schema)
            self._tables.append(self.fms.table_name)


    def build(self):
        # run preparation steps
        self.prepare_connection()

        # connect
        self._orator_schema = orator.Schema(self.db)

        # create schema
        self.create_schema()

        return self.db


    def drop_schema(self):
        # get drop order
        drop_order = list(self.schema.tables)
        drop_order.reverse()

        # drop
        for tbl in drop_order:
            self.schema.drop_if_exists()


    def get_tables(self):
        # run preparation steps
        self.prepare_connection()

        if self.config.config["driver"] == "sqlite":
            names = self.db.table("sqlite_master") \
                           .select("name") \
                           .where("type", "=", "table").get()
            names = [t["name"] for t in names if t["name"] != "sqlite_sequence"]
        else:
            names = self.db.table("pg_catalog.pg_tables") \
                           .select("tablename") \
                           .where("schemaname", "=", "public").get()

            names = [t.tablename for t in names]

        if "migrations" in names:
            names.remove("migrations")

        self._tables = names
        return self.db


class DatasetDatabase(object):
    def __init__(self,
        config: Union[DatabaseConfig, str, pathlib.Path, None] = None,
        user: Union[str, None] = None,
        constructor: Union[DatabaseConstructor, None] = None,
        build: bool = False,
        recent_size: int = 5):

        # enforce types
        checks.check_types(config,
                           [DatabaseConfig, str, pathlib.Path, type(None)])
        checks.check_types(user, [str, type(None)])
        checks.check_types(constructor, [DatabaseConstructor, type(None)])
        checks.check_types(recent_size, int)

        # assume local
        if config is None:
            config = LOCAL

        # read config
        if isinstance(config, (str, pathlib.Path)):
            config = DatabaseConfig(config)

        # state basic items
        self._config = config
        self._user = checks.check_user(user)
        self.recent_size = recent_size

        # create constructor
        if constructor is None:
            self._constructor = DatabaseConstructor(self.config)

        # connect
        if build:
            self._db = self.constructor.build()
        else:
            self._db = self.constructor.get_tables()

        # upload basic items
        self._user_info = self._get_or_insert_user(self.user)


    @property
    def config(self):
        return self._config


    @property
    def user(self):
        return self._user


    @property
    def constructor(self):
        return self._constructor


    @property
    def db(self):
        return self._db


    def _get_or_insert_user(self,
        user: Union[str, None] = None,
        description: Union[str, None] = None) -> Dict[str, GENERIC_TYPES]:

        # enforce types
        checks.check_types(user, [str, type(None)])
        checks.check_types(description, [str, type(None)])

        # get default
        if user is None:
            user = self._user

        # check the os user
        user = checks.check_user(user)
        # if valid set user
        self._user = user

        # attempt get
        user_info = self.get_items_from_table("User", ["Name", "=", user])

        # not found
        if len(user_info) == 0:
            return self._insert_to_table("User",
                {"Name": user,
                 "Description": description,
                 "Created": datetime.now()})
        # found
        if len(user_info) == 1:
            return user_info[0]
        # database structure error
        raise ValueError(TOO_MANY_RETURN_VALUES.format(n=1))


    def upload_dataset(self,
        dataset: Union["Dataset", str, pathlib.Path, pd.DataFrame],
        **kwargs) -> "DatasetInfo":

        # enforce types
        checks.check_types(dataset, [Dataset, str, pathlib.Path, pd.DataFrame])

        # convert dataset
        if not isinstance(dataset, Dataset):
            dataset = Dataset(dataset, **kwargs)

        return


    def get_dataset(self, id=0):
        return pd.DataFrame()


    def get_items_from_table(self,
        table: str,
        conditions: List[Union[List[GENERIC_TYPES], GENERIC_TYPES]] = []) \
        -> List[Dict[str, GENERIC_TYPES]]:

        # enforce types
        checks.check_types(table, str)
        checks.check_types(conditions, list)

        # construct orator table
        table = self.db.table(table)

        # expand multiple conditions
        if all(isinstance(cond, list) for cond in conditions):
            for cond in conditions:
                table = table.where(*cond)
        # expand single condition
        else:
            table = table.where(*conditions)

        # get table
        table = table.get()

        # format
        return [dict(item) for item in table]


    def _insert_to_table(self,
        table: str,
        items: Dict[str, GENERIC_TYPES]) -> Dict[str, GENERIC_TYPES]:

        # enforce types
        checks.check_types(table, str)
        checks.check_types(items, dict)

        # create conditions
        conditions = [[k, "=", v] for k, v in items.items()]

        # check exists
        found_items = self.get_items_from_table(table, conditions)

        # not found
        if len(found_items) == 0:
            id = self.db.table(table)\
                .insert_get_id(items, sequence=(table + "Id"))
            id_condition = [table + "Id", "=", id]
            return self.get_items_from_table(table, id_condition)[0]

        # found
        if len(found_items) == 1:
            return found_items[0]

        # database structure error
        raise ValueError(TOO_MANY_RETURN_VALUES.format(n=1))


    @property
    def recent(self):
        print(("-" * 31) + " DATASET DATABASE " + ("-" * 31))

        for name in self.constructor.tables:
            print("-" * 80)
            print("Recent " + name + ":")
            try:
                table = self.db.table(name)
                if name == "Run":
                    recent = table.order_by("End", "desc")\
                                  .limit(self.recent_size)\
                                  .get()
                else:
                    recent = table.order_by("Created", "desc")\
                                  .limit(self.recent_size)\
                                  .get()

                for item in recent:
                    print(dict(item))

            except (orator.exceptions.query.QueryException, KeyError) as e:
                print("!package or schema not up to date!")


    def __str__(self):
        # basic print
        disp = "Recent Datasets:"
        disp += ("\n" + ("-" * 80))

        # collect datasets
        recent = self.db.table("Dataset")\
                     .order_by("Created", "desc")\
                     .limit(self.recent_size)\
                     .get()

        # format print
        for item in recent:
            disp += ("\n" + str(dict(item)))

        return disp


    def __repr__(self):
        return str(self)


class DatasetInfo(object):
    def __init__(self,
        DatasetId: int,
        Name: str,
        SourceId: int,
        Created: datetime,
        OriginDb: DatasetDatabase,
        Description: Union[str, None] = None,
        FilepathColumns: Union[str, None] = None):

        # enforce types
        checks.check_types(DatasetId, int)
        checks.check_types(Name, str)
        checks.check_types(Description, [str, type(None)])
        checks.check_types(FilepathColumns, [str, type(None)])
        checks.check_types(SourceId, int)
        checks.check_types(Created, datetime)
        checks.check_types(OriginDb, DatasetDatabase)

        self._original_description = Description
        self._original_fp_cols = FilepathColumns

        # convert types
        if Description is None:
            Description = ""
        if FilepathColumns is None:
            FilepathColumns = []
        if isinstance(FilepathColumns, str):
            FilepathColumns = FilepathColumns.split(FILEPATH_COL_SEP)

        # set attributes
        self._id = DatasetId
        self._name = Name
        self._description = Description
        self._fp_cols = FilepathColumns
        self._source_id = SourceId
        self._created = Created
        self._origin = OriginDb

        # validate attributes
        self._validate_info()


    @property
    def id(self):
        return self._id


    @property
    def name(self):
        return self._name


    @property
    def description(self):
        return self._description


    @property
    def filepath_columns(self):
        return self._fp_cols


    @property
    def source_id(self):
        return self._source_id


    @property
    def created(self):
        return self._created


    @property
    def origin(self):
        return self._origin


    def _validate_info(self):
        found = self.origin.get_items_from_table("Dataset", [
                ["DatasetId", "=", self.id],
                ["Name", "=", self.name],
                ["Description", "=", self._original_description],
                ["FilepathColumns", "=", self._original_fp_cols],
                ["SourceId", "=", self.sourceid],
                ["Created", "=", self.created]
        ])

        assert len(found) == 1, INVALID_DS_INFO


    def __str__(self):
        return str({"id": self.id,
                    "name": self.name,
                    "description": self.description,
                    "filepath_columns": self.filepath_columns,
                    "source_id": self.source_id,
                    "created": self.created})


    def __repr__(self):
        return str(self)


class Dataset(object):
    def __init__(self,
        dataset: object = None,
        ds_info: Union[DatasetInfo, None] = None,
        name: Union[str, None] = None,
        description: Union[str, None] = None,
        introspector: Union[Introspector, None] = None):

        # enforce types
        checks.check_types(dataset, object)
        checks.check_types(ds_info, [DatasetInfo, type(None)])
        checks.check_types(name, [str, type(None)])
        checks.check_types(description, [str, type(None)])
        checks.check_types(introspector, [Introspector, type(None)])

        # must provide dataset or ds_info
        assert dataset is not None or ds_info is not None, MISSING_INIT

        # read dataset
        if isinstance(dataset, (str, pathlib.Path)):
            return read_dataset(dataset)

        # core properties
        self._ds = dataset
        self._info = ds_info
        self._md5 = tools.get_object_hash(self.ds)
        self._sha256 = tools.get_object_hash(self.ds, hashlib.sha256)

        # unpack based on info
        # name
        if self.info is None:
            self.name = name
        else:
            self.name = self.info.name

        # description
        if self.info is None:
            self.description = description
        else:
            self.description = self.info.description

        # introspector
        if self.info is None:
            t_ds = type(self.ds)
            if introspector is None:
                if t_ds in INTROSPECTOR_MAP:
                    self._introspector = INTROSPECTOR_MAP[t_ds](self.ds)
                else:
                    self._introspector = ObjectIntrospector(self.ds)
            else:
                self._introspector = introspector
        else:
            self._introspector = self.info.introspector


    @property
    def info(self):
        return self._info


    @property
    def ds(self):
        # get dataset if not in memory
        if self._ds is None:
            return self.info.origin.get_dataset(self.info.id)

        return self._ds


    @property
    def introspector(self):
        return self._introspector


    @property
    def md5(self):
        return self._md5


    @property
    def sha256(self):
        return self._sha256


    @property
    def state(self):
        ds_ready = self.ds is not None
        state = {"info": self.info,
                 "ds": ds_ready,
                 "name": self.name,
                 "description": self.description,
                 "introspector": type(self.introspector),
                 "validated": self.introspector.validated,
                 "md5": self.md5,
                 "sha256": self.sha256}

        return state


    def upload_to(self, database: DatasetDatabase) -> DatasetInfo:
        # enforce types
        checks.check_types(database, DatasetDatabase)

        # run upload
        self.info = database.upload_dataset(self)


    def apply(self,
        function: Union[types.ModuleType, types.FunctionType]):

        # TODO:
        # roundtrip if no ds info attached
        # process
        # new dataset

        return


    def save(self, path: Union[str, pathlib.Path]) -> pathlib.Path:
        # enforce types
        checks.check_types(path, [str, pathlib.Path])

        # convert types
        path = pathlib.Path(path)

        # dump dataset
        path = path.with_suffix(".dataset")
        to_save = Dataset(self.ds,
                          self.info,
                          self.name,
                          self.description,
                          self.introspector)
        tools.write_pickle(to_save, path)

        return path


    def __str__(self):
        return str(self.state)


    def __repr__(self):
        state_str = ""
        for key, value in self.state.items():
            state_str += key
            state_str += ": "
            state_str += str(value)
            state_str += "\n"

        return state_str


def read_dataset(path: Union[str, pathlib.Path]):
    # enforce types
    checks.check_types(path, [str, pathlib.Path])

    # convert types
    path = pathlib.Path(path)

    # attempt read
    try:
        if path.suffix in EXTENSION_MAP:
            ds = EXTENSION_MAP[path.suffix](path)
        else:
            ds = tools.read_pickle(path)
    except pickle.UnpicklingError:
        UNKNOWN_EXTENSION.format(p=path)

    # return
    return Dataset(ds)


# delayed globals
LOCAL = {"driver": "sqlite",
         "database": str(pathlib.Path("./local.db").resolve())}
LOCAL = DatabaseConfig(LOCAL)
