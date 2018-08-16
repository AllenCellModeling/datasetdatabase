#!/usr/bin/env python

# installed
from typing import Union, Dict, List
from datetime import datetime
import pandas as pd
import pathlib
import orator
import pickle
import types
import json
import abc

# self
from .schema import FMSInterface
from .schema import SchemaVersion
from .utils import checks

from .version import VERSION

# globals
REQUIRED_CONFIG_ITEMS = ("driver", "database")
MISSING_REQUIRED_ITEMS = "Config must have {i}.".format(i=REQUIRED_CONFIG_ITEMS)
MALFORMED_LOCAL_LINK = "Local databases must have suffix '.db'"
TOO_MANY_CONFIGS = "Config must be only for a single database link."

FILEPATH_COL_SEP = ","
INVALID_DS_INFO = "This set of attributes could not be found in the linked db."

UNIX_REPLACE = {"\\": "/"}
DATASET_EXTENSION = ".dataset"
MISSING_INIT = "Must provide either a pandas DataFrame or a DatasetInfo object."
EDITING_STORED_DATASET = "Datasets connected to DatasetInfo are immutable."

APPROVED_EXTENSIONS = (".csv", ".tsv", ".dataset")
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
        for tbl, func in self.version.tables.items():
            func(self.schema)
            if tbl not in self.tables:
                self._tables.append(tbl)

        # create file table from fms module
        if self.fms.table_name not in self.tables:
            self.fms.create_File(self.schema)
            self._tables.append(self.fms.table_name)


    def build(self):
        # run preparation steps
        self.prepare_connection()

        # connect
        self._schema = orator.Schema(self.db)

        # create schema
        self.create_schema()

        return self.db


    def drop_schema(self):
        # get drop order
        drop_order = list(self.version.tables)
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
        # self.add_user(self.user)


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


    def upload_dataset(self,
        dataset: Union[Dataset, str, pathlib.Path, pd.DataFrame],
        **kwargs) -> DatasetInfo:

        # enforce types
        checks.check_types(dataset, [Dataset, str, pathlib.Path, pd.DataFrame])

        # convert dataset
        if not isinstance(dataset, Dataset):
            dataset = Dataset(dataset, **kwargs)

        return


    def get_dataset(self, id=0):
        return pd.DataFrame()


    def get_items_from_table(self, table, conditions):
        return []


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
        dataset: Union[str, pathlib.Path, pd.DataFrame, None] = None,
        ds_info: Union[DatasetInfo, None] = None,
        name: Union[str, None] = None,
        description: Union[str, None] = None,
        filepath_columns: Union[str, List[str], None] = None,
        type_validation_map: Union[Dict[str, type], None] = None,
        value_validation_map: Union[Dict[str, types.FunctionType], None] = None,
        import_as_type_map: bool = False,
        replace_paths: Union[Dict[str, str], None] = None):

        # enforce types
        checks.check_types(dataset, [str, pathlib.Path,
                                     pd.DataFrame, type(None)])
        checks.check_types(ds_info, [DatasetInfo, type(None)])
        checks.check_types(name, [str, type(None)])
        checks.check_types(description, [str, type(None)])
        checks.check_types(filepath_columns, [str, list, type(None)])
        checks.check_types(type_validation_map, [dict, type(None)])
        checks.check_types(value_validation_map, [dict, type(None)])
        checks.check_types(import_as_type_map, bool)
        checks.check_types(replace_paths, [dict, type(None)])

        # must provide dataset or ds_info
        assert dataset is not None or ds_info is not None, MISSING_INIT

        # read dataset
        if isinstance(dataset, (str, pathlib.Path)):
            return read_dataset(dataset)

        # core properties
        self._dataframe = dataset
        self._info = ds_info

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

        # filepath_columns
        if self.info is None:
            self.filepath_columns = filepath_columns
        else:
            self.filepath_columns = self.info.filepath_columns

        # validation
        if self.info is None:
            self._validated = {"types": False,
                              "values": False,
                              "files": False}

            # run validations
            if import_as_type_map and type_validation_map is not None:
                self.coerce_types_using_map(type_validation_map)
            if replace_paths is not None:
                self.replace_paths_using_map(replace_paths)
            if filepath_columns is not None:
                self.enforce_files_exist_from_columns(filepath_columns)
            if type_validation_map is not None:
                self.enforce_types_using_map(type_validation_map)
            if value_validation_map is not None:
                self.enforce_values_using_map(value_validation_map)

        # equivalence check
        # if dataset is not None and ds_info is not None:
            # TODO:
            # check database source hash
            # against this object hash


    @property
    def info(self):
        return self._info


    @property
    def df(self):
        # get dataset if not in memory
        if self._dataframe is None:
            return self.info.origin.get_dataset(self.info.id)

        return self._dataframe


    @property
    def state(self):
        df_preped = True
        if self._dataframe is None:
            df_preped = False

        state = {"info": self.info,
                 "df": df_preped,
                 "name": self.name,
                 "description": self.description,
                 "filepath_columns": self.filepath_columns,
                 "validated_items": self._validated}

        return state


    def coerce_types_using_map(self,
        type_map: Union[Dict[str, type], None] = None):
        # assert new dataset
        assert self.info is None, EDITING_STORED_DATASET

        # enforce types
        checks.check_types(type_map, [dict, type(None)])

        # update type validation map
        self.type_validation_map = type_map

        # run casts
        if self.type_validation_map is not None:
            print("Casting values to type map...")
            checks.format_dataset(self.df, self.type_validation_map)

        # updated validated
        self._validated["values"] = False
        self._validated["types"] = False


    def replace_paths_using_map(self,
        path_replace: Union[Dict[str, str], None] = None):
        # assert new dataset
        assert self.info is None, EDITING_STORED_DATASET

        # enforce types
        checks.check_types(path_replace, [dict, type(None)])

        # update replace paths map
        self.replace_paths = path_replace

        # run replace
        if self.replace_paths is not None and self.filepath_columns is not None:
            print("Fixing filepaths using {r}...".format(r=self.replace_paths))
            checks.format_paths(self.df,
                                self.filepath_columns,
                                self.replace_paths)

        # update validated
        self._validated["values"] = False


    def enforce_files_exist_from_columns(self,
        fp_cols: Union[str, List[str], None] = None):
        # assert new dataset
        assert self.info is None, EDITING_STORED_DATASET

        # enforce types
        checks.check_types(fp_cols, [str, list, type(None)])

        # update filepath columns
        self.filepath_columns = fp_cols

        # run exists
        if self.filepath_columns is not None:
            print("Checking files exist...")
            checks.validate_dataset_files(self.df,
                                          self.filepath_columns)

        # update validated
        self._validated["files"] = True


    def enforce_types_using_map(self,
        type_map: Union[Dict[str, type], None] = None):
        # assert new dataset
        assert self.info is None, EDITING_STORED_DATASET

        # enforce types
        checks.check_types(type_map, [dict, type(None)])

        # update type validation map
        self.type_validation_map = type_map

        # run type checks
        if self.type_validation_map is not None:
            print("Checking dataset value types...")
            checks.validate_dataset_types(self.df,
                                          self.type_validation_map)

        # update validated
        self._validated["types"] = True


    def enforce_values_using_map(self,
        value_map: Union[Dict[str, types.FunctionType], None] = None):
        # assert new dataset
        assert self.info is None, EDITING_STORED_DATASET

        # enforce types
        checks.check_types(value_map, [dict, type(None)])

        # update value validation map
        self.value_validation_map = value_map

        # run value checks
        if self.value_validation_map is not None:
            print("Checking dataset values...")
            checks.validate_dataset_values(self.df,
                                           self.value_validation_map)

        # update validated
        self._validated["values"] = True


    def upload_to(self, database: DatasetDatabase) -> DatasetInfo:
        # enforce types
        checks.check_types(database, DatasetDatabase)

        database.run()


    def apply(self,
        function: Union[types.ModuleType, types.FunctionType]):
        return


    def save(self,
        path: Union[str, pathlib.Path],
        as_type: str = DATASET_EXTENSION,
        **kwargs) -> pathlib.Path:
        # enforce types
        checks.check_types(path, [str, pathlib.Path])

        # convert types
        path = pathlib.Path(path)
        if as_type[0] != ".":
            as_type = "." + as_type

        # dump dataset
        if as_type == DATASET_EXTENSION:
            path = path.with_suffix(".dataset")
            with open(path, "wb") as write_out:
                to_save = Dataset(self.df,
                                  self.info,
                                  self.name,
                                  self.description,
                                  self.filepath_columns)
                pickle.dump(to_save, write_out, **kwargs)
        else:
            path = path.with_suffix(as_type)
            self.dataframe.to_csv(path, **kwargs)

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

    # enforce paths
    assert path.suffix in APPROVED_EXTENSIONS, UNKNOWN_EXTENSION.format(p=path)

    # read csv
    if path.suffix == ".csv":
        dataset = pd.read_csv(path)
        path = path.with_suffix("")
        return Dataset(dataset=dataset, name=path.name)

    # read tsv
    if path.suffix == ".tsv":
        dataset = pd.read_csv(path, sep="\t")
        path = path.with_suffix("")
        return Dataset(dataset=dataset, name=path.name)

    # read packaged dataset
    with open(path, "rb") as read_in:
        return pickle.load(read_in)


# delayed globals
LOCAL = {"driver": "sqlite",
         "database": str(pathlib.Path("./local.db").resolve())}
LOCAL = DatabaseConfig(LOCAL)
