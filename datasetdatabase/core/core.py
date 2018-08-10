#!/usr/bin/env python

# installed
from datetime import datetime
from typing import Union
import pandas as pd
import pathlib
import pickle
import json

# self
from ..schema import SchemaVersion
from ..schema import FMSInterface

from ..utils import checks

# globals
REQUIRED_CONFIG_ITEMS = ("driver", "database")
MISSING_REQUIRED_ITEMS = "Config must have {i}".format(i=REQUIRED_CONFIG_ITEMS)
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
        config: Union[str, pathlib.Path, dict]):
        # enforce types
        checks.check_types(config, [str, pathlib.Path, dict])

        # convert types
        if isinstance(config, str):
            config = pathlib.Path(config)
        if isinstance(config, pathlib.Path):
            with open(config, "r") as read_in:
                config = json.load(read_in)

        # assert only single name and link given
        assert len(config) == 1, TOO_MANY_CONFIGS

        # enforce minimum
        for db_name, cfg in config.items():
            valid_config = all(k in REQUIRED_CONFIG_ITEMS for k in cfg)
            assert valid_config, MISSING_REQUIRED_ITEMS

            # import config
            self.name = db_name
            for key, item in cfg.items():
                self.__setattr__(key, item)

            # convert database link and enforce exists
            if self.driver == "sqlite":
                self.database = pathlib.Path(self.database)
                assert self.database.suffix == ".db", MALFORMED_LOCAL_LINK
                if not self.database.exists():
                    self.database.parent.mkdir(parents=True, exist_ok=True)


    def __str__(self):
        return str(self.__dict__)


    def __repr__(self):
        return str(self)


class DatasetDatabase(object):
    def __init__(self,
        config: DatabaseConfig,
        build: bool = True,
        user: Union[str, None] = None,
        schema_version: Union[SchemaVersion, None] = minimal,
        fms: Union[FMSInterface, None] = None,
        recent_size: int = 5):

        # enforce types
        checks.check_types(config, DatabaseConfig)
        checks.check_types(build, bool)
        checks.check_types(user, [str, type(None)])
        checks.check_types(schema_version, [SchemaVersion, type(None)])
        checks.check_types(fms, [FMSInterface, type(None)])

        # basic items
        self.user = checks.check_user(user)
        self.add_user(self.user)
        self._config = config

    def get_dataset(self, id=0):
        return pd.DataFrame()

    def get_items_from_table(self, table, conditions):
        return []


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
        filepath_columns: Union[str, list, None] = None,
        type_validation_map: Union[dict, None] = None,
        value_validation_map: Union[dict, None] = None,
        import_as_type_map: bool = False,
        replace_paths: Union[dict, None] = None):

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


    def coerce_types_using_map(self, type_map: Union[dict, None] = None):
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


    def replace_paths_using_map(self, path_replace: Union[dict, None] = None):
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
            fp_cols: Union[str, list, None] = None):
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


    def enforce_types_using_map(self, type_map: Union[dict, None] = None):
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


    def enforce_values_using_map(self, value_map: Union[dict, None] = None):
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
