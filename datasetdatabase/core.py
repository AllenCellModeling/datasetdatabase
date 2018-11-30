#!/usr/bin/env python

# installed
from pandas import read_csv as pd_read_csv
from typing import Union, Dict, List
from datetime import datetime
import _pickle as pickle
import subprocess
import inspect
import pathlib
import hashlib
import orator
import types
import json
import uuid
import os

# self
from .introspect import ObjectIntrospector
from .introspect import RECONSTRUCTOR_MAP
from .introspect import INTROSPECTOR_MAP
from .introspect import Introspector

from .schema import FMSInterface
from .schema import SchemaVersion
from .utils import checks, tools

from .version import VERSION

# globals
REQUIRED_CONFIG_ITEMS = ("driver", "database")
MISSING_REQUIRED_ITEMS = "Config must have {i}."\
                         .format(i=REQUIRED_CONFIG_ITEMS)
MALFORMED_LOCAL_LINK = "Local databases must have suffix '.db'"

INVALID_DS_INFO = "This set of attributes could not be found in the linked db."
NO_DS_INFO = "There is no dataset info attached to this dataset object."
DATETIME_PARSE = "%Y-%m-%d %H:%M:%S.%f"

MISSING_PARAMETER = "Must provide at least one of the following: {p}"
VERSION_LOOKUP = ["__version__", "__VERSION__", "VERSION", "version"]
UNKNOWN_DATASET_HASH = "Dataset hash changed.\n\tOriginal: {o}\n\tCurrent: {c}"

GENERIC_TYPES = Union[bytes, str, int, float, None, datetime]
MISSING_INIT = "Must provide either an object or a DatasetInfo object."
TOO_MANY_RETURN_VALUES = "Too many values returned from query expecting {n}."
DATASET_NOT_FOUND = "Dataset not found using keyword arguments:\n\t{kw}"
NONAPPROVED_PURGE = "Cannot purge a dataset that was used as an input."

MISSING_DATASET_INFO = "Dataset info attribute missing. No link to database."

UNKNOWN_EXTENSION = "Unsure how to read dataset from the passed path.\n\t{p}"


class DatabaseConfig(object):
    """
    Create a DatabaseConfig.

    A DatabaseConfig is an object you can create before connecting to a
    DatasetDatabase, but more commonly this object will be created by a
    DatabaseConstructor in the process of connecting or building. A minimum
    requirements config needs just "driver" and "database" attributes.


    #### Example
    ```
    >>> DatabaseConfig("/path/to/valid/config.json")
    {"name": "local",
     "config": {
        "driver": "sqlite",
        "database": "local.db"}
    }

    >>> DatabaseConfig("/path/to/invalid/config.json")
    AssertionError: "Config must have ('driver', 'database')."

    ```


    #### Parameters
    ##### config: str, pathlib.Path, Dict[str: str]
    A string, or pathlib.Path path to a json file storing the
    connection config information. Or a dictionary of string keys and
    string values as a connection config.

    ##### name: str, None = None
    A specific name for this connection. If none is passed the name
    gets set to the value stored by the "database" key in the passed
    config.


    #### Returns
    ##### self


    #### Errors
    ##### AssertionError
    One or more of the required config attributes are missing from the
    passed config.

    """

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
            config = config.expanduser()
            config = config.resolve()
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

    @property
    def config(self):
        return self._config

    def __iter__(self):
        yield self.name, self.config

    def __str__(self):
        return str(dict(self))

    def __repr__(self):
        return str(self)


class DatabaseConstructor(object):
    """
    Created a DatabaseConstructor.

    A DatabaseConstructor is an object used to create, teardown, or
    reconnect to both a database and the assoicated FMS. You can provide
    both a custom DatasetDatabase schema and a custom FMS interface.


    #### Example
    ```
    >>> DatabaseConstructor().config
    {"name": "local",
     "config": {
        "driver": "sqlite",
        "database": "local.db"
     }
    }

    >>> config = DatabaseConfig("path/to/valid/config.json")
    >>> DatabaseConstructor(config)
    <class DatabaseConstructor ... >

    ```


    #### Parameters
    ##### config: DatabaseConfig, None = None
    The config for the connection to the database. If None provided, LOCAL
    is chosen.

    ##### schema: SchemaVersion, None = None
    The schema to build. If None provided, Minimal is chosen.

    ##### fms: FMSInterface, None = None
    An FMS (File Management System) to handle the supporting files of
    datasets. If None provided, QuiltFMS is chosen.


    #### Returns
    ##### self


    #### Errors

    """

    def __init__(self,
                 config: Union[DatabaseConfig, None] = None,
                 schema: Union[SchemaVersion, None] = None,
                 fms: Union[FMSInterface, None] = None):
        # enforce types
        checks.check_types(config, [DatabaseConfig, type(None)])
        checks.check_types(schema, [SchemaVersion, type(None)])
        checks.check_types(fms, [FMSInterface, type(None)])

        # default config is local
        if config is None:
            config = LOCAL

        # store attributes
        self._config = config
        self._schema = schema
        self._fms = fms
        self._tables = []
        self._db = orator.DatabaseManager(dict(self.config))
        self._orator_schema = orator.Schema(self.db)

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
        """
        Prepare a database connection by asserting required attributes are
        present in the config passed. If the database link is a file, enforce
        that the file and all parent directories exist.


        #### Example
        ```
        >>> good_config.prepare_connection()


        >>> bad_config.prepare_connection()
        AssertionError: "Local databases must have suffix '.db'"

        ```


        #### Parameters

        #### Returns

        #### Errors
        ##### AssertionError
        The local database link is not the appropriate file type (.db).

        """

        # convert database link and enforce exists
        if self.config.config["driver"] == "sqlite":
            link = self.config.config["database"]
            link = pathlib.Path(link)
            assert link.suffix == ".db", MALFORMED_LOCAL_LINK
            if not link.exists():
                link.parent.mkdir(parents=True, exist_ok=True)

    def create_schema(self):
        """
        Create all the tables referenced by the SchemaVersion that was passed
        in the DatabaseConfig constructor and fms attachment.


        #### Example
        ```
        >>> constructor.create_schema()

        ```


        #### Parameters

        #### Returns

        #### Errors

        """

        # create all tables in version
        for tbl, func in self.schema.tables.items():
            func(self.orator_schema)
            if tbl not in self.tables:
                self._tables.append(tbl)

        # create file table from fms module
        if self.fms.table_name not in self.tables and \
                self.fms.table_name is not None:
            self.fms.create_File(self.orator_schema)
            self._tables.append(self.fms.table_name)

    def build(self):
        """
        Connect to a database and build the tables found in the SchemaVersion
        passed to the DatabaseConstructor initialization.

        This is mainly a wrapper around the prepare_connection and
        create_schema functions that additionally returns the
        orator.DatabaseManager object created.


        #### Example
        ```
        >>> constructor.build()

        ```

        #### Parameters

        #### Returns
        ##### db: orator.DatabaseManager
        A constructed database manager object that can be used to fully
        interact with the database, but additionally, all the tables have
        been stored in the constructor.tables attribute.


        #### Errors

        """

        # run preparation steps
        self.prepare_connection()

        # create schema
        self.create_schema()

        return self.db

    def _drop_schema(self):
        """
        Teardown the entire database connected to this DatabaseConstructor.

        From the original SchemaVersion passed, the tables list is reversed for
        teardown precedure. From there all tables are dropped in full one at a
        time.


        #### Example
        ```
        >>> constructor._drop_schema()

        ```


        #### Parameters

        #### Returns

        #### Errors

        """

        # get drop order
        drop_order = list(self.schema.tables)
        drop_order.reverse()

        # drop
        for tbl in drop_order:
            self.orator_schema.drop_if_exists(tbl)

    def get_tables(self):
        """
        In the case that a database is already fully constructed, a get_tables
        operation should be run so that the table map is fully up-to-date
        without overwriting or constructing useless tables.


        #### Example
        ```
        >>> constructor.get_tables()

        ```

        #### Parameters

        #### Returns
        ##### db: orator.DatabaseManager
        A constructed database manager object that can be used to fully
        interact with the database, but additionally, all the tables have
        been stored in the constructor.tables attribute.


        #### Errors

        """

        # run preparation steps
        self.prepare_connection()

        if self.config.config["driver"] == "sqlite":
            names = self.db.table("sqlite_master") \
                           .select("name") \
                           .where("type", "=", "table").get()
            names = [t["name"] for t in names if
                     t["name"] != "sqlite_sequence"]
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
    """
    Create a DatasetDatabase.

    A DatasetDatabase is an object you can create to both initialize a
    connection to an orator.DatabaseManager object but additional carry out
    many of the task that this package was created for such as ingestion,
    retrieval, search, etc.


    #### Example
    ```
    >>> DatasetDatabase(config="/path/to/valid/config.json")
    Recent Datasets:
    ------------------------------------------------------------------------

    >>> DatasetDatabase(config="/path/to/invalid/config.json")
    AssertionError: "Config must have ('driver', 'database')."

    ```


    #### Parameters
    ##### config: DatabaseConfig, str, pathlib.Path, dict, None = None
    An already created DatabaseConfig, or either a str, pathlib.Path,
    that when read, or dictionary, contains the required attributes to
    construct a DatabaseConfig. If None provided, a local database
    connection is created.

    ##### user: str, None = None
    What is the user name you would like to connect with. If None
    provided, the os system user is used.

    ##### constructor: DatabaseConstructor, None = None
    A specific database constructor that will either build the database
    schema or retrieve the database schema. If None is provided, one is
    created by initializing a new one and passing the DatabaseConfig,
    passed or created.

    ##### build: bool = False
    Should the constructor build or get the schema from the database.

    ##### recent_size: int = 5
    How many items should be returned by any of the recent calls.

    ##### processing_limit: int, None = None
    How many processes should the system max out at when ingesting or
    getting a dataset. If None provided, os.cpu_count() is used as
    default.


    #### Returns
    ##### self


    #### Errors

    """

    def __init__(self,
                 config: Union[
                    DatabaseConfig,
                    str,
                    pathlib.Path,
                    dict,
                    None
                 ] = None,
                 user: Union[str, None] = None,
                 constructor: Union[DatabaseConstructor, None] = None,
                 build: bool = False,
                 recent_size: int = 5,
                 processing_limit: Union[int, None] = None):
        # enforce types
        checks.check_types(config, [
            DatabaseConfig,
            str,
            pathlib.Path,
            dict,
            type(None)
        ])
        checks.check_types(user, [str, type(None)])
        checks.check_types(constructor, [DatabaseConstructor, type(None)])
        checks.check_types(build, bool)
        checks.check_types(recent_size, int)
        checks.check_types(processing_limit, [int, type(None)])

        # handle processing limit
        if processing_limit is None:
            processing_limit = os.cpu_count()

        # update os environ
        os.environ["DSDB_PROCESS_LIMIT"] = str(processing_limit)

        # assume local
        if config is None:
            config = LOCAL

        # read config
        if isinstance(config, (str, dict, pathlib.Path)):
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
        self._user_info = self.get_or_create_user(self.user)

    @property
    def config(self):
        return self._config

    @property
    def user(self):
        return self._user

    @property
    def user_info(self):
        return self._user_info

    @property
    def constructor(self):
        return self._constructor

    @property
    def db(self):
        return self._db

    def get_or_create_user(self,
                           user: Union[str, None] = None,
                           description: Union[str, None] = None):
        """
        Get or create a user in the database. This function is largely a
        wrapper around the insert to table functionality in that it gets or
        inserts whichever user and description is passed but additionally
        updates the user and user_info attributes of self.


        #### Example
        ```
        >>> db.get_or_create_user("jacksonb")
        {"UserId": 1,
         "Name": "jacksonb",
         "Description": None,
         "Created": 2018-09-28 16:39:09}

        ```


        #### Parameters
        ##### user: str, None = None
        What is the user name of the person you want to add to the database.
        If None provided, the user is determined by db.user.

        ##### description: str, None = None
        What is the description of the person you want to add to the
        database. If None provided, no description is given.


        #### Returns
        ##### user_info: dict
        A dictionary of the row found or created detailing the user.


        #### Errors
        ##### ValueError
        Too many rows returned from the database when expected only one or
        zero rows returned. This indicates something is drastically wrong
        with the database.

        """

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
            user_info = self._insert_to_table("User", {
                 "Name": user,
                 "Description": description,
                 "Created": datetime.utcnow()
                })

            self._user_info = user_info
            return self.user_info

        # found
        if len(user_info) == 1:
            self._user_info = user_info[0]
            return self.user_info

        # database structure error
        raise ValueError(TOO_MANY_RETURN_VALUES.format(n=1))

    def get_or_create_algorithm(self,
                                algorithm: Union[
                                    types.MethodType,
                                    types.FunctionType
                                ],
                                name: Union[str, None] = None,
                                description: Union[str, None] = None,
                                version: Union[str, None] = None):
        """
        Get or create an algorithm in the database. This function is largely a
        wrapper around the insert to table functionality in that it gets or
        inserts whichever algorithm name, description, and version is passed
        but additionally if none are passed tried to detect or create the
        parameters.


        #### Example
        ```
        >>> def hello_world():
                print("hello world")
        >>> # in a git repo
        >>> db.get_or_create_algorithm(hello_world)
        {"AlgorithmId": 2,
         "Name": "hello_world",
         "Description": "originally created by jacksonb",
         "Version": "akljsdf7asdfhkjasdf897sd87aa",
         "Created": 2018-09-28 16:46:32}

        >>> # out of a git repo
        >>> db.get_or_create_algorithm(hello_world)
        ValueError: Could not determine version of algorithm.

        ```


        #### Parameters
        ##### algorithm: types.MethodType, types.FunctionType
        Any python method of function that you want to use in processing a
        dataset.

        ##### name: str, None = None
        A name for the algorithm as it should be stored in the database. If
        None provided, the name is stored as the function name that was
        passed.

        ##### description: str, None = None
        A description for the algorithm as it should be stored in the
        database. If None provided, the description is a standard string
        created that details who originally added the algorithm to the
        database.

        ##### version: str, None = None
        A version for the algorithm. If None provided, there is an attempt
        to determine git commit hash of the code.


        #### Returns
        ##### user_info: dict
        A dictionary of the row found or created detailing the user.


        #### Errors
        ##### ValueError
        The version could not be determined through git hash and no version
        parameter was passed.

        """

        # enforce types
        checks.check_types(algorithm, [types.MethodType, types.FunctionType])
        checks.check_types(name, [str, type(None)])
        checks.check_types(description, [str, type(None)])
        checks.check_types(version, [str, type(None)])

        # get name
        if name is None:
            name = str(algorithm)
            method = "<bound method "
            function = "<function "
            end_at = " at "
            end_of = " of "
            try:
                end_index = name.index(end_at)
            except ValueError:
                end_index = name.index(end_of)
            if method in name:
                name = name[len(method): end_index]
            else:
                name = name[len(function): end_index]

        # get description
        if description is None:
            description = "Originally created by {u}".format(u=self.user)

        # get version
        if version is None:
            # naive module version lookup
            module = inspect.getmodule(algorithm)
            attrs = dir(module)
            for v_lookup in VERSION_LOOKUP:
                if v_lookup in attrs:
                    version = getattr(module, v_lookup)
                    break

            # naive git repo version lookup
            if version is None:
                version = ""
                try:
                    git_dir = os.path.dirname(os.path.abspath(__file__))
                    version = subprocess.check_output(
                        ["git", "rev-list", "-1", "HEAD", "./"], cwd=git_dir)
                    version = version.strip()
                    version = version.decode("utf-8")
                except subprocess.CalledProcessError:
                    pass

            if version == "":
                raise ValueError("Could not determine version of algorithm.")

        # enforce version string
        version = str(version)

        # get or create alg
        alg_info = self.get_items_from_table("Algorithm", [
            ["Name", "=", name], ["Version", "=", version]])

        # found
        if len(alg_info) == 1:
            return alg_info[0]
        # new
        elif len(alg_info) == 0:
            alg_info = self._insert_to_table("Algorithm", {
                "Name": name,
                "Description": description,
                "Version": version,
                "Created": datetime.utcnow()
            })
            return alg_info

        # database structure error
        else:
            raise ValueError(TOO_MANY_RETURN_VALUES.format(n=1))

    def process(self,
                algorithm: Union[types.MethodType, types.FunctionType],
                input_dataset: Union["Dataset", None] = None,
                input_dataset_info: Union["DatasetInfo", None] = None,
                input_dataset_id: Union[int, None] = None,
                input_dataset_name: Union[str, None] = None,
                algorithm_name: Union[str, None] = None,
                algorithm_description: Union[str, None] = None,
                algorithm_version: Union[str, None] = None,
                run_name: Union[str, None] = None,
                run_description: Union[str, None] = None,
                algorithm_parameters: dict = {},
                output_dataset_name: Union[str, None] = None,
                output_dataset_description: Union[str, None] = None):
        """
        This is largely the core function of the database as most other
        functions in some way are passed through this function as to retain
        information regarding how datasets change and are manipulated. However
        as a standalone function you can pass a processing function or method
        through this to pull and apply to a dataset before returning the
        results back. It is recommended however that if you want to use the
        above described behavior to use a `dataset.apply` command.


        #### Example
        ```
        >>> test_dataset = Dataset(data)
        >>> def increment_column(dataset, column):
                dataset.ds[column] += 1

                return dataset
        >>> db.process(increment_column,
                       test_dataset,
                       algorithm_parameters={"column": "my_column"})
        info: {'id': 4, 'name': '720a1712-0287-4690-acbc-80a6617ebc63', ...

        ```


        #### Parameters
        ##### algorithm: types.MethodType, types.FunctionType
        Any python method of function that you want to use in processing a
        dataset.

        ##### input_dataset: Dataset, None = None
        Which dataset to apply the algorithm to.

        ##### input_dataset_id: int, None = None
        Which dataset to pull before applying the algorithm to.

        ##### input_dataset_info: DatasetInfo, None = None
        Which dataset to pull before applying the algorithm to.

        ##### input_dataset_name: str, None = None
        Which dataset to pull before applying the algorithm to.

        ##### algorithm_name: str, None = None
        A name for the algorithm as it should be stored in the database. If
        None provided, the name is stored as the function name that was
        passed.

        ##### algorithm_description: str, None = None
        A description for the algorithm as it should be stored in the
        database. If None provided, the description is a standard string
        created that details who originally added the algorithm to the
        database.

        ##### algorithm_version: str, None = None
        A version for the algorithm. If None provided, there is an attempt
        to determine git commit hash of the code.

        ##### run_name: str, None = None
        A name for this specific run. Usually left blank but if a specific
        run is rather important and you want to easily find it later you
        can detail a name here.

        ##### run_description: str, None = None
        A description for this specific run. Usually left blank but if a
        specific run is rather important and you need more than just a run
        name you can detail a run description here.

        ##### algorithm_parameters: dict = {}
        A dictionary of parameters that get passed to the algorithm. The
        dictionary gets expanded when passed to the function so the
        parameters become keyword arguments.

        ##### output_dataset_name: str, None = None
        A name for the produced dataset.

        ##### output_dataset_description: str, None = None
        A description for the produced dataset.


        #### Returns
        ##### output: Dataset
        A dataset of the produced containing the results from applying the
        passed application.


        #### Errors
        ##### AssertionError
        Missing parameters, must provided at least one of the various
        parameter option sets.

        ##### ValueError
        Malformed database, results from the database were not the format
        expected.

        """

        # enforce types
        checks.check_types(algorithm, [types.MethodType, types.FunctionType])
        checks.check_types(input_dataset, [Dataset, type(None)])
        checks.check_types(input_dataset_info, [DatasetInfo, type(None)])
        checks.check_types(input_dataset_id, [int, type(None)])
        checks.check_types(input_dataset_name, [str, type(None)])
        checks.check_types(algorithm_name, [str, type(None)])
        checks.check_types(algorithm_description, [str, type(None)])
        checks.check_types(algorithm_version, [str, type(None)])
        checks.check_types(run_name, [str, type(None)])
        checks.check_types(run_description, [str, type(None)])
        checks.check_types(algorithm_parameters, dict)
        checks.check_types(output_dataset_name, [str, type(None)])
        checks.check_types(output_dataset_description, [str, type(None)])

        # must provide input dataset info
        dataset_lookups = [input_dataset, input_dataset_info,
                           input_dataset_id, input_dataset_name]
        assert any(i is not None for i in dataset_lookups), \
            MISSING_PARAMETER.format(p=dataset_lookups)

        # convert dataset to workable type
        if input_dataset is not None:
            # check hash
            found_ds = self.get_items_from_table(
                "Dataset", [["MD5", "=", input_dataset.md5],
                            ["SHA256", "=", input_dataset.sha256]])

            # found
            if len(found_ds) == 1:
                ds_info = found_ds[0]
                ds_info["OriginDb"] = self
                input_dataset._info = DatasetInfo(**ds_info)
                input = input_dataset

            # not found
            elif len(found_ds) == 0:
                input = input_dataset

            # database structure error
            else:
                raise ValueError(TOO_MANY_RETURN_VALUES.format(n=1))

        elif input_dataset_info is not None:
            input = input_dataset_info.origin.get_dataset(
                input_dataset_info.id)
        elif input_dataset_id is not None:
            input = self.get_dataset(id=input_dataset_id)
        else:
            input = self.get_dataset(name=input_dataset_name)

        # create algorithm info before run
        alg_info = self.get_or_create_algorithm(algorithm,
                                                algorithm_name,
                                                algorithm_description,
                                                algorithm_version)

        print("Storing algorithm parameters.")
        # check parameters for dsdb parameters
        if algorithm_name == "dsdb.Dataset.store_files":
            db = algorithm_parameters.pop("db", None)
            fms = algorithm_parameters.pop("fms", None)

            alg_params_ds = self._create_dataset(
                algorithm_parameters,
                description="algorithm parameters")

            algorithm_parameters["db"] = db
            algorithm_parameters["fms"] = fms

        # store params used
        else:
            alg_params_ds = self._create_dataset(
                algorithm_parameters,
                description="algorithm parameters")

        # begin
        if algorithm_name != "dsdb.DatasetDatabase.upload_dataset":
            print("Dataset processing has begun...")

        begin = datetime.utcnow()

        # run
        output = algorithm(input, **algorithm_parameters)

        # end
        end = datetime.utcnow()
        if algorithm_name != "dsdb.DatasetDatabase.upload_dataset":
            print("Dataset processing has ended...")

        # handle returned dataset
        if isinstance(output, Dataset):
            output = output.ds

        # ingest output
        output = self._create_dataset(
            output,
            name=output_dataset_name,
            description=output_dataset_description)

        # update run table
        run_info = self._insert_to_table("Run", {
            "AlgorithmId": alg_info["AlgorithmId"],
            "UserId": self.user_info["UserId"],
            "Name": run_name,
            "Description": run_description,
            "AlgorithmParameters": alg_params_ds.info.id,
            "Begin": begin,
            "End": end
        })

        # update run input table
        if input.info is not None:
            self._insert_to_table("RunInput", {
                "RunId": run_info["RunId"],
                "DatasetId": input.info.id,
                "Created": end
            })

        # update run output table
        self._insert_to_table("RunOutput", {
            "RunId": run_info["RunId"],
            "DatasetId": output.info.id,
            "Created": end})

        return output

    def _create_dataset(self,
                        dataset: Union["Dataset", object],
                        **kwargs) -> "DatasetInfo":
        # Hidden create dataset method used by the database to actually enforce
        # datasets are unique and exist. Additionally this is the function that
        # starts the dataset deconstruction and subsequent creation of a new
        # DatasetInfo block. Once both are complete the created dataset is
        # returned.

        # enforce types
        checks.check_types(dataset, [Dataset, object])

        # convert dataset
        if not isinstance(dataset, Dataset):
            dataset = Dataset(dataset, **kwargs)

        # check hash
        found_ds = self.get_items_from_table(
            "Dataset", [["MD5", "=", dataset.md5],
                        ["SHA256", "=", dataset.sha256]])

        # found
        if len(found_ds) == 1:
            ds_info = found_ds[0]
            ds_info["OriginDb"] = self
            ds_info = DatasetInfo(**ds_info)

            print("Input dataset already exists in database.", ds_info.id)
            return Dataset(dataset=dataset.ds, ds_info=ds_info)

        # not found
        elif len(found_ds) == 0:
            introspector_module = str(type(dataset.introspector))
            begin = len("<class '")
            end = introspector_module.index("'>")
            introspector_module = introspector_module[begin: end]
            ds_info = {"Name": dataset.name,
                       "Description": dataset.description,
                       "Introspector": introspector_module,
                       "MD5": dataset.md5,
                       "SHA256": dataset.sha256,
                       "Created": dataset.created}
            ds_info["DatasetId"] = self.db.table("Dataset")\
                .insert_get_id(ds_info, sequence=("DatasetId"))
            ds_info["OriginDb"] = self

            ds_info = DatasetInfo(**ds_info)

        # database structure error
        else:
            raise ValueError(TOO_MANY_RETURN_VALUES.format(n=1))

        # deconstruct
        dataset.introspector.deconstruct(db=self.db, ds_info=ds_info, fms=self.constructor.fms)

        # attach info to a dataset
        return Dataset(dataset=dataset.ds, ds_info=ds_info)

    def _upload_dataset(self, dataset, **params):
        # Hidden upload dataset function used by the process method to simply
        # pass the non linked dataset to the output which will then be stored
        # as a linked dataset.

        return dataset

    def upload_dataset(self, dataset: "Dataset", **kwargs) -> "DatasetInfo":
        """
        Upload a dataset to the database. Simply put it prepares a dataset for
        ingestion and passes parameters to the process function to properly
        record ingestion time, result, user, etc. If the dataset already exists
        in the database, it is not stored again and is simply fast returned
        with the attached DatasetInfo block. It is recommended that you use a
        `dataset.upload_to` command to upload datasets to database but this
        function is the underlying core of that function.


        #### Example
        ```
        >>> data = Dataset("/path/to/some/data.csv")
        >>> db.upload_dataset(data)

        ```


        #### Parameters
        ##### dataset: Dataset
        The dataset object ready for ingestion to a database.


        #### Returns
        ##### dataset: Dataset
        The same dataset object post ingestion with a DatasetInfo block
        attached.


        #### Errors
        ##### AssertionError
        Unknown dataset hash. The hash for the passed dataset does not
        match the hash for the originally intialized dataset. Usually
        indicates that the dataset has in some way changed since original
        creation.

        """

        # enforce types
        checks.check_types(dataset, Dataset)

        # enforce no change since create
        curr_md5 = dataset.introspector.get_object_hash()
        md5_match = curr_md5 == dataset.md5
        assert md5_match, UNKNOWN_DATASET_HASH.format(
            o=dataset.md5, c=curr_md5)

        # create basic params
        create_params = {}
        create_params["algorithm"] = self._upload_dataset
        create_params["input_dataset"] = dataset
        create_params["algorithm_name"] = "dsdb.DatasetDatabase.upload_dataset"
        create_params["algorithm_description"] = "DSDB ingest dataset function"
        create_params["algorithm_version"] = VERSION
        create_params["output_dataset_name"] = dataset.name
        create_params["output_dataset_description"] = dataset.description

        # update introspector after create
        current_introspector = dataset.introspector
        current_annotations = dataset.annotations
        uploaded = self.process(**create_params)
        uploaded._introspector = current_introspector
        uploaded._introspector._validated = True
        uploaded._annotations = current_annotations
        uploaded.update_annotations()

        return uploaded

    def get_dataset(self,
                    name: Union[str, None] = None,
                    id: Union[int, None] = None) -> "Dataset":
        """
        Pull and reconstruct a dataset from the database. Must provided either
        a dataset name or a dataset id to retrieve the dataset.


        #### Example
        ```
        >>> db.get_dataset("Label Free Images")
        {info: ...}

        >>> db.get_dataset("This dataset doesn't exist")
        ValueError: Dataset not found using parameters...

        ```


        #### Parameters
        ##### name: str, None = None
        The name of the dataset you want to reconstruct.

        ##### id: int, None = None
        The id of the dataset you want to reconstruct.


        #### Returns
        ##### dataset: Dataset
        A reconstructed Dataset with attached DatasetInfo block.


        #### Errors
        ##### AssertionError
        Missing parameter, must provided either id or name.

        ##### ValueError
        Dataset not found using the provided id or name.

        ##### ValueError
        Malformed database error, too many values returned from a query
        expecting a single value or no value to return.

        """

        # enforce types
        checks.check_types(name, [str, type(None)])
        checks.check_types(id, [int, type(None)])

        # must pass parameter
        assert any(i is not None for i in [id, name]), \
            MISSING_PARAMETER.format(p=["id", "name"])

        # get ds_info
        if id is not None:
            found_ds = self.get_items_from_table(
                "Dataset", ["DatasetId", "=", id])
        else:
            found_ds = self.get_items_from_table(
                "Dataset", ["Name", "=", name])

        # found
        if len(found_ds) == 1:
            ds_info = found_ds[0]
            ds_info["OriginDb"] = self
            ds_info = DatasetInfo(**ds_info)

        # not found
        elif len(found_ds) == 0:
            raise ValueError(DATASET_NOT_FOUND.format(kw=locals()))

        # database structure error
        else:
            raise ValueError(TOO_MANY_RETURN_VALUES.format(n=1))

        # reconstruct object
        obj = RECONSTRUCTOR_MAP[ds_info.introspector](db=self.db, ds_info=ds_info, fms=self.constructor.fms)

        return Dataset(dataset=obj, ds_info=ds_info)

    def preview(self,
                name: Union[str, None] = None,
                id: Union[int, None] = None) -> "Dataset":
        """
        Pull and create summary info about a dataset from the database. Must
        provided either a dataset name or a dataset id to retrieve the dataset.


        #### Example
        ```
        >>> db.preview("Label Free Images")
        {info: ...}

        >>> db.preview("This dataset doesn't exist")
        ValueError: Dataset not found using parameters...

        ```


        #### Parameters
        ##### name: str, None = None
        The name of the dataset you want to preview.

        ##### id: int, None = None
        The id of the dataset you want to preview.


        #### Returns
        ##### preview: Dataset
        A dictionary with summary info about a dataset that contains things
        like the DatasetInfo block, the shape, columns/ keys, and any
        annotations.


        #### Errors
        ##### AssertionError
        Missing parameter, must provided either id or name.

        ##### ValueError
        Dataset not found using the provided id or name.

        ##### ValueError
        Malformed database error, too many values returned from a query
        expecting a single value or no value to return.

        """

        # enforce types
        checks.check_types(name, [str, type(None)])
        checks.check_types(id, [int, type(None)])

        # must pass parameter
        assert any(i is not None for i in [id, name]), \
            MISSING_PARAMETER.format(p=["id", "name"])

        # get ds_info
        if id is not None:
            found_ds = self.get_items_from_table(
                "Dataset", ["DatasetId", "=", id])
        else:
            found_ds = self.get_items_from_table(
                "Dataset", ["Name", "=", name])

        # found
        if len(found_ds) == 1:
            ds_info = found_ds[0]
            ds_info["OriginDb"] = self
            ds_info = DatasetInfo(**ds_info)

        # not found
        elif len(found_ds) == 0:
            raise ValueError(DATASET_NOT_FOUND.format(kw=locals()))

        # database structure error
        else:
            raise ValueError(TOO_MANY_RETURN_VALUES.format(n=1))

        # get groups
        groups = self.get_items_from_table(
            "GroupDataset", ["DatasetId", "=", ds_info.id])

        # get first group
        iota_groups = self.get_items_from_table(
            "IotaGroup", ["GroupId", "=", groups[0]["GroupId"]])

        # shape
        shape = (len(groups), len(iota_groups))

        # keys
        keys = []
        for ig in iota_groups:
            keys.append(self.get_items_from_table(
                "Iota", ["IotaId", "=", ig["IotaId"]])[0]["Key"])

        # annotations
        annotation_datasets = self.get_items_from_table(
            "AnnotationDataset", ["DatasetId", "=", ds_info.id])

        # annotations
        annotations = []
        for ad in annotation_datasets:
            annotations.append(self.get_items_from_table(
                "Annotation", ["AnnotationId", "=", ad["AnnotationId"]])[0])

        return {"info": ds_info,
                "shape": shape,
                "keys": keys,
                "annotations": annotations}

    def get_items_from_table(self,
                             table: str,
                             conditions: List[
                                Union[
                                    List[GENERIC_TYPES],
                                    GENERIC_TYPES
                                ]
                             ] = []):
        """
        Get items from a table that match conditions passed. Primarily a
        wrapper around orator's query functionality.


        #### Example
        ```
        >>> db.get_items_from_table("Dataset", ["Name", "=", "Test Dataset"])
        [{"DatasetId": 2, "Name": "Test Dataset", ...}]

        >>> db.get_items_from_table("Dataset")
        [{...},
         {...},
         {...}]

        >>> db.get_items_from_table("Dataset", [
        >>>         ["Description", "=", "algorithm_parameters"],
        >>>         ["Created", "=", datetime.now()]])
        []

        ```


        #### Parameters
        ##### table: str
        Which table you want to get items from.

        ##### conditions: List[Union[List[GENERIC_TYPES], GENERIC_TYPES]]
        A list or a list of lists containing generic types that the database
        can construct where conditions from. The where conditions are
        AND_WHERE conditions, not OR_WHERE.


        #### Returns
        ##### results: List[dict]
        A list of dictionaries containing all the items found that match
        the conditions passed.


        #### Errors

        """

        # enforce types
        checks.check_types(table, str)
        checks.check_types(conditions, list)

        return tools.get_items_from_db_table(self.db, table, conditions)

    def _insert_to_table(self,
                         table: str,
                         items: Dict[str, GENERIC_TYPES]):
        # Hidden insert to table function used to insert values to tables.
        # Given a table name as a string and a dictionary with the items to
        # insert as key = column name, and value = value, will insert the row
        # into the table. Additionally it returns this created row.

        # enforce types
        checks.check_types(table, str)
        checks.check_types(items, dict)

        return tools.insert_to_db_table(self.db, table, items)

    def _purge_dataset(self,
                       name: Union[str, None] = None,
                       id: Union[int, None] = None):
        # Hidden purge dataset method that will simply remove the group dataset
        # joins, the run output, run, and dataset rows related to a dataset
        # name or id. If the dataset is used a run input the dataset will not
        # be removed.

        # enforce types
        checks.check_types(name, [str, type(None)])
        checks.check_types(id, [int, type(None)])

        # must pass parameter
        assert any(i is not None for i in [id, name]), \
            MISSING_PARAMETER.format(p=["id", "name"])

        # specific
        if id is None:
            id = self.get_items_from_table(
                "Dataset", ["Name", "=", name])[0]["DatasetId"]

        # do not remove a dataset used as an input
        if self.db.table("RunInput").where("DatasetId", "=", id).count() > 0:
            raise ValueError(NONAPPROVED_PURGE)

        # get all runs
        runs = self.get_items_from_table(
            "RunOutput", ["DatasetId", "=", id])

        # deletes
        self.db.table("GroupDataset").where("DatasetId", "=", id).delete()
        self.db.table("RunOutput").where("DatasetId", "=", id).delete()
        for run in runs:
            self.db.table("Run").where("RunId", "=", run["RunId"]).delete()
        self.db.table("Dataset").where("DatasetId", "=", id).delete()

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
    """
    Create a DatasetInfo.

    A DatasetInfo is an object usually created by a database function to be
    returned to the user as a block attached to a Dataset. It's primary
    responsibility is to be used as a verification source against changed
    datasets.


    #### Example
    ```
    >>> stats = db.preview("Label Free Images")
    >>> type(stats["info"])
    DatasetInfo

    ```


    #### Parameters
    ##### DatasetId: int
    The dataset id stored in the database.

    ##### Name: str, None
    The dataset name stored in the database.

    ##### Introspector: str
    Which introspector should be used or was used to deconstruct and
    reconstruct the dataset.

    ##### MD5: str
    The MD5 hash of the underlying data object.

    ##### SHA256: str
    The SHA256 hash of the underlying data object.

    ##### Created: datetime, str
    The utc datetime when the dataset was created.

    ##### OriginDb: DatasetDatabase
    The database that this dataset is stored in.

    ##### Description: str, None = None
    The description for the dataset.


    #### Returns
    ##### self


    #### Errors
    ##### AssertionError
    The attributes passed could not be verified in the database.

    """

    def __init__(self,
                 DatasetId: int,
                 Name: Union[str, None],
                 Introspector: str,
                 MD5: str,
                 SHA256: str,
                 Created: Union[datetime, str],
                 OriginDb: DatasetDatabase,
                 Description: Union[str, None] = None):
        # enforce types
        checks.check_types(DatasetId, int)
        checks.check_types(Name, [str, type(None)])
        checks.check_types(Description, [str, type(None)])
        checks.check_types(Introspector, str)
        checks.check_types(MD5, str)
        checks.check_types(SHA256, str)
        checks.check_types(Created, [datetime, str])
        checks.check_types(OriginDb, DatasetDatabase)

        # convert types
        if isinstance(Created, str):
            Created = datetime.strptime(Created, DATETIME_PARSE)

        # set attributes
        self._id = DatasetId
        self._name = Name
        self._description = Description
        self._introspector = Introspector
        self._md5 = MD5
        self._sha256 = SHA256
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
    def introspector(self):
        return self._introspector

    @property
    def md5(self):
        return self._md5

    @property
    def sha256(self):
        return self._sha256

    @property
    def created(self):
        return self._created

    @property
    def origin(self):
        return self._origin

    def _validate_info(self):
        # Hidden function used to validate that all the attributes passed do
        # actually exist and are the same in database.

        # get found
        found = self.origin.get_items_from_table("Dataset", [
            ["MD5", "=", self.md5],
            ["SHA256", "=", self.sha256]
        ])

        # assert that it exists by len(1)
        assert len(found) == 1, INVALID_DS_INFO

    def __str__(self):
        return str({"id": self.id,
                    "name": self.name,
                    "description": self.description,
                    "introspector": self.introspector,
                    "created": self.created})

    def __repr__(self):
        return str(self)


class Dataset(object):
    """
    Create a Dataset.

    A Dataset is an object that allows for easiser validation of the attached
    object, application of algorithms against an object, as well as some other
    dataset state niceities. This object can be independently used from a
    DatasetDatabase as the data validation mechanisms alone are rather nice to
    use. However if you are doing computational research it is highly
    recommended you use both the Dataset and DatasetDatabase objects together.


    #### Example
    ```
    >>> data = Dataset(data)
    {info: None,
     ds: True,
     md5: "aksjk09sdafj8912u8naosdf8234",
     sha256: "oasjdfa0231982q0934jlkajsdf8912lkjasdf",
     ...}

    ```


    #### Parameters
    ##### dataset: object, None = None
    The data object you want to use as a dataset. This can be anything python
    object. This object gets passed to an introspector and so the Dataset
    object becomes more valuable if the object you are wanting to store has an
    assoicated dataset.

    If a string or pathlib.Path are passed the `read_dataset` function is used
    to determine how to read the dataset.

    ##### ds_info: DatasetInfo, None = None
    If a dataset is being created or reconstructed by a database it will have a
    DatasetInfo object to attach to ensure that data doesn't change between
    operations.

    ##### name: str, None = None
    A name for the dataset. This will be stored by the database and cannot be
    changed once ingested to a database. If you originally initialized with a
    name but before you ingested a dataset to a database you can change the
    name.

    ##### description: str, None = None
    A description for the dataset. This will be stored by the database and like
    the name, cannot be changed once ingested to a database. Again like the
    name, after initialization but before ingestion you can change the
    description however you like.

    ##### introspector: Introspector, str, None = None
    Datasets use introspectors to teardown and reconstruct datasets to their
    Iota and Groups. If None is provided, an introspector is determined by type
    but you can optionally force an introspector to be used.


    #### Returns
    ##### self


    #### Errors
    ##### AssertionError
    Must pass one or both of the following, dataset or ds_info.

    """

    def __init__(self,
                 dataset: Union[object, None] = None,
                 ds_info: Union[DatasetInfo, None] = None,
                 name: Union[str, None] = None,
                 description: Union[str, None] = None,
                 introspector: Union[Introspector, str, None] = None):

        # enforce types
        checks.check_types(dataset, [object, type(None)])
        checks.check_types(ds_info, [DatasetInfo, type(None)])
        checks.check_types(name, [str, type(None)])
        checks.check_types(description, [str, type(None)])
        checks.check_types(introspector, [Introspector, str, type(None)])

        # must provide dataset or ds_info
        assert dataset is not None or ds_info is not None, MISSING_INIT

        # read dataset
        if isinstance(dataset, (str, pathlib.Path)):
            dataset = read_dataset(dataset).ds

        # info
        self._info = ds_info

        # introspector
        if self.info is None:
            if introspector is None:
                t_ds = type(dataset)
                if t_ds in INTROSPECTOR_MAP:
                    self._introspector = INTROSPECTOR_MAP[t_ds](dataset)
                else:
                    self._introspector = ObjectIntrospector(dataset)
            else:
                self._introspector = introspector
        else:
            self._introspector = self.info.introspector

        # read introspector
        if isinstance(self.introspector, str):
            self._introspector = INTROSPECTOR_MAP[self.introspector](dataset)

        # update hashes
        self._md5 = self.introspector.get_object_hash()
        self._sha256 = self.introspector.get_object_hash(hashlib.sha256)

        # unpack based on info
        # name
        if name is None:
            name = str(uuid.uuid4())
        if self.info is None:
            self.name = name
        else:
            self.name = self.info.name

        # description
        if self.info is None:
            self.description = description
        else:
            self.description = self.info.description

        # annotations
        if self.info is None:
            self._annotations = []
        else:
            annotation_dataset = self.info.origin.get_items_from_table(
                "AnnotationDataset", ["DatasetId", "=", self.info.id])
            annotations = []
            for ad in annotation_dataset:
                annotations += self.info.origin.get_items_from_table(
                    "Annotation", ["AnnotationId", "=", ad["AnnotationId"]])
            self._annotations = annotations

        # created
        if self.info is None:
            self.created = datetime.utcnow()
        else:
            self.created = self.info.created

    @property
    def info(self):
        return self._info

    @property
    def ds(self):
        # get dataset if not in memory
        if self.introspector.obj is None:
            ds = self.info.origin.get_dataset(self.info.id)
            self._introspector._obj = ds

        return self.introspector.obj

    @property
    def validated(self):
        if self.info is None:
            return self.introspector.validated

        return True

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
    def annotations(self):
        return self._annotations

    @property
    def state(self):
        ds_ready = self.ds is not None
        state = {"info": self.info,
                 "ds": ds_ready,
                 "name": self.name,
                 "description": self.description,
                 "introspector": type(self.introspector),
                 "md5": self.md5,
                 "sha256": self.sha256,
                 "validated": self.validated,
                 "annotations": self.annotations}

        return state

    def validate(self, **kwargs):
        """
        Validate a dataset using the assigned introspectors validation method.
        This is a wrapper around that functionality and for specific dataset
        validation details look at your objects introspector for which
        documentation you should look at.

        [DataFrame Introspectors](../introspect/dataframe) are most common.

        Additionally, because values may change during validation, this
        function also updates the datasets md5 and sha256.

        No object is returned, the current object is updated to reflect the
        changes made (if any).


        #### Example
        ```
        >>> dataframe_dataset.validate(filepath_columns="files")

        ```


        #### Parameters
        ##### **kwargs
        All arguments given will be passed to the objects introspector
        validate.


        #### Returns


        #### Errors

        """

        # validate obj
        self.introspector.validate(**kwargs)

        # update hashes
        self._md5 = self.introspector.get_object_hash()
        self._sha256 = self.introspector.get_object_hash(hashlib.sha256)

    def store_files(self, **kwargs):
        """
        Store the supporting files of a dataset using the assigned
        introspectors store_files method. This is a wrapper around that
        functionality and for specific dataset store_files details look at your
        objects introspector for which documentation you should look at.

        [DataFrame Introspectors](../introspect/dataframe) are most common.

        Unlike validate, this is an operation to be done after ingestion to a
        database as a database link is required for the FMS functionality to
        fully work.

        No object is returned, the current object is updated to reflect the
        changes made (if any).


        #### Example
        ```
        >>> dataframe_dataset.store_files(filepath_columns="files")

        ```


        #### Parameters
        ##### **kwargs
        All arguments given will be passed to the objects introspector
        store_files.


        #### Returns


        #### Errors
        ##### AttributeError
        This dataset has not been ingested to a database yet as the DatasetInfo
        block is not populated.

        """

        # enforce data
        if self.info is None:
            raise AttributeError(MISSING_DATASET_INFO)

        # prep params
        params = {"db": self.info.origin.constructor.db,
                  "fms": self.info.origin.constructor.fms,
                  **kwargs}

        # store
        self.apply(self.introspector.store_files,
                   algorithm_name="dsdb.Dataset.store_files",
                   algorithm_version=VERSION,
                   output_dataset_name=self.name + " (FMS)",
                   output_dataset_description=self.description,
                   algorithm_parameters=params)

    def upload_to(self, database: DatasetDatabase):
        """
        Upload the dataset to a database. This is a wrapper around the
        database.uplaod_dataset functionality that in-itself is a wrapper
        around the objects introspector deconstruct function.

        [DataFrame Introspectors](../introspect/dataframe) are most common.

        No object is returned, the current object is updated to reflect the
        changes made (if any).


        #### Example
        ```
        >>> data.upload_to(db)

        ```


        #### Parameters
        ##### database: DatasetDatabase
        The target database to upload to.


        #### Returns


        #### Errors

        """
        # enforce types
        checks.check_types(database, DatasetDatabase)

        # run upload
        ds = database.upload_dataset(self)

        # reassign self
        self._reassign_self(ds)

    def apply(self,
              algorithm: Union[types.MethodType, types.FunctionType],
              database: Union[DatasetDatabase, None] = None,
              algorithm_name: Union[str, None] = None,
              algorithm_description: Union[str, None] = None,
              algorithm_version: Union[str, None] = None,
              run_name: Union[str, None] = None,
              run_description: Union[str, None] = None,
              algorithm_parameters: dict = {},
              output_dataset_name: Union[str, None] = None,
              output_dataset_description: Union[str, None] = None):
        """
        Apply an algorithm against a dataset. Given an algorithm (function or
        method), run the algorithm against the dataset and store the results in
        the linked database before updating the current object.

        No object is returned, the current object is updated to reflect the
        changes made (if any).


        #### Example
        ```
        >>> data.apply(func)

        ```


        #### Parameters
        ##### algorithm: types.MethodType, types.FunctionType
        The method of function to run against the dataset. Note: the function
        or method must have a dataset parameters that this dataset can be
        passed to, any other parameters required will not only be stored by the
        database but additionally be passed and expanded into the function call
        as keyword arguments. An example of this would look like the following
        `def increment_column(dataset, column):` where dataset is this dataset
        that is passed and the column would be a keyword argument expanded from
        the other "algorithm_parameters" parameter.

        ##### database: DatasetDatabase, None = None
        If this dataset is not currently ingested to a database you can pass a
        database as a parameter to first upload the current dataset to and then
        run the algorithm and store the results in the same database.

        ##### algorithm_name: str, None = None
        A name for the algorithm. If None provided, the name stored is the
        function or method name.

        ##### algorithm_description: str, None = None
        A description for the algorithm. If None provided, a description is
        written and stored in the database based off who was the original user
        to apply this algorithm.

        ##### algorithm_version: str, None = None
        A version for the algorith. If None provided, a version is attempted to
        be determined from module version or from git commit hash.

        ##### run_name: str, None = None
        If an algorithm application occurs often as a daily/ hourly/ etc
        process and you want to be able to track runs more quickly and easily
        than looking through dataset ids giving a run a name is the best way to
        do so.

        ##### run_description: str, None = None
        Much like the run_name parameter, if you a run occurs often or just has
        more significance than other runs, it is recommended to attach
        information here for easily discovery and tracking.

        ##### algorithm_parameters: dict = {}
        These parameters will be expanded as keyword arguments to whatever '
        algorithm was passed. Example: {"column": "size"} would be expanded to
        column="size" in the function call. These parameters are additionally
        stored in the database as their own dataset so that if processing
        didn't go as expected you can reconstruct the parameters as a double
        check of parameters vs algorithm issues.

        ##### output_dataset_name: str, None = None
        The name for the produced dataset. If None provided, a GUID is
        generated and used as the name.

        ##### output_dataset_description: str, None = None
        A description for the produced dataset.


        #### Returns


        #### Errors
        ##### KeyError
        If DatasetInfo is missing and no database is provided this function
        fails quickly.

        ##### ValueError
        Too many datasets found with the same MD5 and SHA256, indicating
        something is drastically wrong with the database.

        """

        # enforce types
        checks.check_types(algorithm, [types.MethodType, types.FunctionType])
        checks.check_types(database, [DatasetDatabase, type(None)])
        checks.check_types(algorithm_name, [str, type(None)])
        checks.check_types(algorithm_description, [str, type(None)])
        checks.check_types(algorithm_version, [str, type(None)])
        checks.check_types(run_name, [str, type(None)])
        checks.check_types(run_description, [str, type(None)])
        checks.check_types(algorithm_parameters, dict)
        checks.check_types(output_dataset_name, [str, type(None)])
        checks.check_types(output_dataset_description, [str, type(None)])

        # database already in info
        if database is None:
            if self.info is None:
                raise KeyError(MISSING_PARAMETER.format(p="database"))
            else:
                database = self.info.origin

        # ensure dataset is in database
        found_ds = database.get_items_from_table(
            "Dataset", [["MD5", "=", self.md5],
                        ["SHA256", "=", self.sha256]])

        # not found
        if len(found_ds) == 0:
            ds = database.upload_dataset(self)
            self._reassign_self(ds)

        # database structure error
        elif len(found_ds) >= 2:
            raise ValueError(TOO_MANY_RETURN_VALUES.format(n=1))

        # apply to self
        ds = database.process(
            algorithm=algorithm,
            input_dataset=self,
            algorithm_name=algorithm_name,
            algorithm_description=algorithm_description,
            algorithm_version=algorithm_version,
            run_name=run_name,
            run_description=run_description,
            algorithm_parameters=algorithm_parameters,
            output_dataset_name=output_dataset_name,
            output_dataset_description=output_dataset_description)

        # reassign self
        self._reassign_self(ds)

    def _reassign_self(self, ds: "Dataset"):
        # Hidden function to reassign all properties of a dataset to the return
        # of another dataset. Primarily used after any approved malform dataset
        # operation.

        # enforce types
        checks.check_types(ds, Dataset)

        # reassign self
        self._info = ds.info
        self._introspector = ds.introspector
        self.name = ds.name
        self.description = ds.description
        self._annotations = ds.annotations
        self._md5 = ds.md5
        self._sha256 = ds.md5

    @property
    def graph(self):
        """
        While listed as a property, this produces a network graph detailing the
        changes a dataset has undergone and it's relatives. Primarly a wrapper
        around DatasetDatabase.display_dataset_graph function. Must be attached
        to a database to function properly.
        """

        # produce graph if attached
        if self.info is not None:
            self.info.origin.display_dataset_graph(self.info.id)
        else:
            return NO_DS_INFO

    @property
    def connections(self):
        """
        While listed as a property, this produces a DataFrame of network
        connections detailing the changes a dataset has undergone and it's
        relatives. Primarly a wrapper around
        DatasetDatabase.get_dataset_connections function. Must be attached to a
        database to function properly.
        """

        # produce connections if attached
        if self.info is not None:
            return self.info.origin.get_dataset_connections(self.info.id)
        else:
            return NO_DS_INFO

    def add_annotation(self, annotation: str):
        """
        Add an annotation to a dataset so that others know your notes regarding
        it. The dataset doesn't have to be attached to a database for this to
        work however if it is, the annotations also get sent to the database.

        No object is returned, the current object is updated to reflect the
        changes made (if any).


        #### Example
        ```
        >>> data.add_annotation(
        >>>     "Outdated, please refer to Dataset: 'QCB_features_new'.")

        ```

        #### Parameters
        ##### annotation: str
        Any annotation you would like to make on a dataset however it must be a
        string.


        ##### Returns


        ##### Errors

        """

        # enforce types
        checks.check_types(annotation, str)

        # add new annotation
        self._annotations.append(
            {"Value": annotation,
             "UserId": self.info.origin.user_info["UserId"],
             "Created": datetime.utcnow()})

        # send update is possible
        if self.info is not None:
            self.update_annotations()

    def update_annotations(self):
        """
        Update the annotations in the database with the ones stored on the
        dataset. Useful when collaborating and you believe your dataset is out
        of sync.

        No object is returned, the current object is updated to reflect the
        changes made (if any).


        #### Example
        ```
        >>> data.update_annotations()

        ```

        #### Parameters


        ##### Returns


        ##### Errors

        """

        # upload annotations missing ids
        for annotation in self.annotations:
            if "AnnotationId" not in annotation:
                anno_info = self.info.origin._insert_to_table(
                    "Annotation", annotation)
                self.info.origin._insert_to_table("AnnotationDataset", {
                    "AnnotationId": anno_info["AnnotationId"],
                    "DatasetId": self.info.id,
                    "Created": datetime.utcnow()
                })

    def save(self, path: Union[str, pathlib.Path]) -> pathlib.Path:
        """
        Save the dataset to a '.dataset' file that retains the underlying data
        object in full, as well as the dataset name, description, and the
        config to reconnect to a database when read from the file. This is
        useful if you are working with large datasets and you want to store the
        dataset so that you do not have to reconstruct it when you resume your
        work.

        "Why not just use csv?": The purpose of the objects laid out in this
        project has been rather clear which is that tracking, versioning, and
        deduplication are huge benefits to computational research. While this
        function does generate a file much like storing a dataframe to a csv,
        this file will allow you to store any object type while retaining
        enough information to allow for the usefullness I just described.
        Reconnection to a database and validating that the dataset has not been
        malformed alone is useful.


        #### Example
        ```
        >>> data.save("test_dataset")

        ```


        #### Parameters
        ##### path: str, pathlib.Path
        Where should the file be stored. Any folder in the path provided will
        be created as well.


        #### Returns
        ##### write_path: pathlib.Path
        The full resolved path of where the file was stored.


        ##### Errors

        """

        # enforce types
        checks.check_types(path, [str, pathlib.Path])

        # convert types
        path = pathlib.Path(path)

        # dump dataset
        path = path.with_suffix(".dataset")
        to_save = {"obj": self.ds,
                   "id": self.info.id,
                   "config": self.info.origin.config,
                   "user": self.info.origin.user,
                   "name": self.name,
                   "description": self.description}

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


def _return_dataset_obj(dataset):
    return dataset.ds


def _read_csv(path: pathlib.Path) -> Dataset:
    return Dataset(pd_read_csv(path))


def _read_dataset(path: pathlib.Path) -> Dataset:
    saved = tools.read_pickle(path)
    database = DatasetDatabase(saved["config"], user=saved["user"])
    ds_info = database.get_items_from_table(
        "Dataset", ["DatasetId", "=", saved["id"]])[0]
    ds_info["OriginDb"] = database
    ds_info = DatasetInfo(**ds_info)

    return Dataset(dataset=saved["obj"], ds_info=ds_info)


EXTENSION_MAP = {".csv": _read_csv,
                 ".dataset": _read_dataset}


def read_dataset(path: Union[str, pathlib.Path]) -> Dataset:
    """
    Read a dataset using deserializers known to DatasetDatabase. This function
    will properly handle ".csv" and our own ".dataset" files. Other support to
    come.

    #### Example
    ```
    >>> read_dataset("path/to/data.csv")
    {info: ...,
     ds: True,
     md5: "lakjdsfasdf9823h7yhkjq237",
     sha256: "akjsdf7823g2173gkjads7fg12321378bhfgasdf",
     ...}

    >>> read_dataset("path/to/data.dataset")
    {info: ...,
     ds: True,
     md5: "8hasdfh732baklhjsf",
     sha256: "87932bhksadkjhf78923klhjashdlf",
     ...}

    ```


    #### Parameters
    ##### path: str, pathlib.Path
    The filepath to read.


    #### Returns
    ##### dataset: Dataset
    The reconstructed and read dataset with it's database connection intact.


    #### Errors
    ##### ValueError
    The dataset could not be read.

    """

    # enforce types
    checks.check_types(path, [str, pathlib.Path])

    # convert types
    path = pathlib.Path(path)

    # attempt read
    try:
        if path.suffix in EXTENSION_MAP:
            return EXTENSION_MAP[path.suffix](path)

        ds = tools.read_pickle(path)
        return Dataset(ds)
    except pickle.UnpicklingError:
        raise ValueError(UNKNOWN_EXTENSION.format(p=path))


# delayed globals
LOCAL = {"driver": "sqlite",
         "database": str(pathlib.Path("./local.db").resolve())}
LOCAL = DatabaseConfig(LOCAL)
