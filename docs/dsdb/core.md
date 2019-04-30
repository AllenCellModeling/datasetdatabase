<h1 id="datasetdatabase.core">datasetdatabase.core</h1>


<h2 id="datasetdatabase.core.LOCAL">LOCAL</h2>


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


<h3 id="datasetdatabase.core.LOCAL.name">name</h3>

str(object='') -> str
str(bytes_or_buffer[, encoding[, errors]]) -> str

Create a new string object from the given object. If encoding or
errors is specified, then the object must expose a data buffer
that will be decoded using the given encoding and error handler.
Otherwise, returns the result of object.__str__() (if defined)
or repr(object).
encoding defaults to sys.getdefaultencoding().
errors defaults to 'strict'.
<h2 id="datasetdatabase.core.DatabaseConfig">DatabaseConfig</h2>

```python
DatabaseConfig(self, config:Union[str, pathlib.Path, Dict[str, str]], name:Union[str, NoneType]=None)
```

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


<h2 id="datasetdatabase.core.DatabaseConstructor">DatabaseConstructor</h2>

```python
DatabaseConstructor(self, config:Union[datasetdatabase.core.DatabaseConfig, NoneType]=None, schema:Union[datasetdatabase.schema.schemaversion.SchemaVersion, NoneType]=None, fms:Union[datasetdatabase.schema.filemanagers.fmsinterface.FMSInterface, NoneType]=None)
```

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


<h3 id="datasetdatabase.core.DatabaseConstructor.prepare_connection">prepare_connection</h3>

```python
DatabaseConstructor.prepare_connection(self)
```

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


<h3 id="datasetdatabase.core.DatabaseConstructor.create_schema">create_schema</h3>

```python
DatabaseConstructor.create_schema(self)
```

Create all the tables referenced by the SchemaVersion that was passed
in the DatabaseConfig constructor and fms attachment.


#### Example
```
>>> constructor.create_schema()

```


#### Parameters

#### Returns

#### Errors


<h3 id="datasetdatabase.core.DatabaseConstructor.build">build</h3>

```python
DatabaseConstructor.build(self)
```

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


<h3 id="datasetdatabase.core.DatabaseConstructor.get_tables">get_tables</h3>

```python
DatabaseConstructor.get_tables(self)
```

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


<h2 id="datasetdatabase.core.DatasetDatabase">DatasetDatabase</h2>

```python
DatasetDatabase(self, config:Union[datasetdatabase.core.DatabaseConfig, str, pathlib.Path, dict, NoneType]=None, user:Union[str, NoneType]=None, constructor:Union[datasetdatabase.core.DatabaseConstructor, NoneType]=None, build:bool=False, recent_size:int=5, processing_limit:Union[int, NoneType]=None)
```

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


<h3 id="datasetdatabase.core.DatasetDatabase.get_or_create_user">get_or_create_user</h3>

```python
DatasetDatabase.get_or_create_user(self, user:Union[str, NoneType]=None, description:Union[str, NoneType]=None)
```

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


<h3 id="datasetdatabase.core.DatasetDatabase.get_or_create_algorithm">get_or_create_algorithm</h3>

```python
DatasetDatabase.get_or_create_algorithm(self, algorithm:Union[method, function], name:Union[str, NoneType]=None, description:Union[str, NoneType]=None, version:Union[str, NoneType]=None)
```

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


<h3 id="datasetdatabase.core.DatasetDatabase.process">process</h3>

```python
DatasetDatabase.process(self, algorithm:Union[method, function], input_dataset:Union[_ForwardRef('Dataset'), NoneType]=None, input_dataset_info:Union[_ForwardRef('DatasetInfo'), NoneType]=None, input_dataset_id:Union[int, NoneType]=None, input_dataset_name:Union[str, NoneType]=None, algorithm_name:Union[str, NoneType]=None, algorithm_description:Union[str, NoneType]=None, algorithm_version:Union[str, NoneType]=None, run_name:Union[str, NoneType]=None, run_description:Union[str, NoneType]=None, algorithm_parameters:dict={}, output_dataset_name:Union[str, NoneType]=None, output_dataset_description:Union[str, NoneType]=None)
```

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


<h3 id="datasetdatabase.core.DatasetDatabase.upload_dataset">upload_dataset</h3>

```python
DatasetDatabase.upload_dataset(self, dataset:'Dataset', **kwargs) -> 'DatasetInfo'
```

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


<h3 id="datasetdatabase.core.DatasetDatabase.get_dataset">get_dataset</h3>

```python
DatasetDatabase.get_dataset(self, name:Union[str, NoneType]=None, id:Union[int, NoneType]=None) -> 'Dataset'
```

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


<h3 id="datasetdatabase.core.DatasetDatabase.preview">preview</h3>

```python
DatasetDatabase.preview(self, name:Union[str, NoneType]=None, id:Union[int, NoneType]=None) -> 'Dataset'
```

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


<h3 id="datasetdatabase.core.DatasetDatabase.get_items_from_table">get_items_from_table</h3>

```python
DatasetDatabase.get_items_from_table(self, table:str, conditions:List[Union[List[Union[bytes, str, int, float, NoneType, datetime.datetime]], bytes, str, int, float, NoneType, datetime.datetime]]=[])
```

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


<h2 id="datasetdatabase.core.DatasetInfo">DatasetInfo</h2>

```python
DatasetInfo(self, DatasetId:int, Name:Union[str, NoneType], Introspector:str, MD5:str, SHA256:str, Created:Union[datetime.datetime, str], OriginDb:datasetdatabase.core.DatasetDatabase, Description:Union[str, NoneType]=None)
```

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


<h2 id="datasetdatabase.core.Dataset">Dataset</h2>

```python
Dataset(self, dataset:object=None, ds_info:Union[datasetdatabase.core.DatasetInfo, NoneType]=None, name:Union[str, NoneType]=None, description:Union[str, NoneType]=None, introspector:Union[datasetdatabase.introspect.introspector.Introspector, str, NoneType]=None)
```

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


<h3 id="datasetdatabase.core.Dataset.connections">connections</h3>


While listed as a property, this produces a DataFrame of network
connections detailing the changes a dataset has undergone and it's
relatives. Primarly a wrapper around
DatasetDatabase.get_dataset_connections function. Must be attached to a
database to function properly.

<h3 id="datasetdatabase.core.Dataset.graph">graph</h3>


While listed as a property, this produces a network graph detailing the
changes a dataset has undergone and it's relatives. Primarly a wrapper
around DatasetDatabase.display_dataset_graph function. Must be attached
to a database to function properly.

<h3 id="datasetdatabase.core.Dataset.validate">validate</h3>

```python
Dataset.validate(self, **kwargs)
```

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


<h3 id="datasetdatabase.core.Dataset.store_files">store_files</h3>

```python
Dataset.store_files(self, **kwargs)
```

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


<h3 id="datasetdatabase.core.Dataset.upload_to">upload_to</h3>

```python
Dataset.upload_to(self, database:datasetdatabase.core.DatasetDatabase)
```

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


<h3 id="datasetdatabase.core.Dataset.apply">apply</h3>

```python
Dataset.apply(self, algorithm:Union[method, function], database:Union[datasetdatabase.core.DatasetDatabase, NoneType]=None, algorithm_name:Union[str, NoneType]=None, algorithm_description:Union[str, NoneType]=None, algorithm_version:Union[str, NoneType]=None, run_name:Union[str, NoneType]=None, run_description:Union[str, NoneType]=None, algorithm_parameters:dict={}, output_dataset_name:Union[str, NoneType]=None, output_dataset_description:Union[str, NoneType]=None)
```

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


<h3 id="datasetdatabase.core.Dataset.add_annotation">add_annotation</h3>

```python
Dataset.add_annotation(self, annotation:str)
```

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


<h3 id="datasetdatabase.core.Dataset.update_annotations">update_annotations</h3>

```python
Dataset.update_annotations(self)
```

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


<h3 id="datasetdatabase.core.Dataset.save">save</h3>

```python
Dataset.save(self, path:Union[str, pathlib.Path]) -> pathlib.Path
```

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


<h2 id="datasetdatabase.core.read_dataset">read_dataset</h2>

```python
read_dataset(path:Union[str, pathlib.Path]) -> datasetdatabase.core.Dataset
```

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


