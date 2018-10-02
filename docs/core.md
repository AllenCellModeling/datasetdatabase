<h1 id="datasetdatabase.core">datasetdatabase.core</h1>


<h2 id="datasetdatabase.core.LOCAL">LOCAL</h2>


Create a DatabaseConfig.

A DatabaseConfig is an object you can create before connecting to a
DatasetDatabase, but more commonly this object will be created by a
DatabaseConstructor in the process of connecting or building. A minimum
requirements config needs just "driver" and "database" attributes.

Example
==========
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

Parameters
==========
config: str, pathlib.Path, Dict[str: str]
    A string, or pathlib.Path path to a json file storing the
    connection config information. Or a dictionary of string keys and
    string values as a connection config.
name: str, None = None
    A specific name for this connection. If none is passed the name
    gets set to the value stored by the "database" key in the passed
    config.

Returns
==========
self

Errors
==========
AssertionError
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

Example
==========
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

Parameters
==========
config: str, pathlib.Path, Dict[str: str]
    A string, or pathlib.Path path to a json file storing the
    connection config information. Or a dictionary of string keys and
    string values as a connection config.
name: str, None = None
    A specific name for this connection. If none is passed the name
    gets set to the value stored by the "database" key in the passed
    config.

Returns
==========
self

Errors
==========
AssertionError
    One or more of the required config attributes are missing from the
    passed config.


<h2 id="datasetdatabase.core.DatabaseConstructor">DatabaseConstructor</h2>

```python
DatabaseConstructor(self, config:Union[datasetdatabase.core.DatabaseConfig, NoneType]=None, schema:Union[datasetdatabase.schema.schemaversion.SchemaVersion, NoneType]=None, fms:Union[datasetdatabase.schema.filemanagers.fmsinterface.FMSInterface, NoneType]=None)
```

DatabaseConstructor is an object that will handle the construction,
teardown, and reconnection of a DatasetDatabase to the passed
DatabaseConfig. It will also manage the construction, teardown, and
reconnection of the passed FMSInterface.

<h3 id="datasetdatabase.core.DatabaseConstructor.prepare_connection">prepare_connection</h3>

```python
DatabaseConstructor.prepare_connection(self)
```

Prepare a database connection by asserting required attributes are
present in the config passed. If the database link is a file, enforce
that the file and all parent directories exist.

Example
==========
```
>>> good_config.prepare_connection()


>>> bad_config.prepare_connection()
AssertionError: "Local databases must have suffix '.db'"

```

Parameters
==========

Returns
==========

Errors
==========
AssertionError
    The local database link is not the appropriate file type (.db).


<h3 id="datasetdatabase.core.DatabaseConstructor.create_schema">create_schema</h3>

```python
DatabaseConstructor.create_schema(self)
```

Create all the tables referenced by the SchemaVersion that was passed
in the DatabaseConfig constructor and fms attachment.

Example
==========
```
>>> constructor.create_schema()

```

Parameters
==========

Returns
==========

Errors
==========


<h3 id="datasetdatabase.core.DatabaseConstructor.build">build</h3>

```python
DatabaseConstructor.build(self)
```

Connect to a database and build the tables found in the SchemaVersion
passed to the DatabaseConstructor initialization.

This is mainly a wrapper around the prepare_connection and create_schema
functions that additionally returns the orator.DatabaseManager object
created.

Example
==========
```
>>> constructor.build()

```

Parameters
==========

Returns
==========
db: orator.DatabaseManager
    A constructed database manager object that can be used to fully
    interact with the database, but additionally, all the tables have
    been stored in the constructor.tables attribute.

Errors
==========


<h3 id="datasetdatabase.core.DatabaseConstructor.get_tables">get_tables</h3>

```python
DatabaseConstructor.get_tables(self)
```

In the case that a database is already fully constructed, a get_tables
operation should be run so that the table map is fully up-to-date
without overwriting or constructing useless tables.

Example
==========
```
>>> constructor.get_tables()

```

Parameters
==========

Returns
==========
db: orator.DatabaseManager
    A constructed database manager object that can be used to fully
    interact with the database, but additionally, all the tables have
    been stored in the constructor.tables attribute.

Errors
==========


<h2 id="datasetdatabase.core.DatasetDatabase">DatasetDatabase</h2>

```python
DatasetDatabase(self, config:Union[datasetdatabase.core.DatabaseConfig, str, pathlib.Path, dict, NoneType]=None, user:Union[str, NoneType]=None, constructor:Union[datasetdatabase.core.DatabaseConstructor, NoneType]=None, build:bool=False, recent_size:int=5, processing_limit:Union[int, NoneType]=None)
```

DatasetDatabase is the primary object you will use to do large scale
interactions with the database. More common interactions are largely
handled by the Dataset object however some operations can be focused or
targeted specifically at a specific database. Additionally, there are many
admin tools and features on the DatasetDatabase object compared to the
Dataset object.

<h3 id="datasetdatabase.core.DatasetDatabase.get_or_create_user">get_or_create_user</h3>

```python
DatasetDatabase.get_or_create_user(self, user:Union[str, NoneType]=None, description:Union[str, NoneType]=None) -> Dict[str, Union[bytes, str, int, float, NoneType, datetime.datetime]]
```

Get or create a user in the database. This function is largely a
wrapper around the insert to table functionality in that it gets or
inserts whichever user and description is passed but additionally
updates the user and user_info attributes of the DatasetDatabase object.

Example
==========
```
>>> db.get_or_create_user("jacksonb")
{"UserId": 1,
 "Name": "jacksonb",
 "Description": None,
 "Created": 2018-09-28 16:39:09}

```

Parameters
==========
user: str, None = None
    What is the user name of the person you want to add to the database.
    If None provided, the user is determined by db.user.
description: str, None = None
    What is the description of the person you want to add to the
    database. If None provided, no description is given.

Returns
==========
user_info: dict
    A dictionary of the row found or created detailing the user.

Errors
==========
ValueError
    Too many rows returned from the database when expected only one or
    zero rows returned. This indicates something is drastically wrong
    with the database.


<h3 id="datasetdatabase.core.DatasetDatabase.get_or_create_algorithm">get_or_create_algorithm</h3>

```python
DatasetDatabase.get_or_create_algorithm(self, algorithm:Union[method, function], name:Union[str, NoneType]=None, description:Union[str, NoneType]=None, version:Union[str, NoneType]=None) -> Dict[str, Union[bytes, str, int, float, NoneType, datetime.datetime]]
```

Get or create an algorithm in the database. This function is largely a
wrapper around the insert to table functionality in that it gets or
inserts whichever algorithm name, description, and version is passed
but additionally if none are passed tried to detect or create the
parameters.

Example
==========
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

Parameters
==========
algorithm: types.MethodType, types.FunctionType
    Any python method of function that you want to use in processing a
    dataset.
name: str, None = None
    A name for the algorithm as it should be stored in the database. If
    None provided, the name is stored as the function name that was
    passed.
description: str, None = None
    A description for the algorithm as it should be stored in the
    database. If None provided, the description is a standard string
    created that details who originally added the algorithm to the
    database.
version: str, None = None
    A version for the algorithm. If None provided, there is an attempt
    to determine git commit hash of the code.

Returns
==========
user_info: dict
    A dictionary of the row found or created detailing the user.

Errors
==========
ValueError
    The version could not be determined through git hash and no version
    parameter was passed.


<h3 id="datasetdatabase.core.DatasetDatabase.process">process</h3>

```python
DatasetDatabase.process(self, algorithm:Union[method, function], input_dataset:Union[_ForwardRef('Dataset'), NoneType]=None, input_dataset_info:Union[_ForwardRef('DatasetInfo'), NoneType]=None, input_dataset_id:Union[int, NoneType]=None, input_dataset_name:Union[str, NoneType]=None, algorithm_name:Union[str, NoneType]=None, algorithm_description:Union[str, NoneType]=None, algorithm_version:Union[str, NoneType]=None, run_name:Union[str, NoneType]=None, run_description:Union[str, NoneType]=None, algorithm_parameters:dict={}, output_dataset_name:Union[str, NoneType]=None, output_dataset_description:Union[str, NoneType]=None) -> 'Dataset'
```

This is largely the core function of the database as most other
functions in some way are passed through this function as to retain
information regarding how datasets change and are manipulated. However
as a standalone function you can pass a processing function or method
through this to pull and apply to a dataset before returning the
results back. It is recommended however that if you want to use the
above described behavior to use a `dataset.apply` command.

Example
==========
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

Parameters
==========
algorithm: types.MethodType, types.FunctionType
    Any python method of function that you want to use in processing a
    dataset.
input_dataset: Dataset, None = None
    Which dataset to apply the algorithm to.
input_dataset_id: int, None = None
    Which dataset to pull before applying the algorithm to.
input_dataset_info: DatasetInfo, None = None
    Which dataset to pull before applying the algorithm to.
input_dataset_name: str, None = None
    Which dataset to pull before applying the algorithm to.
algorithm_name: str, None = None
    A name for the algorithm as it should be stored in the database. If
    None provided, the name is stored as the function name that was
    passed.
algorithm_description: str, None = None
    A description for the algorithm as it should be stored in the
    database. If None provided, the description is a standard string
    created that details who originally added the algorithm to the
    database.
algorithm_version: str, None = None
    A version for the algorithm. If None provided, there is an attempt
    to determine git commit hash of the code.
run_name: str, None = None
    A name for this specific run. Usually left blank but if a specific
    run is rather important and you want to easily find it later you
    can detail a name here.
run_description: str, None = None
    A description for this specific run. Usually left blank but if a
    specific run is rather important and you need more than just a run
    name you can detail a run description here.
algorithm_parameters: dict = {}
    A dictionary of parameters that get passed to the algorithm. The
    dictionary gets expanded when passed to the function so the
    parameters become keyword arguments.
output_dataset_name: str, None = None
    A name for the produced dataset.
output_dataset_description: str, None = None
    A description for the produced dataset.

Returns
==========
output: Dataset
    A dataset of the produced containing the results from applying the
    passed application.

Erros
==========
AssertionError
    Missing parameters, must provided at least one of the various
    parameter option sets.
ValueError
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

Example
==========
```
>>> data = Dataset("/path/to/some/data.csv")
>>> db.upload_dataset(data)

```

Parameters
==========
dataset: Dataset
    The dataset object ready for ingestion to a database.

Returns
==========
dataset: Dataset
    The same dataset object post ingestion with a DatasetInfo block
    attached.

Errors
==========
AssertionError
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

Example
==========
```
>>> db.get_dataset("Label Free Images")
{info: ...}

>>> db.get_dataset("This dataset doesn't exist")
ValueError: Dataset not found using parameters...

```

Parameters
==========
name: str, None = None
    The name of the dataset you want to reconstruct.
id: int, None = None
    The id of the dataset you want to reconstruct.

Returns
==========
dataset: Dataset
    A reconstructed Dataset with attached DatasetInfo block.

Errors
==========
AssertionError
    Missing parameter, must provided either id or name.
ValueError
    Dataset not found using the provided id or name.
ValueError
    Malformed database error, too many values returned from a query
    expecting a single value or no value to return.


<h3 id="datasetdatabase.core.DatasetDatabase.preview">preview</h3>

```python
DatasetDatabase.preview(self, name:Union[str, NoneType]=None, id:Union[int, NoneType]=None) -> 'Dataset'
```

Pull and create summary info about a dataset from the database. Must
provided either a dataset name or a dataset id to retrieve the dataset.

Example
==========
```
>>> db.preview("Label Free Images")
{info: ...}

>>> db.preview("This dataset doesn't exist")
ValueError: Dataset not found using parameters...

```

Parameters
==========
name: str, None = None
    The name of the dataset you want to preview.
id: int, None = None
    The id of the dataset you want to preview.

Returns
==========
preview: Dataset
    A dictionary with summary info about a dataset that contains things
    like the DatasetInfo block, the shape, columns/ keys, and any
    annotations.

Errors
==========
AssertionError
    Missing parameter, must provided either id or name.
ValueError
    Dataset not found using the provided id or name.
ValueError
    Malformed database error, too many values returned from a query
    expecting a single value or no value to return.


<h3 id="datasetdatabase.core.DatasetDatabase.get_items_from_table">get_items_from_table</h3>

```python
DatasetDatabase.get_items_from_table(self, table:str, conditions:List[Union[List[Union[bytes, str, int, float, NoneType, datetime.datetime]], bytes, str, int, float, NoneType, datetime.datetime]]=[]) -> List[Dict[str, Union[bytes, str, int, float, NoneType, datetime.datetime]]]
```

Get items from a table that match conditions passed. Primarily a
wrapper around orator's query functionality.

Example
==========
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

Parameters
==========
table: str
    Which table you want to get items from.
conditions: List[Union[List[GENERIC_TYPES], GENERIC_TYPES]]
    A list or a list of lists containing generic types that the database
    can construct where conditions from. The where conditions are
    AND_WHERE conditions, not OR_WHERE.

Returns
==========
results: List[dict]
    A list of dictionaries containing all the items found that match
    the conditions passed.

Errors
==========


<h2 id="datasetdatabase.core.DatasetInfo">DatasetInfo</h2>

```python
DatasetInfo(self, DatasetId:int, Name:Union[str, NoneType], Introspector:str, MD5:str, SHA256:str, Created:Union[datetime.datetime, str], OriginDb:datasetdatabase.core.DatasetDatabase, Description:Union[str, NoneType]=None)
```

DatasetInfo is an object used to limit limit how users manipulate datasets
that have a connection to a database. If a dataset already has a connection
to a database this object is used to check the md5 and sha256 against in
order to verify that it has not been malformed.

