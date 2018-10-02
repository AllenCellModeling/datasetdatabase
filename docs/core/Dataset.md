<h1 id="datasetdatabase.core.Dataset">Dataset</h1>

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


<h2 id="datasetdatabase.core.Dataset.connections">connections</h2>


While listed as a property, this produces a DataFrame of network
connections detailing the changes a dataset has undergone and it's
relatives. Primarly a wrapper around
DatasetDatabase.get_dataset_connections function. Must be attached to a
database to function properly.

<h2 id="datasetdatabase.core.Dataset.graph">graph</h2>


While listed as a property, this produces a network graph detailing the
changes a dataset has undergone and it's relatives. Primarly a wrapper
around DatasetDatabase.display_dataset_graph function. Must be attached
to a database to function properly.

<h2 id="datasetdatabase.core.Dataset.validate">validate</h2>

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
All arguments given will be passed to the objects introspector validate.


#### Returns


#### Errors


<h2 id="datasetdatabase.core.Dataset.store_files">store_files</h2>

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


<h2 id="datasetdatabase.core.Dataset.upload_to">upload_to</h2>

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


<h2 id="datasetdatabase.core.Dataset.apply">apply</h2>

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


<h2 id="datasetdatabase.core.Dataset.add_annotation">add_annotation</h2>

```python
Dataset.add_annotation(self, annotation:str)
```

Add an annotation to a dataset so that others know your notes regarding
it. The dataset doesn't have to be attached to a database for this to
work however if it is, the annotations get sent to the database as well.

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


<h2 id="datasetdatabase.core.Dataset.update_annotations">update_annotations</h2>

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


<h2 id="datasetdatabase.core.Dataset.save">save</h2>

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


