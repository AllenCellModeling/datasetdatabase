immutable# Dataset Ingestion

#### Index:
1. [Creating a Dataset](#dataset-creation)

    1. [Reading DataFrame Like](#dataframe-like)
    2. [From Serialization](#import-dataset)
    3. [Merging Datasets](#merging-datasets)
    4. [Custom Object Ingestion](#custom-objects)

2. [Dataset Validation](#dataset-validation)

    1. [Data Type](#data-type-validation)
    2. [Data Value](#data-value-validation)
    3. [File Versioning](#file-checking-and-versioning)

3. [Database Ingestion](#dataset-ingestion)

    1. [Connecting to a Database](#database-connection)
    2. [Uploading to a Database](#dataset-upload)
    3. [Minimal Step Ingestion](#minimal-step-ingestion)

## Dataset Creation
Datasets can be created by providing a path to a a csv, or tsv file, by providing the dataset as a [`pandas.DataFrame`](https://pandas.pydata.org), by reading a serialization created by this package (`.dataset` or a [Quilt](https://quiltdata.io) package), or finally, by joining Datasets based off an Iota found in both (or all). Additionally, you can create a custom deserializer and pass any object to the Dataset initialization. This is more complicated however drastically expands the types of data that can be handled by the system.

### DataFrame Like
Data formats like csv, tsv, and `pandas.DataFrame` were the most common for data scientists to use when this package was under original development. Because of this, they are all handled natively.

Datasets are a relatively simply object that largely is a wrapper around a pandas DataFrame object but they additionally some metadata about the state of the Dataset in relation to the database. Let's create a Dataset object in the simplest sense and then go through the state.

```python
import datasetdatabase as dsdb

ds_path = "path/to/test_dataset.csv"
ds = dsdb.Dataset(ds_path)
ds
```

    info: None,
    obj: True,
    name: "test_dataset",
    description: None,
    filepath_columns: None,
    validated: {
        types: False,
        values: False,
        files: False
    }

```python
ds.obj
```
||foo|bar|
|--|--|--|
|1|"hello"|True|
|2|"world"|False|

As you can see from the above representation (`repr`) of the Dataset, in the case of a dataframe like object, a Dataset will be created and the minimum validation fields are created but not validated yet. The minimum validation requirements for dataframe like objects are filepath_columns, and validated types, values, and files. There is additionally an "info" attribute, this is what stores the details about a Dataset and how it relates to a database. If this Dataset was pulled from a database, the info field would be populated with a DatasetInfo object. Of note: if there is a DatasetInfo item attached to the info attribute, all other attributes become immutable as that would break the usage of the database. Do not, malform, mutate, or change a Dataset's underlying object data. If you do, the purpose of this package to track, version, deduplicate, and store your data is moot. There are checks in place to detect if a dataset has been malformed in between creation/ ingestion, getting, and processing that will throw errors if differences are found.

### Import Dataset
Due to the size that Datasets can become, and the desire to not have to connect, pull, and rebuild a Dataset object after every break in work. There is a simple pickle serialization of the Dataset object available.

```python
ds.save("stored/datasets/1.dataset")
```

This allows you to then read in the Dataset with it's full object in memory as well as, and importantly, it's database connection intact.

```python
ds = dsdb.read_dataset("stored/datasets/1.dataset")
ds
```

    info: None,
    obj: True,
    name: "test_dataset",
    description: None,
    filepath_columns: None,
    validated: {
        types: False,
        values: False,
        files: False
    }

### Merging Datasets
In certain situations, you can take advantage of how data is stored in the database by attaching metadata or unique identifiers to items in your object.

In the case of multiple dataframe like objects that share data across them you could attach a GUID or some other identifier to each shared data point.

***Shared Data Points Example:*** Same image, multiple datasets, different features.

*Cell Volume Dataset:* stored in database as Dataset 1
|  | cell_id | cell_image | feature_cell_volume |
|---|----------------------------------------|-------------------------------|---------------------|
| 1 | "8f2dcec5-3d3d-407a-85c7-52e7583156aa" | "path/to/image/of/cell/1.png" | 3.20948 |
| 2 | "1e93d4f6-7e34-4ca9-9c21-d6062b2cd7fe" | "path/to/image/of/cell/2.png” | 12.9723 |

*Cell Height Dataset:* stored in database as Dataset 2
|  | cell_id | cell_image | feature_cell_height |
|---|----------------------------------------|-------------------------------|---------------------|
| 1 | "8f2dcec5-3d3d-407a-85c7-52e7583156aa" | "path/to/image/of/cell/1.png" | 1.84235 |
| 2 | "1e93d4f6-7e34-4ca9-9c21-d6062b2cd7fe" | "path/to/image/of/cell/2.png” | 6.12739 |

You can then join these Datasets by simply merging by `cell_id`.
```python
ds = database.create_dataset(join=["cell_id"], datasets=[1, 2])
ds
```

    info: DatasetInfo,
    obj: True,
    name: "merge_1_2",
    description: "created dataset using 'cell_id' found in datasets [1, 2]",
    filepath_columns: "cell_image",
    validated: {
        types: True,
        values: True,
        files: True
    }



### Custom Objects
While dataframe like datasets were a large priority when the initial builds of DSDB were being worked on, the ability to expand the capabilities of the system were called for as well. Why limit the system to a single format of data. The versioning and tracking of data itself is valuable. Due to this, custom object introspection has been added to Dataset creation.

A simple example of this in practice could be that you want to run a single image through a classifier.

***Example:*** `path/to/unclassified/image.png`

You could form this into a dataframe like Dataset by simply putting it into a single column, single row dataframe, but that is just a roundabout way of getting to what you want.

***Example in Dataframe:***
|  | unclassified_images |
|---|----------------------------------|
| 1 | "path/to/unclassified/image.png" |

Instead you can write a custom [Introspector](../datasetdatabase/introspect/introspector.py) for that object that informs the Dataset validation, deconstruction, and reconstruction of that object. You can view the [pandas DataFrame Introspector](../datasetdatabase/introspect/dataframe.py) as an example, but there is no limit to what custom Introspector you would like to use. Deconstruction is important during converting the object from it's standard form to a storable form in the database. Validation is used directly by the Dataset object and informs the object on whether or not the object is valid to be pushed into a database. And lastly, is reconstruction, which is the reverse operation of deconstruction. It informs the database on how to recreate the original object given its mapping of Iota, IotaGroup, and Group.

A more complex example is a Dataset of Datasets. There is occasionally a need for this due to an algorithm requiring multiple inputs and while it is encouraged to find a way to format your multiple datasets into a single source that can then act as the single source of truth, it is understandable that in some cases you may not want to do so.

***Dataset of Datasets Example:*** Specific models against specific items in a dataset.

*Image Data Dataset:* stored Dataset object with an attached DatasetInfo
|  | unclassified_images |
|---|----------------------------------|
| 1 | "path/to/unclassified/image.png" |

*Models Available Dataset:* stored Dataset object with an attached DatasetInfo
|  | path_to_serialized_model |
|---|--------------------------|
| 1 | "path/to/model/1.pkl" |
| 2 | "path/to/model/2.pkl" |

*Dataset of Datasets:*
|  | dataset |
|---|----------------|
| 1 | images_dataset |
| 2 | models_dataset |

***Expanded Single Dataset Example:*** Specific models against specific items in a dataset.

||unclassified_images|apply_model_1|apply_model_2|
|--|--|--|--|
|1|"path/to/unclassified/image.png"|"path/to/model/1.pkl"|"path/to/model/2.pkl"|

There are benefits to both systems. In a Dataset of Datasets system your DatasetInfo objects are attached and thus you can pull data from multiple databases at the same time as well as ensure that everything is validated regardless of system. However on the expanded single dataset system, it is it's own Dataset, and regardless of other data, it will be locked to that version that will be easily accessible to anyone who wants to view how both the data and the models were prepared.

## Dataset Validation
There is inherent data validation when you attempt to push data to a Database or before if you choose to. The following examples of such are examples of dataframe like data being validated as that is again, the assumed, default data type. Given a custom object and Introspector your validation method can be completely different. For example if you have a single image as a "Dataset" you can write a validation function that determines if the image is the correct shape, is too bright, etc.

### Data Type Validation
For dataframe like Datasets you can validate the types of each data point by simply passing a `type_validation_map` parameter.

If our underlying data is as follows:
|  | foo | bar |
|---|---------|-------|
| 1 | "hello" | True |
| 2 | "world" | False |

A reasonable `type_validation_map` could be:

    "foo": str
    "bar": bool

Where the keys are the column names and the values are the data types. You can pass multiple types in a list or tuple for example if you have filepaths in mixed string and pathlib formats your `type_validation_map` could include the following:

    "files": (str, pathlib.Path)

If you want to validate your data types before attempting to ingest you can simply run: `ds.validate()` where ds is the Dataset object needing validation.

If you want to validate a specific subsection, in this case, data types, you would need to look into the Introspector assigned to your Dataset. `ds.introspector.validate_types()` for dataframe like Datasets.

You can also use the `import_as_type_map=True` parameter to attempt to cast each value to the type in the `type_validation_map` if it's type does not match the one found in the `type_validation_map`.

### Data Value Validation
Similarly, for dataframe like Datasets you can validate the values of each data point by simply passing a `value_validation_map` parameter.

If our underlying data is as follows:
|  | foo | bar |
|---|---------|-------|
| 1 | "hello" | True |
| 2 | "world" | False |

A reasonable `value_validation_map` could be:

    "foo": lambda x: len(x) >= 5

Where the keys are the column names and the values are functions (lambda, functions, methods, or modules). You can pass multiple functions in a list or tuple if you need multi-step value validation your `value_validation_map` could include the following:

    "floats": (lambda x: x >= 3, lambda x: x <= 12)

The above example could be condensed to a single function but is a simple example for showing multi-step validation.

If you want to validate your data values before attempting to ingest you can simply run: `ds.validate()` where ds is the Dataset object needing validation.

If you want to validate a specific subsection, in this case, data values, you would need to look into the Introspector assigned to your Dataset. `ds.introspector.validate_values()` for dataframe like Datasets.

### File Checking and Versioning
Files are more complex than data type and value checking. In the case where a Dataset contains paths to files, it is recommended to inform the Dataset of such.

Given a dataset as follows:
|  | foo | bar | files |
|---|---------|-------|----------------------|
| 1 | "hello" | True | "path/to/file/1.png" |
| 2 | "world" | False | "path/to/file/2.png" |

We can indicate to the Dataset initialization that the "files" column contains, well, filepaths. Simply pass a single string for a single column, or a list or tuple for multiple columns. `filepath_columns=["files"]`

This will begin a few operations. The first of which is that it will enforce that all files found in the columns specified exist. The second of which is that it will create versioned, and deduplicated versions of those files using whichever [FMS module](./fms.md) you are using (By default this is a Quilt FMS). This is incredibly beneficial if you think the files found in the dataset may change, move, be deleted, etc. This not only ensures that your dataset will be an immutable object but also the files contained in the dataset.

After passing this parameter you will see your filepaths have changed to be the read paths from the FMS created files.
|  | foo | bar | files |
|---|---------|-------|-----------------------------------------|
| 1 | "hello" | True | "quilt/store/objs/8912hujqds78fh122uas" |
| 2 | "world" | False | "quilt/store/objs/a89qhjoiuahsd89fhnai" |

If you want to turn this behavior off simply include `store_files=False`, but this is highly discouraged as there is no guarantee of an imutable Dataset without this.

Like the other default dataframe like validation methods this can be step can be run by using the function `ds.validate()` where ds is the Dataset object needing validation.

If you want to validated a specific subsection, in this case, filepaths, you would need to look into the Introspector assigned to your Dataset. `ds.introspector.validate_files()` for dataframe like Datasets.

**Note:**
Lastly, if no validation parameters are found in a Dataset initialization or ingestion, it is assumed that all types, values, and files are correct. The same occurs for custom Introspectors.

## Database Ingestion
There are a few more things needed to know about Dataset ingestion, mainly, how to connect and upload.

### Database Connection
Assuming an admin has already set up a Dataset Database for you this is simple, if you are the admin looking to set up a Database please read the [database creation documentation](./constructor.md).

To connect to a DSDB you will simply need the DatabaseConfig, or a json representation of it.
```python
production_db = dsdb.DatasetDatabase(config="path/to/config.json")
production_db
```

```
Recent Datasets:
--------------------------------------------------------------------------------
```

That's it.

Most of the other functionality of creating and managing Dataset Databases can be read at the [database documentation](./constructor.md).

### Dataset Upload
Now that you have a database connection created you have two options for upload.

The first is an upload that is aimed at the database.

`production_db.ingest(ds)` where ds is the Dataset object that should be uploaded.

The second is an upload that is aimed at the dataset, using this you can send a single dataset to multiple databases.

`ds.upload_to(production_db)` or `ds.upload_to((production_db, external_db))`

This is useful when you may want to work in multiple databases. For example you want to share your work with collaborators but keep a local copy of each Dataset yourself.

### Minimal Step Ingestion
To wrap up, while it's not recommended, all of this functionality can be incredibly reduced, to a single line that will infer most of the work for you. Instead of building and validating a Dataset object you can simply do the following.

`production_db.ingest("data.csv")` this will assume the data is already validated, will not deduplicate files, etc. However for the sake of transparency, this functionality does exist but is highly discouraged.

---

That about wraps up all the information about Dataset creation and ingestion. Questions and issues can be pointed at the [project repository issues tracker](https://github.com/AllenCellModeling/datasetdatabase/issues).

**Last updated: 28 August 2018**
