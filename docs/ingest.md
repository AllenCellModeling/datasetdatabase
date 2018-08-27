# Dataset Ingestion
**Last updated: 21 August 2018**

#### Index:
1. [Creating a Dataset](#dataset-creation)

    1. [Reading DataFrame Like](#dataframe-like)
    2. [From Serialization](#import-from-external)
    3. [Joining Datasets](#dataset-joins)

2. [Dataset Validation](#dataset-validation)

    1. [Data Type](#data-type-validation)
    2. [Data Value](#data-value-validation)
    3. [File Versioning](#file-checking-and-versioning)

3. [Database Ingestion](#dataset-ingestion)

    1. [Connecting to a Database](#database-connection)
    2. [Uploading to a Database](#dataset-upload)
    3. [Minimal Step Ingestion](#minimal-step-ingestion)

## Dataset Creation
Datasets can be created by providing a path to a a csv, or tsv file, by providing the dataset as a [`pandas.DataFrame`](https://pandas.pydata.org), by reading a serialization created by this package (`.dataset` or a [Quilt](https://quiltdata.io) package), or finally, by joining datasets based off an Iota found in both (or all).

### Reading DataFrame Like
Data formats like csv, tsv, and `pandas.DataFrame` were the most common for data scientists to use when this package was under original development. Because of this, they are all handled natively.

```python
import datasetdatabase as dsdb

ds_path = "path/to/test_dataset.csv"
ds = dsdb.Dataset(ds_path)
ds
```
    info: None,
    df: True,
    name: "test_dataset",
    filepath_columns: None,
    validated: {
        types:
        files: False
    }

```python
ds.df
```
||foo|bar|
|--|--|--|
|1|"hello"|True|
|2|"world"|False|
