# DatasetDatabase
DatasetDatabase is a python tool for creating, pushing, and pulling datasets. This tool is intended to be used to share data amongst data scientists whom have diverse data sharing needs. In short, DSDB is a collection of basic practices for sharing data, enforced by python, with data stored in a database of your choosing.

## Documentation
[https://allencellmodeling.gitbook.io/datasetdatabase/](https://allencellmodeling.gitbook.io/datasetdatabase/)

## Quickstart Guide

#### Index:
0. [Introduction](#Introduction)
1. [Setting up your credentials](#Credentials)
2. [Connect to DSDB](#Connect-to-DSDB)
3. [Getting data](#Getting-data)
    1. [Browsing existing datasets](#Browsing-existing-datasets)
    2. [Pulling data](#Pulling-data)
4. [Pushing data](#Pushing-data)
5. [Help us help you](#help-us-help-you)

### Introduction
A dataset can be any collection of the normal data science stuff, usually things in a spreadsheet and files (sort of like what you would find in a `pandas.DataFrame`). First you need to install the software:

#### Install
```bash
pip install git+https://github.com/AllenCellModeling/datasetdatabase.git
```

#### Reinstall (useful if Jackson changes anything)
```bash
pip uninstall datasetdatabase
pip install git+https://github.com/AllenCellModeling/datasetdatabase.git
```

### Credentials
Credentials are important. Make a file that looks like this and put it somewhere. Generally, we put these in our home
directories, like so: `~/.config_dsdb.json`.

```json
{
   "driver": "postgres",
   "host": "<hostname>",
   "database": "<database_name>",
   "user": "<username>",
   "password": "<password>"
}
```

If you don't know what is supposed to go in there, go ask Jackson.

### Connect to a database
After you set up your credentials, in Python you can get a dsdb connection object as follows:
```python
import datasetdatabase as dsdb  
my_database = dsdb.DatasetDatabase(config='~/.config_dsdb')
```

### Getting data
### Browsing existing datasets
To see recently uploaded datasets you can simply request the representation of the object:
```python
my_database

# Or use a print
# print(my_database)
```

To see all the datasets available in a database, you can do:
```python
my_database.get_items_from_table('Dataset')
```

You will see something like:  
```python
[{"DatasetId": 1,  
"Name": "deba767d-4c3a-483c-a0bc-66ecf7ad4e1b",  
"Description": "algorithm parameters",  
"Introspector": "datasetdatabase.introspect.dictionary.DictionaryIntrospector",  
"MD5": "d34b2ff3ebd47dff1d21eee2d69d4ffd",  
"SHA256": "08e14d71fb56a8d6888e9b1f0c21d1a98050e7cdf034fdfcd06d0db59f810477",  
"Created": datetime.datetime(2018, 9, 28, 21, 22, 39, 745444)},
{"DatasetId": 2,  
"Name": "QCB_TOM20_feature_old",  
"Description": None,  
"Introspector": "datasetdatabase.introspect.dataframe.DataFrameIntrospector",  
...
```


To preview what is in a dataset, take whatever number is after the "DatasetID" and plug it in like:
```python
my_database.preview(id=3)
```

You'll see the preview:
```python
{'info': {'id': 3, 'name': 'MitoData: Hidden 20180917', 'description': 'Mitosis Hidden Validation Data for Mito-classification', 'introspector': 'datasetdatabase.introspect.dataframe.DataFrameIntrospector', 'created': datetime.datetime(2018, 9, 28, 21, 23, 16, 75313)},
'shape': (1138, 38),
...
'source_data',
'structureChannel',
'structureProteinName',
'timePoint',
'uuid',
'uuid_short',
'well_ID'],
'annotations': []}
 ```

### Pulling Data
Once you have found a dataset you like, you can pull it with:
```python
my_dataset = my_database.get_dataset(id={id})
```

This returns a `Dataset` object which contains metadata about the dataset:
```python
{info: {'id': 3, 'name': 'MitoData: Hidden 20180917', 'description': 'Mitosis Hidden Validation Data for Mito-classification', 'introspector': 'datasetdatabase.introspect.dataframe.DataFrameIntrospector', 'created': datetime.datetime(2018, 9, 28, 21, 23, 16, 75313)}
ds: True
name: MitoData: Hidden 20180917
description: Mitosis Hidden Validation Data for Mito-classification
introspector: <class 'datasetdatabase.introspect.dataframe.DataFrameIntrospector'>
md5: 70ff8d4fbd75305465ddab70b4cf18bf
sha256: 3c75693977b436e5580569f50ce419004c832f66378989dbf22462b83fbd5683
validated: True
annotations: []
}
```

You can get to the actual data portion, whether it is a `pandas.DataFrame` or python `dictionary` with:
```python
my_dataframe = my_dataset.ds
```

### Pushing data
Datasets can be created by providing a path to a csv, by providing the dataset as a `pandas.DataFrame`, or a `dictionary`. There are other ways to do this, but you'll have to read the documentation for that.

Lets say I have data in a `.csv` file on disk, load it into a `dataframe` and make a dataset, or directly load it from the file path:
```python
import datasetdatabase as dsdb
import pandas as pd

ds_path = "path/to/test_dataset.csv"
ds = dsdb.Dataset(ds_path,
                  name="My dataset from a csv",
                  description="Some neat data")

ds_df = pd.read_csv(ds_path)
ds = dsdb.Dataset(ds_df,
                  name="My dataset from a pandas dataframe",
                  description="Some neat data")
```

Then you can specify a schema to tell it what type each column is. You can even specify that a column is a bunch of files, and DSDB will check it for you. The `import_as_type_map` variable will force the column to be that data type if it is `True`.
```python

type_validation_map = {
    'column_name_one': str,
    'column_name_two': int,
    'column_name_etc': float
}

filepath_columns = ['column_name_files1', 'column_name_files2']

import_as_type_map = True

ds.validate(type_validation_map = type_validation_map,
            filepath_columns = filepath_columns,
           import_as_type_map = import_as_type_map)
```

Then you can upload it with your connection to dsdb:

```python

my_database = dsdb.DatasetDatabase(config='~/.config_dsdb')
ds = my_database.upload_dataset(ds)

```

If you wanted to be really specific about what values are also in your dataset, you can additionally provide `lambda` functions that check you dataset values:
```python
ds.validate(value_validation_map={
    "column_of_ints_or_floats": lambda x: x >= 0,
    "column_of_strings": lambda x: "this substring must be" in x
})
```

**Note:** Think that uploading data takes a while? Increase the workers allowed to push data from your machine:
```python
my_database = dsdb.DatasetDatabase(config='~/.config_dsdb', processing_limit={integer})
```
I usually set it at `5 * os.cpu_count()`

## Help us help you

If you find any bugs or this tutorial doesn't work, then please submit an issue here:
https://github.com/AllenCellModeling/datasetdatabase




**Last updated: 28 November 2018**


## Contact
Jackson Maxfield Brown

jacksonb@alleninstitute.org

#### Important Note:
Version 1.* of DSDB is currently only handling bug fixes. Feature requests should be directed at version 2.

## License
`DatasetDatabase` is licensed under the terms of the BSD-2 license. See the file
"LICENSE" for information on the history of this software, terms & conditions
for usage, and a DISCLAIMER OF ALL WARRANTIES.

All trademarks referenced herein are property of their respective holders.
Copyright (c) 2018--, Jackson Maxfield Brown, The Allen Institute for Cell
Science.
