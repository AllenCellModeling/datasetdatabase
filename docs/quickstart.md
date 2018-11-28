# Greg's Super-Quick Quick-Start Guide for DSDB

#### Index:
0. [Introduction](#Introduction)
1. [Setting up your credentials](#Credentials)
2. [Connect to DSDB](#Connect-to-DSDB)
3. [Getting data](#Getting-data)
    1. [Browsing existing datasets](#Browsing-existing-datasets)
    2. [Pulling data](#Pulling-data)

4. [Pushing data](#Pushing-data)
5. [Help us help you](#help-us-help-you)

## Introduction
Datasetdatabase is a python tool for creating, pushing, and pulling datasets. This tool is intended to be used to share data amongst data scientists whom have diverse data sharing needs. A dataset can be any collection of the normal data science stuff, usually things in a spreadsheet and files (sort of like what you would find in a [`pandas.DataFrame`]). First you need to install the software:

#### Important Note:
DSDB is currently under development, and many of these instructions are subject to change, or maybe not even work. 

#### Install
```bash
pip install git+https://github.com/AllenCellModeling/datasetdatabase.git
```

#### Reinstall (useful if Jackson changes anything)
```bash
pip uninstall datasetdatabase
pip install git+https://github.com/AllenCellModeling/datasetdatabase.git
```

## Credentials
Credentials are important. Make a file that looks like this and put it somewhere. I put mine at `~/.config_dsdb.json`.

```bash
{
   "driver": "postgres",
   "host": "<hostname>",
   "database": "<database_name>",
   "user": "<username>",
   "password": "<password>"
}
```

If you don't know what is supposed to go in there, go ask Jackson.

## Connect to DSDB
After you set up your credentials, in Python you can get a dsdb object as follows:

```python
import datasetdatabase as dsdb  
my_database = dsdb.DatasetDatabase(config='~/.config_dsdb', user='<the first part of your email>', processing_limit=30)
```

## Getting data
### Browsing existing datasets
To see what is in your database, you can do:  
```python
my_database.get_items_from_table('Dataset')
```

You'll see something like  

```python
[{'DatasetId': 1,  
'Name': 'deba767d-4c3a-483c-a0bc-66ecf7ad4e1b',  
'Description': 'algorithm parameters',  
'Introspector': 'datasetdatabase.introspect.dictionary.DictionaryIntrospector',  
'MD5': 'd34b2ff3ebd47dff1d21eee2d69d4ffd',  
'SHA256': '08e14d71fb56a8d6888e9b1f0c21d1a98050e7cdf034fdfcd06d0db59f810477',  
'Created': datetime.datetime(2018, 9, 28, 21, 22, 39, 745444)},  
{'DatasetId': 2,  
'Name': 'QCB_TOM20_feature_old',  
'Description': None,  
'Introspector': 'datasetdatabase.introspect.dataframe.DataFrameIntrospector',  
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
Once you have found a datset you like, you can pull it with:

```python
my_dataset = get_dataset(id={id})
```

You can get the data as a [`pandas.DataFrame`] with:

```python
my_dataframe = my_dataset.ds
```


## Pushing data
Datasets can be created by providing a path to a csv, by providing the dataset as a [`pandas.DataFrame`], or a dictionary. There are other ways to do this, but you'll have to read the documentation for that. 

Lets say I have data in a .csv file on disk, load it into a dataframe and make a dataset, or directly load it from the file path:

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

my_database = dsdb.DatasetDatabase(config='~/.config_dsdb', user='<the first part of your email>', processing_limit=30)
my_database.upload_to(ds)

```
 
## Help us help you

If you find any bugs or this tutorial doesn't work, then please submit an issue here:
https://github.com/AllenCellModeling/datasetdatabase

   

   
**Last updated: 28 November 2018**

