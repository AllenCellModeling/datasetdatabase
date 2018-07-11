# SPEC FOR dsdb.export_as_quilt(dataset)

### Index
1. [Reason](#Reason)
2. [Planned Reference](#Planned-Reference)
3. [Other Comments](#Other-Comments)

## Reason
There is both a desire for datasets to be quickly shareable between individuals not using a DSDB instance and those using a DSDB instance. Specifically, in the case of current collaborations we need have a system known to work well for handling datasets (DSDB) and external collaborators we wish to try their software for. I have already completed work on DSDB to handle the importing of files and store them as quilt packages. The next step it would seem would be to build a packager that simply takes the dataset that was imported, and structures a build manifest in a way that we can export it and use that same dataset quickly and easily on a completely different machine with relative similarity between the two environments.

## Planned Reference
Below is a mockup of how a dataset would enter, store, transfer, and use under this functionality.

DATASET (delievered)

name: None

    foo |   bar     |   baz     |   files       |
    0   |   True    |   "hello" |   test.png    |
    1   |   False   |   "world" |   test.txt    |
    
DATASET (ingested by dsdb)

name: "fms_pkl_7a78..."

    foo |   bar     |   baz     |   files                                                           |
    0   |   True    |   "hello" |   /home/jovyan/.local/share/QuiltCli/quilt_packages/objs/5f8f...  |
    1   |   False   |   "world" |   /home/jovyan/.local/share/QuiltCli/quilt_packages/objs/a31q...  |
    
DATASET (exported as quilt)

name: "aics/dsdb_fms_pkl_7a78..."

    README
        file: generated_readme.md
        transform: id
    data:
        file: generated_data.pkl
        transform:id
    files:
        5f8f...:
            package: dsdb_storage/fms_png_5f8f...
        a31q...:
            package: dsdb_storage/fms_txt_a31q...

DATASET (usage)

name: "aics/dsdb_fms_pkl_7a78..."

```
>>> import quilt
>>> quilt.install("aics/dsdb_fms_pkl_7a78...")

>>> from quilt.data.aics import dsdb_fms_pkl_7a78... as dataset
>>> import pickle
>>> ds = pickle.load(dataset.data())
>>> ds["files"]
dsdb_fms_pkl_7a78...["files"]["5f8f..."]
dsdb_fms_pkl_7a78...["files"]["a31q..."]

>>> ds["files"][0]()
/path/to/external/users/quilt/store/quilt_packages/objs/5f8f...

>>> ds["files"][1]()
/path/to/external/users/quilt/store/quilt_packages/objs/a31q...
```

Realistically you should never have to used the files list in the quilt package as that is just present to transfer the files to the external collaborators quilt storage. The `generated_data.pkl` file would store the functions required to access the read paths instead of storing the paths themselves. A little bit extra work, but this would allow people to easily transfer datasets within this system while not duplicating work.

## Other Comments
Because this is an export function, to me it only makes sense that I would also write an `import_from_quilt` function that would read a quilt package and create the neccessary Iota, Dataset, Source, and Junction items. In which case it would actually be easier for people to use the system as follows.

```
>>> import quilt
>>> quilt.install("aics/dsdb_fms_pkl_7a78...")

>>> import datasetdatabase as dsdb
>>> mngr = dsdb.ConnectionManager(dsdb.LOCAL)
>>> local = mngr.connect(dsdb.LOCAL)
>>> local.import_from_quilt("aics/dsdb_fms_pkl_7a78...")
>>> local
Recent Dataset:
------------------------------------------------------------------
{'DatasetId': 1, 'Name': 'fms_pkl_7a78...', 'Description': '...', 'SourceId': 1, 'Created': '2018-07-11 22:33:13.836238'}

```

#### Now that's open science.