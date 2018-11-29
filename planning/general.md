# nonameDB

### Index
1. [General Operations](#general-operations)
    1. [Connect](#connect)
    2. [Create](#create)
    3. [Validate](#validate)
    4. [Upload](#upload)
    5. [Get](#get)
    6. [Preview](#preview)
    7. [Search](#search)
    8. [Annotate](#annotate)
2. [Advanced Usage](#advanced-usage)
    1. [Apply](#apply)
    2. [Graph](#graph)
    3. [Find](#find)
    4. [Purge](#purge)
    5. [Rehydrate](#rehydrate)
    6. [Export](#export)
3. [Database Deployment](#database-deployment)
    1. [Setup](#setup)
    2. [FMS](#fms)


## General Operations
### Connect
`db = noname.connect(config)`

Create an object that acts as a mechanism to store, retrieve, preview, discover, and track the history of various objects.

## Create
`ds = noname.Dataset(obj)`

Create an immutable copy of the object that other `noname` operations can then run against. If the object passed into the Dataset is a
known type, during storage and retrieval the object will be stored and rebuilt from its Iota blocks. If it isn't a known type it will be
serialized and stored in full. The benefits of deconstruction to Iota are the drastic storage cost reduction due to potential deduplication,
dataset discovery from Iota searching, and if setup, supporting file deduplication.

### Validate
`ds = ds.validate(conditions)`

Validate the current dataset under the passed constraints. Each object type has different validation functions, a `pandas.DataFrame`
object for example has functions for validating value types, file existence, and arbitrary value lambda checks, each per column. Any attempt at
uploading unvalidated datasets to a database will be rejected.

### Upload
`ds = db.upload(ds)`

Upload a dataset to a database. If the underlying base object stored in the dataset is of a known type, the object will be deconstructed to its
`Iota` blocks. The returned object has the exact same underlying object but new database specific metadata. If the dataset has the same hash as an
already stored dataset, the upload will be skipped and the database metadata will simply be attached to the object and returned.

### Get
`ds = db.get(name)`

Get (and potentially rebuild) a dataset from the database. The object will be reconstructed if it was a known type when it was uploaded. The returned
object is the exact same object, including the metadata, as the object returned from when it was uploaded.

### Preview
`info = db.preview(hash)`

Preview a dataset. A dataset preview is largely a collection of metadata information stored in the database about that dataset however this preview
is also used as a `README` for a dataset if the dataset was to ever be exported. Because of this behavior, owners of datasets can specifically assign
a markdown file to be returned along with the normal metadata collection.

### Search
`results = db.search(keywords, conditions)`

Search for a dataset using a sequence of keywords with some conditions. The basic search can run quite quickly as it will search the generated or
attached metadata of each dataset. Optionally, you can include searching against `Iota`. Conditions to search against are things like owner, date,
etc or you can make this conditions to not search against, WHERE or WHERE NOT.

### Annotate
`ds = ds.annotate(comment)`

Annotate a dataset. Annotations are different from a dataset description, take for example, a dataset that is no longer valuable, it's description
may still be valid but you may want to mark it as "out of date". Annotations will be used during search operations so making them more descriptive
is better.

## Advanced Usage
### Apply
`ds = ds.apply(function, parameters)`

Apply any callable to a dataset and any additional parameters that should be passed along to the callable. This will attempt to run the callable
against the underlying object contained in the dataset and store the results as a new Dataset. However at any point during this operation, if any
error arises, whether in the passed callable or during upload of metadata or created object, this will error out with full traceback and the database
will rollback any transactions. In the case of success, multiple objects will have been created and stored. The first of which will be a Dockerfile
that can be used to rerun this apply step if the callable is "simple" enough, usually this means "doesn't require specific device drivers".
Additionally, the parameters passed to the callable will be stored in the database as a new dataset. All run information will be stored and linked
together. It is important to note however that if the hashes for the input dataset, generated Dockerfile, and generated parameters dataset are all
found in the `Run` table, the process won't run again, it will instead simply return the found dataset. You can explicitly force it to run again if
desired.

### Graph
`archive = db.graph(name, n_steps)`

Graph the connections/ processing history of a dataset and stop at n_steps away from the origin dataset. By default n_steps is None meaning to
continue to make edges and vertices until the database is exhausted. To be clear this is not an information sharing graph but a dataset processing
history graph. By default, this returns a `pandas.DataFrame` of the connections but when running in a notebook environment, this will also display
the resulting graph.

#### Find
`archive = db.find(key, value)`

Find all datasets that contain a specific Iota. Key and Value can be put in normal form, the value will be cast to bytes for you. Useful when trying
to find datasets that contain a specific file path for example. Optionally, you can leave one of these parameters blank and the results returned will
be based off the other.

### Purge
`info = db.purge(hash)`

Available to the owners/ admins of datasets. When a dataset is no longer being actively used, it is best practice to purge the dataset from the
dataset, this will remove all the sub-dataset objects from the database while keeping the high level and metadata information; removes Iota, Group,
and their associated Junction items, but keeps Dataset, Run, and Annotation items. Optionally, you may request to purge the entire tree of a datasets
history. If so, the described operation will occur for all datasets found to be linked to the targeted hash unless it is the true origin (the
originally uploaded dataset). Additionally, files that were created and stored in the attached FMS will be deleted, except, again, for those files
that are largely metadata, or are Dockerfiles.

### Rehydrate
`ds = db.rehydrate(origin, destination)`

Rehydrate a dataset that has previously been purged. This will rehydrate all stepping stone datasets along the way as the system needs. Rehydrate, in
this context, means to reconstruct, store, all Iota, Group, and their associated Junction items, as well as apply the Dockerfiles stored previously
if needed.

### Export
`package = ds.package()`

Export the dataset as a Quilt/T4 package for others to use. This will gather all supporting files and metadata and construct documentation for you
unless explicitly overridden. Of note, these packages can also be imported using the `db.upload` and the system will unpack the contents for you.


## Database Deployment
### Setup
```
docker run --it nonameDB \
    -v /connected/storage:/database/storage
```


### FMS
There is a basic FMS built in with the database system that can store any supporting files used in a dataset.
