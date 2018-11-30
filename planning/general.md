# dslib


## Index
1. [Requirements](#requirements)
2. [Related Work](#related-work)
3. [Lessons Learned](#lessons-learned)
4. [Implementation](#implementation)
5. [Planned API](#planned-api)

## Planned API
1. [Core](#core-requirements)
    1. [Connect](#connect)
    2. [Create](#create)
    3. [Validate](#validate)
    4. [Upload](#upload)
    5. [Get](#get)
    6. [Preview](#preview)
    7. [Search](#search)
    8. [Annotate](#annotate)
    9. [Find](#find)
    10. [Purge](#purge)
    11. [Export](#export)
    12. [Couple](#couple)
    13. [Decouple](#decouple)
    14. [Graph](#graph)
2. [Future](#future)
    1. [Apply](#apply)
    2. [Rehydrate](#rehydrate)

### Core
These functions will be delivered with the "release" v2.0. These have been chosen because they are most actively used in v1.1 or there have been expressed desires to have them in the next release.

#### Connect
`db = dslib.Database(config)`

Connect to a database. While most users will use this very minimally, this is an object I currently use a lot for maintenance and testing of the database.

#### Create
`ds = dslib.Dataset(obj)`

Create an immutable copy of the object that other `dslib` operations can then run against. If the object passed into the `Dataset` is of a known type, during storage and retrieval the object will be stored and rebuilt from its `Iota` blocks. If it isn't of a known type it will be serialized to a file and the file path will be stored. The benefits of deconstruction to `Iota` are the drastic storage cost reduction due to potential deduplication, dataset discovery from `Iota` searching, and if setup, supporting file storage and deduplication.

#### Validate
`ds = ds.validate(conditions)`

Validate the current dataset under the passed conditions. Each object type has different validation functions, a `pandas.DataFrame` object for example has functions for validating value types, file existence, and arbitrary value lambda checks, each per column. Any attempt at uploading unvalidated datasets to a database will be rejected.

#### Upload
`ds = db.upload(ds)`

Upload a dataset to a database. If the underlying object stored in the dataset is of a known type, the object will be deconstructed to its `Iota` blocks. The returned object has the exact same underlying object but new database specific metadata. If the dataset has the same hash as an already stored dataset, the upload will be skipped and the database metadata will simply be attached to the object and returned.

#### Get
`ds = db.get(name)`

Get (and potentially rebuild) a dataset from the database. The object will be reconstructed if it was a known type when it was uploaded. The returned object is the exact same object, including the metadata, as the object returned from an upload operation.

#### Preview
`info = db.preview(hash)`

Preview a dataset. A dataset preview is largely a collection of metadata information stored in the database about that dataset however this preview is also used as a `README` for the dataset if the dataset was to ever be exported. Because of this behavior, owners of datasets can specifically assign a markdown file to be returned along with the normal metadata collection.

#### Search
`results = db.search(keywords, conditions)`

Search for a dataset using a sequence of keywords with some conditions. The basic search can run quite quickly as it will search the generated or attached metadata of each dataset. Optionally, you can include searching against `Iota`. Conditions to search against are things like owner, date, etc or you can make this conditions to not search against, i.e. WHERE or WHERE NOT.

#### Annotate
`ds = ds.annotate(comment)`

Annotate a dataset. Annotations are different from a dataset description, take for example, a dataset that is no longer valuable, it's description may still be valid but you may want to mark it as "out of date". Annotations will be used during search operations so making them more descriptive is better.

#### Find
`archive = db.find(key, value)`

Find all datasets that contain a specific `Iota`. Key and Value can be put in normal form, key is a string while, value is whatever object type you are searching for. Useful when trying to find datasets that contain a specific file path for example. Optionally, you can leave one of these parameters blank and the results returned will be based off the other. I.e. leave `key` blank but use `"/path/to/a/file.ome.tiff"` as the value, will return a list of all iota and their associated datasets that contain that value.

#### Purge
`info = db.purge(hash)`

Purge a dataset from the database. Available to the owners/ admins of datasets, when a dataset is no longer being actively used, it is best practice to purge the dataset from the database, this will remove all the sub-dataset objects from the database while keeping the high level and metadata information; removes `Iota` and `Group`, but keeps Dataset, Run, and Annotation items. Optionally, you may request to purge the entire tree of a datasets history. If so, the described operation will occur for all datasets found to be linked to the targeted hash unless it is the true origin (the originally uploaded dataset). The true origin dataset must be purged with `force=True`.

#### Export
`package = ds.package()`

Export the dataset as a Quilt/T4 package for others to use. This will gather all supporting files and metadata and construct documentation for you unless explicitly overridden. Of note, these packages can also be imported using the `db.upload` and the system will unpack the contents for you.

#### Couple
`archive = db.couple(from, to, comment)`

Couple two datasets together. Available to the owners/ admins of datasets, this is a useful operation to retain lineage and ensure that users have an easy method to reach the most up-to-date dataset for the project they are working on.

#### Decouple
`archive = db.decouple(from, to)`

Decouple two datasets from one another. Much like the `db.purge` and the `db.couple` operations described above, this operation can only be run by dataset admins/ owners. This will remove the linkage of two datasets from one another.

#### Graph
`archive = db.graph(name, n_steps)`

Graph the connections/ processing history of a dataset. This is not a graph of information sharing between datasets, this function returns an archive of how datasets are linked together, either from `db.link` or `ds.apply` operations.

### Future
These functions will be delivered after the "release" v2.0. While these have been shown to be useful, the are not widely used and do not *yet* provide the benefit I believed they will.

#### Apply
`ds = ds.apply(callable, parameters)`

Apply any callable to a dataset and pass any additional parameters that should be passed along to the callable. This will attempt to run the callable against the underlying object contained in the dataset and store the results as a new dataset. However, at any point during this operation, if any error arises, whether during the runtime of the passed callable, or during the upload of metadata, parameters, or created object, this will error out with full traceback and the database will rollback any transaction. In the case of success, multiple objects will have been created and stored. The first of which will be a Dockerfile that can be used to rerun this apply step if the callable is "simple" enough. Usually "simple" means: "doesn't require specific device drivers". Additionally, the parameters passed to the callable will also be stored as a dataset in the database. All run information will be stored and the two datasets and parameters will be linked together. It is important to note however, that the hashes for the input dataset, the generated Dockerfile, and the passed parameters dataset make up the unique index assigned to a run and if this unique index already exists, the results of the previously computed run will be returned instead of the apply running to completion. This functionality can be explicitly overridden.

#### Rehydrate
`ds = db.rehydrate(origin, destination)`

Rehydrate a dataset that has previously been purged. This will rehydrate all stepping stone datasets along the way as the system needs. Rehydrate, in this context, means to reconstruct, store, all Iota, Group, and their associated Junction items, as well as apply the Dockerfiles stored previously if needed.
