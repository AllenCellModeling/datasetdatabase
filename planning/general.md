# arkiv
Jackson Maxfield Brown, 5 December 2018

## Index
1. [Terminology](#terminology)
2. [Requirements](#requirements)
3. [Related Work](#related-work)
4. [Lessons Learned](#lessons-learned)
5. [Planned API](#planned-api)
6. [Implementation](#implementation)

## Terminology
### Dataset:
A collection of related sets of information that is composed of separate elements but can be used as an input to, or is the output from, analysis. In relation to computational programming this collection is an in-memory object.

### Introspector:
An object used to perform operations against a specific type of python object that produced results that are recommended to be stored in a `arkiv` database. `dsdb.introspector.dataframe.DataFrameIntrospector` is an example of an `Introspector` as it has functions to determine a unique hash for a `pandas.DataFrame` and validate the data contained in the `pandas.DataFrame`.

### Known-type:
An object of known-type is an object that has a specified `Introspector` module. Examples of these are `pandas.DataFrame` and `dictionary` as their `Introspector`s are included as modules in `dsdb.introspect`.

## Requirements
There is a very real desire and need to have a centralized system for file and object retrieval in the institute for cross-team non-production workflows.
From conversations between the multiple institute members who are starting to use `dsdb` v1.2, it has become obvious that there are issues with the current implementation that should be addressed in future iterations.

The core requirement of v2.0 of whatever we name this system (for now I am using `arkiv`, English: "archive") is fast storage and retrieval of any dataset users may want to throw at it.

## Planned API
While there will be plenty of supporting functions that accompany this package, the following functions are aimed at the users, not the developers or maintainers of the database that support `arkiv` operations.

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
    10. [Export](#export)
    11. [Couple](#couple)
    12. [Decouple](#decouple)
    13. [Graph](#graph)
2. [Future](#future)
    1. [Apply](#apply)
    2. [Reproduce](#reproduce)

### Core
These functions will be delivered with the "release" v2.0. These have been chosen because they are most actively used in v1.1 or there have been expressed desires to have them in the next release.

#### Connect
`db = arkiv.Database(config)`

Connect to a database. This returns a python object that retains the information required to make requests to a `arkiv` database.

#### Create
`ds = arkiv.Dataset(obj)`

Create an immutable copy of the object that other `arkiv` operations can then run against. If the object passed into `Dataset` is of a known-type it will use default serialize/ deserialize settings known to that object's `Introspector`. If the object isn't of a known-type, the user will be forced to specify serialize/ deserialize callables that will also be stored.

#### Validate
`ds = ds.validate(conditions)`

Validate the current `Dataset` under the passed conditions. Each object type has different validation functions, a `pandas.DataFrame` object for example has functions for validating value types, file existence, and arbitrary value lambda checks, each per column. Any attempt at uploading unvalidated known-type `Dataset`s to an `arkiv` database will be rejected. The validation conditions will be stored in the `arkiv` database as a "schema".

#### Upload
`ds = db.upload(ds)`

Upload a `Dataset` to a database. If the underlying object stored in the `Dataset` is of a known-type, the object will be serialized using the default serialization method. The returned object has the exact same underlying object but new `arkiv` database specific metadata. If the `Dataset` has the same hash as an already stored `Dataset`, the upload will be skipped and the `arkiv` database metadata will simply be attached to the `Dataset` and returned.

#### Get
`ds = db.get(name)`

Get a `Dataset` from an `arkiv` database. The object will be deserialized using the callable passed to the `arkiv` database when the `Dataset` was originally uploaded.

#### Preview
`info = db.preview(hash)`

Preview a `Dataset`. A `Dataset` preview is largely a collection of metadata information stored in the database about that `Dataset` however this preview is also used as a `README` for the `Dataset` if it was to ever be exported. Because of this behavior, owners/ admins of `Dataset`s can specifically assign a markdown file to be returned along with the normal metadata collection.

#### Search
`results = db.search(keywords, conditions)`

Search for a `Dataset` using a sequence of keywords with some conditions. The search runs against the generated or attached metadata of each `Dataset` stored in an `arkiv` database.

#### Annotate
`ds = ds.annotate(comment)`

Annotate a `Dataset`. Annotations are different from a `Dataset` descriptions. Take for example, a `Dataset` that is no longer valuable, it's description may still be valid but you may want to mark it as "out of date". Annotations will be used during search operations so making them more descriptive is better.

#### Export
`package = ds.package(name)`

Export the `Dataset` as a Quilt/T4 package for others to use. This will gather all supporting files and metadata and construct documentation for you unless explicitly overridden. Of note, these packages can also be imported using the `db.upload` and the system will unpack the contents for you.

#### Couple
`archive = db.couple(from, to, comment)`

Couple two `Dataset`s together. Available to the owners/ admins of `Dataset`s, this is a useful operation to retain lineage and ensure that users have an easy method to reach the most up-to-date `Dataset` for the project they are working on.

#### Decouple
`archive = db.decouple(from, to)`

Decouple two `Dataset`s from one another. Like `db.couple` described above, this operation can only be run by dataset admins/ owners. This will remove the linkage of two `Dataset`s from one another.

#### Graph
`archive = db.graph(name, n_steps)`

Graph the connections/ processing history of a `Dataset`. This is not a graph of information sharing between `Dataset`s, this function returns an archive of how `Dataset`s are linked together, either from `db.couple` or `ds.apply` operations.

### Future
These functions will be delivered after the "release" v2.0. While these have been shown to be useful, the are not widely used and do not *yet* provide the benefit I believed they would.

#### Apply
`ds = ds.apply(callable, parameters)`

Apply any callable to a `Dataset` and pass any additional parameters that should be passed along to the callable. This will attempt to run the callable against the underlying object contained in the `Dataset` and store the results as a new `Dataset`. However, at any point during this operation, if any error arises, whether during the runtime of the passed callable, or during the upload of metadata, parameters, or created object, this will error out with full traceback and the `arkiv` database will rollback any transaction. In the case of success, multiple objects will have been created and stored. The first of which will be a Dockerfile that can be used to rerun this apply step if the callable is "simple" enough. Usually, "simple" means: "doesn't require specific device drivers". Additionally, the parameters passed to the callable will also be stored as a `Dataset` in the `arkiv` database. All run information will be stored and the two `Dataset`s and parameters will be linked together. It is important to note however, that the hashes for the input `Dataset`, the generated Dockerfile, and the passed parameters `Dataset` make up the unique index assigned to a run and if this unique index already exists, the results of the previously computed run will be returned instead of the apply running to completion. This uniqueness check can be explicitly overridden.

#### Reproduce
`ds = db.reproduce(origin_hash, destination_hash)`

Reproduce a `Dataset`. Reproduce, in this context, means to reconstruct, apply the Dockerfiles stored previously, and check the hashes of the produced intermediate `Dataset`s to ensure that a processing history is fully encapsulated in an `arkiv` database.
