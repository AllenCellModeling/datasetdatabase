# arkiv
Jackson Maxfield Brown, 14 December 2018

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
From conversations between the multiple institute members who are starting to use `dsdb` v1.2, it has become obvious that there are issues with the current implementation of `dsdb` that should be addressed in future iterations.

The core requirement of v2.0 of whatever we name this system (for now I am using `arkiv`, English: "archive") is fast storage and retrieval of any dataset users may want to throw at it. Compared to v1.2, this means that all datasets will be serialized in full, no `Iota` or `Group` creation. However, to still make it easy for users to actually interact with the data and not worry about data formats, the request was for full object return instead of the file path return.

Additionally, unlike in v1.2 which has optional dataset validation, it was requested to have dataset validation be required for known-type datasets. This was requested for two reasons: the first, only "cleaned" datasets should be entering the system, and the second, was that if a full validation specification was provided to the system, the system should save that for the user to preview and see how the dataset was validated before requesting the data.

Lastly, versioning, search, and preview were all requested to be either retained in v2.0 from v1.2 or added functionality from v1.2. Search was briefly available in v0.8, and worked based on building a [tf-idf](https://en.wikipedia.org/wiki/Tf–idf) index to compare the search phrase against. In v2.0 this index will be created and stored as a batch process.

Requirements Summarized:
* Fast Storage and Retrieval
* Deserialization (Object Delivery)
* Dataset Validation Tools
* Lineage of Datasets (Versioning)
* Search and Preview (Discovery)

## Related Work
The paper [Principles of Dataset Versioning: Exploring the Recreation/ Storage Tradeoff](https://arxiv.org/pdf/1505.05211.pdf) provides an excellent, in-depth look at a system that behaves similar to v1.2 but instead of storing `Iota` and `Group`s in full like in v1.2, it stores only the deltas, so to rebuild a version of a dataset you must actually make the cell level changes at each step of the reconstruction process. This paper also compares v1.2 like (cell based) systems against git and SVN for storage costs. At a high level they find that the storage costs are comparable and in specific cases computing the deltas for a specific dataset under git failed while under a cell based system it did not. Recreation cost was not considered against git and SVN however multiple delta computing algorithms were used to test different reconstruction scenarios. What this paper doesn't address are the other requirements determined from users such as deserialization and search, this is because it was solely focused on cost analysis.

https://www.researchgate.net/publication/250888132_Preserving_Scientific_Data_with_XMLArch
file:///Users/jacksonb/Downloads/Managing_Structured_Collections_of_Community_Data.pdf
https://haslab.uminho.pt/jop/publications/survey-and-classification-storage-deduplication-systems

## Lessons Learned
While `dsdb` v1.2 showed promise in delivering a generalized object store it had serious problems with speed and scaling. When it was originally released, queries took a long time to run resulting in slow to return objects. That said, we did learn the basis for active research currently happening in a few labs, such as Aditya's (author of the "Principles of Dataset Versioning" paper from the prior section) versioned datasets lab that is looking into systems similar to `dsdb` but with no concept of `Iota` just `Group`s. Aditya, and his lab, have now tried two separate systems at solving the dataset versioning problem the first as outlined in "Principles of Dataset Versioning", in my opinion, is very close to what I call the `Iota` concept. Or storing entire datasets in key-value building blocks so that you can very quickly understand the total diff between two datasets. His newest version is more streamlined but has a few issues in my opinion for our data specifically. His newest system focuses on `Group` level deduplication, as each dataset is just it's own relational database table and versioning is done by simply joining another table and a junction table to it with foreign keys indicating which rows to select given a version of a dataset. My two current issues with this are as follows: this system is very good at value changes but not schema changes. What I mean by this is that if you want to add a new column to a dataset, that is a whole new table, not a new version. Our research and development data usually requires multiple small schema updates, so this system would be complicated to run on a large scale for our usage. The second issue is that without further tooling there are no real mechanisms currently in place for preview, search, discovery, etc., that have already been shown to be valuable. That said, because of it's simplicity it is also quite fast at delivering unique versions of datasets given they are all the same schema. In fact it may be worth it to follow this model but build additional tooling to indicate that the "next" version of a dataset changed schema and simply point at the next table. The last drawback of both Aditya's systems, are their lack of complex dataset support. As we are now seeing with FISH & RNAseq data, complex data formats need odd support cases that can be supported by a system like his most recent but again, this issue is additional engineering time to add the features in.

The primary lessons learned from `dsdb` v1.2 in my opinion were that there are a severe lack of tools to support basic dataset versioning principles. These include things like validation, cleaning, and dataset uniqueness. Fortunately, `dsdb` v1.2 did produce tooling to directly support these needs and that functionality will be carried over but initially more focused on DataFrame like datasets. These were found to be useful in identifying common user error, such as uncleaned datasets or invalid dataset conditions, and, while rare, duplicate datasets. Additionally, the largest benefit provided was building a centralized and simplified method of accessing datasets. It made it simple for users to naturally understand and have no doubts in that the dataset someone was handing off was the one they found.

## Planned API
While there will be plenty of supporting functions that accompany this package, the following functions are aimed at the users, not the developers or maintainers of the database that support `arkiv` operations.

1. [Core](#core-requirements)
    1. [Connect](#connect)
    2. [Create](#create)
    3. [Validate](#validate)
    4. [Upload](#upload)
    5. [Get](#get)
    6. [Open](#open)
    7. [Close](#close)
    8. [Preview](#preview)
    9. [Search](#search)
    10. [Annotate](#annotate)
    11. [Set Upstream](#set-upstream)
    12. [Graph](#graph)
2. [Future](#future)
    1. [Export](#export)
    2. [Apply](#apply)
    3. [Reproduce](#reproduce)

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

#### Open
`ds = db.open(name)`

Open a `Dataset` from an `arkiv` database. Unlike `get` this will create a local copy of the `Dataset` on the user's machine for them to specifically interact with and then open that local copy. This is useful for when you have a multi-gb large `Dataset`s that commonly use file locking so the user can interact with portions of the `Dataset` at a time. However, this file lock means only one person can use the `Dataset`, when using `open` a local copy is created and the user can interact with the `Dataset` as normal.

#### Close
`ds.close()`

Close a `Dataset` that was `open`. This will remove the local copy of the `Dataset` from the user's machine. This function and `open` as described above will also have a context manager as a wrapper to allow users to use the common `with db.open(name) as ds` functionality that creates a local copy and then removes the local copy on exit of the context manager.

#### Preview
`info = db.preview(hash)`

Preview a `Dataset`. A `Dataset` preview is largely a collection of metadata information stored in the database about that `Dataset` however this preview is also used as a `README` for the `Dataset` if it was to ever be exported. Because of this behavior, owners/ admins of `Dataset`s can specifically assign a markdown file to be returned along with the normal metadata collection.

#### Search
`results = db.search(keywords, conditions)`

Search for a `Dataset` using a sequence of keywords with some conditions. The search runs against the generated or attached metadata of each `Dataset` stored in an `arkiv` database.

#### Annotate
`ds = ds.annotate(comment)`

Annotate a `Dataset`. Annotations are different from a `Dataset` descriptions. Take for example, a `Dataset` that is no longer valuable, it's description may still be valid but you may want to mark it as "out of date". Annotations will be used during search operations so making them more descriptive is better.

#### Set Upstream
`archive = ds.set_upstream(parent_hash, comment)`

Set a `Dataset` to be the parent of the current `Dataset`. Available to the owners/ admins of `Dataset`s, this is a useful operation to retain lineage and ensure that users have an easy method to reach the most up-to-date `Dataset` for the project they are working on.

#### Graph
`archive = db.graph(name, n_steps)`

Graph the connections/ processing history of a `Dataset`. This is not a graph of information sharing between `Dataset`s, this function returns an archive of how `Dataset`s are linked together, either from `db.couple` or `ds.apply` operations.

### Future
These functions will be delivered after the "release" v2.0. While these have been shown to be useful, the are not widely used and do not *yet* provide the benefit I believed they would.

#### Export
`package = ds.package(name)`

Export the `Dataset` as a Quilt/T4 package for others to use. This will gather all supporting files and metadata and construct documentation for you unless explicitly overridden. Of note, these packages can also be imported using the `db.upload` and the system will unpack the contents for you.

#### Apply
`ds = ds.apply(callable, parameters)`

Apply any callable to a `Dataset` and pass any additional parameters that should be passed along to the callable. This will attempt to run the callable against the underlying object contained in the `Dataset` and store the results as a new `Dataset`. However, at any point during this operation, if any error arises, whether during the runtime of the passed callable, or during the upload of metadata, parameters, or created object, this will error out with full traceback and the `arkiv` database will rollback any transaction. In the case of success, multiple objects will have been created and stored. The first of which will be a Dockerfile that can be used to rerun this apply step if the callable is "simple" enough. Usually, "simple" means: "doesn't require specific device drivers". Additionally, the parameters passed to the callable will also be stored as a `Dataset` in the `arkiv` database. All run information will be stored and the two `Dataset`s and parameters will be linked together. It is important to note however, that the hashes for the input `Dataset`, the generated Dockerfile, and the passed parameters `Dataset` make up the unique index assigned to a run and if this unique index already exists, the results of the previously computed run will be returned instead of the apply running to completion. This uniqueness check can be explicitly overridden.

#### Reproduce
`ds = db.reproduce(origin_hash, destination_hash)`

Reproduce a `Dataset`. Reproduce, in this context, means to reconstruct, apply the Dockerfiles stored previously, and check the hashes of the produced intermediate `Dataset`s to ensure that a processing history is fully encapsulated in an `arkiv` database.

## Implementation
Due to the planned drastic changes from v1.2 of removing `Iota` and `Group` and all other related dependents, the new API and structure of the system is well suited for a relational database. However, learning from v1.2 and it's rigidity of database choice, as well as from input from others, I don't see why it isn't possible to structure the system to allow any database, the only requirement of this change would be that there must be a wrapper around the database being used to have specific functions to deliver back what has been requested. Ex: a `get` call retrieves the `Dataset` object regardless of the backed database. For example, a relational database would have specific tables to construct and queries to run, where a NoSQL database would need to create collections and have their own query mappings. I particularly care about this for testing purposes of seeing how different databases compare for certain operations. Additionally, while `Iota` and `Group` concepts are not currently under consideration for v2.0 launch, under a graph database they may be useful in reducing redundancy of data, however, because that is again, very research heavy work, the aim for v2.0 is to focus on relational databases first.

I want to be clear, while I believe the `Iota` and `Group` deconstruction process is incredibly useful and research worthy, I believe it is best to prioritize a product that solves the users needs first. That is, first and foremost, solve the requirements as listed above. If successful, we can come back and attempt a graph database solution using `Iota` and `Group` deconstruction process at a later point.

Related to above, there is a hope that the entire deployment of a new `arkiv` database can be entirely done with Docker. This would make it easy to build a testing database or have individual databases for each user to have smaller, even more research and development datasets on if they so desire. Ex: a user has their own personal `arkiv` database for tracking very experimental work and can then deliver more tested and cleaned datasets to their group's shared `arkiv`.

I have attempted to break up the work comprised in this project into smaller pieces that can be more easily tracked and planned.
In no particular order, these tasks are:
* Relational Database Schema and Docker Deployment
* Core Dataset API (DataFrame Oriented First)
* Validation Tooling (DataFrame Oriented First)
* Storage of Dataset and Supporting Files (Given FMS Connection)
* Search, Preview, and Dataset Discovery
* *Futures* (Other Database Types, Further API Development, etc.)
