<h1 id="datasetdatabase.introspect.introspector">datasetdatabase.introspect.introspector</h1>


<h2 id="datasetdatabase.introspect.introspector.Introspector">Introspector</h2>

```python
Introspector(self, obj:object)
```

Introspectors are a class of object that help a Dataset object initialize,
validate, deconstruct, and reconstruct objects. This is the abstract base
class for all others to be built off of. In short a custom Introspector
needs to have an init, a validated property, as well as a validate
function, deconstruct and reconstruct functions, and finally a package
build function that allows for your 'Dataset' object to be fully packaged
with all it's dependancies with Quilt.

For a more defined example of an Introspector look at the
DataFrameIntrospector.

<h3 id="datasetdatabase.introspect.introspector.Introspector.get_object_hash">get_object_hash</h3>

```python
Introspector.get_object_hash(self)
```

Generate a hash for the object that can be reproduced given a
reconstruction from binary of the object. Sometimes reconstructions
from binary fail due to having different memory optimizations than the
original.

<h3 id="datasetdatabase.introspect.introspector.Introspector.validate">validate</h3>

```python
Introspector.validate(self, **kwargs)
```

Validate the object using passed **kwargs.

<h3 id="datasetdatabase.introspect.introspector.Introspector.deconstruct">deconstruct</h3>

```python
Introspector.deconstruct(self, db, ds_info)
```

Generate and insert all Iota, Group, IotaGroup, and GroupDataset items
in the attached database.

<h3 id="datasetdatabase.introspect.introspector.Introspector.package">package</h3>

```python
Introspector.package(self, items:Dict[str, Dict[str, object]]) -> Dict[str, object]
```

Because these are incredibly arbitrary objects, there is not default
way of inferring a package standard between them. Due to this it is
recommended that if you want to share data external of this database.
You will need to write a packaging function that returns both "data"
and "files" attributes. If there are no supporting files, return None.

<h2 id="datasetdatabase.introspect.introspector.reconstruct">reconstruct</h2>

```python
reconstruct(items:Dict[str, Dict[str, object]]) -> object
```

Given dictionary of lists of Iota, Group, and IotaGroup objects,
reconstruct to the base object.

