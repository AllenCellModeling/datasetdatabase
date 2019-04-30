<h1 id="datasetdatabase.introspect">datasetdatabase.introspect</h1>


<h2 id="datasetdatabase.introspect.dataframe">datasetdatabase.introspect.dataframe</h2>


<h3 id="datasetdatabase.introspect.dataframe.DataFrameIntrospector">DataFrameIntrospector</h3>

```python
DataFrameIntrospector(self, obj:pandas.core.frame.DataFrame)
```

Pandas DataFrame introspector. Ingest a DataFrame, validate, store files,
deconstruct, reconstruct, and package.

#### Example
```
>>> DataFrameIntrospector(data)
<class datasetdatabase.introspect.dataframe.DataFrameIntrospector>

```


#### Parameters
##### obj: pd.DataFrame
The dataframe you want to validate and potentially store in a dataset
database.


#### Returns
##### self


#### Errors


<h4 id="datasetdatabase.introspect.dataframe.DataFrameIntrospector.get_object_hash">get_object_hash</h4>

```python
DataFrameIntrospector.get_object_hash(self, alg:builtin_function_or_method=<built-in function openssl_md5>)
```

Get a unique and reproducible hash from the dataframe. DataFrame
hashing can be a bit tricky so this function is rather memory intensive
because it copies the bytes of every key-value pair in the dataframe
but this is in my opinion the best way to ensure a reproducible hash of
a dataset.

#### Example
```
>>> df_introspector.get_object_hash()
asdf123asd3fhas2423dfhjkasd8f92178hb5sdf

>>> df_introspector.get_object_hash(hashlib.sha256)
8hkasdr823hklasdf7832balkjsdf73lkasdjhf73blkakljs892hksdf9

```


#### Parameters
##### alg: types.BuiltinMethodType = hashlib.md5
A hashing algorithm provided by hashlib.


#### Returns
##### hash: str
The hexdigest of the object hash.


#### Errors


<h2 id="datasetdatabase.introspect.dictionary">datasetdatabase.introspect.dictionary</h2>


<h3 id="datasetdatabase.introspect.dictionary.DictionaryIntrospector">DictionaryIntrospector</h3>

```python
DictionaryIntrospector(self, obj:dict)
```

General dictionary introspector. Ingest a dictionary, create and validate,
Iota, etc.

<h2 id="datasetdatabase.introspect.object">datasetdatabase.introspect.object</h2>


<h3 id="datasetdatabase.introspect.object.ObjectIntrospector">ObjectIntrospector</h3>

```python
ObjectIntrospector(self, obj:object)
```

General object introspector. Using a single all attribute validation map,
you can ingest, validate, and deconstruct, an object. There is limited
functionality due to how generalizable this Introspector needs to be.

