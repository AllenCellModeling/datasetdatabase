<h1 id="datasetdatabase.core.DatasetInfo">DatasetInfo</h1>

```python
DatasetInfo(self, DatasetId:int, Name:Union[str, NoneType], Introspector:str, MD5:str, SHA256:str, Created:Union[datetime.datetime, str], OriginDb:datasetdatabase.core.DatasetDatabase, Description:Union[str, NoneType]=None)
```

Create a DatasetInfo.

A DatasetInfo is an object usually created by a database function to be
returned to the user as a block attached to a Dataset. It's primary
responsibility is to be used as a verification source against changed
datasets.


#### Example
```
>>> stats = db.preview("Label Free Images")
>>> type(stats["info"])
DatasetInfo

```


#### Parameters
*DatasetId: int*

    The dataset id stored in the database.

*Name: str, None*

    The dataset name stored in the database.

*Introspector: str*

    Which introspector should be used or was used to deconstruct and
    reconstruct the dataset.

*MD5: str*

    The MD5 hash of the underlying data object.

*SHA256: str*

    The SHA256 hash of the underlying data object.

*Created: datetime, str*

    The utc datetime when the dataset was created.

*OriginDb: DatasetDatabase*

    The database that this dataset is stored in.

*Description: str, None = None*

    The description for the dataset.


#### Returns
*self*


#### Errors
*AssertionError*

    The attributes passed could not be verified in the database.


