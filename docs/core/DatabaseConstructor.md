<h1 id="datasetdatabase.core.DatabaseConstructor">DatabaseConstructor</h1>

```python
DatabaseConstructor(self, config:Union[datasetdatabase.core.DatabaseConfig, NoneType]=None, schema:Union[datasetdatabase.schema.schemaversion.SchemaVersion, NoneType]=None, fms:Union[datasetdatabase.schema.filemanagers.fmsinterface.FMSInterface, NoneType]=None)
```

Created a DatabaseConstructor.

A DatabaseConstructor is an object used to create, teardown, or
reconnect to both a database and the assoicated FMS. You can provide
both a custom DatasetDatabase schema and a custom FMS interface.


#### Example
```
>>> DatabaseConstructor().config
{"name": "local",
 "config": {
    "driver": "sqlite",
    "database": "local.db"
 }
}

>>> config = DatabaseConfig("path/to/valid/config.json")
>>> DatabaseConstructor(config)
<class DatabaseConstructor ... >

```


#### Parameters
*config: DatabaseConfig, None = None*

    The config for the connection to the database. If None provided, LOCAL
    is chosen.

*schema: SchemaVersion, None = None*

    The schema to build. If None provided, Minimal is chosen.

*fms: FMSInterface, None = None*

    An FMS (File Management System) to handle the supporting files of
    datasets. If None provided, QuiltFMS is chosen.


#### Returns
*self*


#### Errors


<h2 id="datasetdatabase.core.DatabaseConstructor.prepare_connection">prepare_connection</h2>

```python
DatabaseConstructor.prepare_connection(self)
```

Prepare a database connection by asserting required attributes are
present in the config passed. If the database link is a file, enforce
that the file and all parent directories exist.


#### Example
```
>>> good_config.prepare_connection()


>>> bad_config.prepare_connection()
AssertionError: "Local databases must have suffix '.db'"

```


#### Parameters

#### Returns

#### Errors
*AssertionError*

    The local database link is not the appropriate file type (.db).


<h2 id="datasetdatabase.core.DatabaseConstructor.create_schema">create_schema</h2>

```python
DatabaseConstructor.create_schema(self)
```

Create all the tables referenced by the SchemaVersion that was passed
in the DatabaseConfig constructor and fms attachment.


#### Example
```
>>> constructor.create_schema()

```


#### Parameters

#### Returns

#### Errors


<h2 id="datasetdatabase.core.DatabaseConstructor.build">build</h2>

```python
DatabaseConstructor.build(self)
```

Connect to a database and build the tables found in the SchemaVersion
passed to the DatabaseConstructor initialization.

This is mainly a wrapper around the prepare_connection and create_schema
functions that additionally returns the orator.DatabaseManager object
created.


#### Example
```
>>> constructor.build()

```

#### Parameters

#### Returns
*db: orator.DatabaseManager*

    A constructed database manager object that can be used to fully
    interact with the database, but additionally, all the tables have
    been stored in the constructor.tables attribute.


#### Errors


<h2 id="datasetdatabase.core.DatabaseConstructor.get_tables">get_tables</h2>

```python
DatabaseConstructor.get_tables(self)
```

In the case that a database is already fully constructed, a get_tables
operation should be run so that the table map is fully up-to-date
without overwriting or constructing useless tables.


#### Example
```
>>> constructor.get_tables()

```

#### Parameters

#### Returns
*db: orator.DatabaseManager*

    A constructed database manager object that can be used to fully
    interact with the database, but additionally, all the tables have
    been stored in the constructor.tables attribute.


#### Errors


