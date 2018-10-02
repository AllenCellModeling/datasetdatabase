<h1 id="datasetdatabase.core.DatabaseConfig">DatabaseConfig</h1>

```python
DatabaseConfig(self, config:Union[str, pathlib.Path, Dict[str, str]], name:Union[str, NoneType]=None)
```

Create a DatabaseConfig.

A DatabaseConfig is an object you can create before connecting to a
DatasetDatabase, but more commonly this object will be created by a
DatabaseConstructor in the process of connecting or building. A minimum
requirements config needs just "driver" and "database" attributes.


#### Example
```
>>> DatabaseConfig("/path/to/valid/config.json")
{"name": "local",
 "config": {
    "driver": "sqlite",
    "database": "local.db"}
}

>>> DatabaseConfig("/path/to/invalid/config.json")
AssertionError: "Config must have ('driver', 'database')."

```


#### Parameters
*config: str, pathlib.Path, Dict[str: str]*

    A string, or pathlib.Path path to a json file storing the
    connection config information. Or a dictionary of string keys and
    string values as a connection config.

*name: str, None = None*

    A specific name for this connection. If none is passed the name
    gets set to the value stored by the "database" key in the passed
    config.


#### Returns
*self*


#### Errors
*AssertionError*

    One or more of the required config attributes are missing from the
    passed config.


