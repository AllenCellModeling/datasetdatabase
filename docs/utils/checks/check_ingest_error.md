<h1 id="datasetdatabase.utils.checks.check_ingest_error">check_ingest_error</h1>

```python
check_ingest_error(e:Exception, err:str='\n\nSomething besides insertion went wrong...\n{e}\n') -> Union[bool, TypeError]
```

Check the provided exception and enforce that it was an ingestion error.


#### Example
```
>>> e = QueryException("SQL: ...")
>>> check_ingest_error(e)
True

>>> e = QueryException("SQL: ...")
>>> check_ingest_error(e)
TypeError:

Something besides insertion went wrong...
{Full Error}

>>> check_ingest_error(e, "this message displays first")
TypeError: this message displays first

Something besides insertion went wrong...
{Full Error}

```


#### Parameters
*e: Exception*

An error that needs to be checked for ingestion error.

*err: str*

An additional error message to be displayed before the standard error
should the provided variable not pass type checks.


#### Returns
*was_ingest: bool*

Returns boolean True if the provided error was an insertion error.


#### Errors
*TypeError*

The provided error was not an insertion error.


