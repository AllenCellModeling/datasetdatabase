<h1 id="datasetdatabase.utils.checks">datasetdatabase.utils.checks</h1>


<h2 id="datasetdatabase.utils.checks.check_types">check_types</h2>

```python
check_types(var, allowed:Union[type, list, tuple], err:str='\n\nAllowed types: {a}\nGiven type: {t}\nGiven value: {v}\n') -> Union[bool, TypeError]
```

Check the provided variable and enforce that it is one of the passed types.

Example
==========
```
>>> temp = "this is a string"
>>> check_types(temp, str)
True

>>> check_types(temp, [str, int])
True

>>> check_types(temp, tuple([str, int]))
True

>>> check_types(temp, [int, list, dict])
TypeError:

Allowed types: (<class 'int'>, <class 'list'>, <class 'dict'>)
Given type: <class 'str'>
Given value: this is a string

>>> check_types(temp, [int, list, dict], "this message displays first")
TypeError: this message displays first

Allowed types: (<class 'int'>, <class 'list'>, <class 'dict'>)
Given type: <class 'str'>
Given value: this is a string

```

Parameters
==========
var: object
    A variable to be checked for type.

allowed: type, list, tuple
    A single, list, or tuple of types to check the provided variable
    against.

err: str
    An additional error message to be displayed before the standard error
    should the provided variable not pass type checks.

Returns
==========
is_type: bool
    Returns boolean True if the provided variable is of the provided
    type(s).

Errors
==========
TypeError:
    The provided variable was not one of the provided type(s).


<h2 id="datasetdatabase.utils.checks.check_file_exists">check_file_exists</h2>

```python
check_file_exists(f:Union[str, pathlib.Path], err:str='\n\nThe provided filepath does not exist.\nGiven filepath: {f}\n') -> Union[bool, FileNotFoundError]
```

Check the provided filepath for existence.

Example
==========
```
>>> temp = "/this/does/exist.jpg"
>>> check_file_exists(temp)
True

>>> temp = "/this/does/not/exist.jpg"
>>> check_file_exists(temp)
FileNotFoundError:

The provided filepath does not exist.
Given filepath: /this/does/not/exist.png

>>> check_file_exists(temp, "this message displays first")
FileNotFoundError: this message displays first

The provided filepath does not exist.
Given filepath: /this/does/not/exist.png

```

Parameters
==========
f: str, pathlib.Path
    A string or pathlib.Path filepath to be checked for existence.

err: str
    An additional error message to be displayed before the standard error
    should the provided variable not pass existence checks.

Returns
==========
file_exists: bool
    Returns boolean True if the provided filepath does exist.

Errors
==========
FileNotFoundError:
    The provided filepath did not exist.


<h2 id="datasetdatabase.utils.checks.check_user">check_user</h2>

```python
check_user(user:Union[str, NoneType]=None, err:str='\n\nUser: "{u}", is a blacklisted username. Please pass a valid username.\n') -> Union[str, ValueError]
```

Check or get the username for approval.

Example
==========
```
>>> check_user()
jacksonb

>>> user = "admin"
>>> check_user(user)
ValueError:

User: "admin", is a blacklisted username. Please pass a valid username.

>>> check_user(user, "this message displays first")
ValueError: this message displays first

The provided filepath does not exist.
User: "admin", is a blacklisted username. Please pass a valid username.

```

Parameters
==========
user: str, None
    A string username.

    Default: None (getpass.getuser())

err: str
    An additional error message to be displayed before the standard error
    should the provided user not pass blacklist checks.

Returns
==========
user: str
    Returns string username if the passed or retrieved username passed
    blacklist checks.

Errors
==========
ValueError:
    The provided username is in the blacklist.


<h2 id="datasetdatabase.utils.checks.check_ingest_error">check_ingest_error</h2>

```python
check_ingest_error(e:Exception, err:str='\n\nSomething besides insertion went wrong...\n{e}\n') -> Union[bool, TypeError]
```

Check the provided exception and enforce that it was an ingestion error.

Example
==========
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

Parameters
==========
e: Exception
    An error that needs to be checked for ingestion error.

err: str
    An additional error message to be displayed before the standard error
    should the provided variable not pass type checks.

Returns
==========
was_ingest: bool
    Returns boolean True if the provided error was an insertion error.

Errors
==========
TypeError:
    The provided error was not an insertion error.


