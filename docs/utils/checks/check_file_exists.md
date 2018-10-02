<h1 id="datasetdatabase.utils.checks.check_file_exists">check_file_exists</h1>

```python
check_file_exists(f:Union[str, pathlib.Path], err:str='\n\nThe provided filepath does not exist.\nGiven filepath: {f}\n') -> Union[bool, FileNotFoundError]
```

Check the provided filepath for existence.


#### Example
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

#### Parameters
*f: str, pathlib.Path*

A string or pathlib.Path filepath to be checked for existence.

*err: str*

An additional error message to be displayed before the standard error
should the provided variable not pass existence checks.


#### Returns
*file_exists: bool*

Returns boolean True if the provided filepath does exist.


#### Errors
*FileNotFoundError*

The provided filepath did not exist.


