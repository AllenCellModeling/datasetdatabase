<h1 id="datasetdatabase.utils.checks.check_types">check_types</h1>

```python
check_types(var, allowed:Union[type, list, tuple], err:str='\n\nAllowed types: {a}\nGiven type: {t}\nGiven value: {v}\n') -> Union[bool, TypeError]
```

Check the provided variable and enforce that it is one of the passed types.


#### Example
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


#### Parameters
*var: object*

A variable to be checked for type.

*allowed: type, list, tuple*

A single, list, or tuple of types to check the provided variable
against.

*err: str*

An additional error message to be displayed before the standard error
should the provided variable not pass type checks.


#### Returns
*is_type: bool*

Returns boolean True if the provided variable is of the provided
type(s).


#### Errors
*TypeError*

The provided variable was not one of the provided type(s).


