<h1 id="datasetdatabase.utils.checks.check_user">check_user</h1>

```python
check_user(user:Union[str, NoneType]=None, err:str='\n\nUser: "{u}", is a blacklisted username. Please pass a valid username.\n') -> Union[str, ValueError]
```

Check or get the username for approval.


#### Example
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


#### Parameters
*user: str, None*

A string username.

Default: None (getpass.getuser())

*err: str*

An additional error message to be displayed before the standard error
should the provided user not pass blacklist checks.


#### Returns
*user: str*

Returns string username if the passed or retrieved username passed
blacklist checks.


#### Errors
*ValueError*

The provided username is in the blacklist.


