#!/usr/bin/env python

# installed
from typing import Union
import pandas as pd
import pathlib
import getpass

# self
from .progressbar import ProgressBar

# globals
CHECK_TYPES_ERR = """

Allowed types: {a}
Given type: {t}
Given value: {v}
"""

CHECK_FILE_EXISTS_ERR = """

The provided filepath does not exist.
Given filepath: {f}
"""

CHECK_USER_ERR = """

User: "{u}", is a blacklisted username. Please pass a valid username.
"""

CHECK_INGEST_ERR = """

Something besides insertion went wrong...
{e}
"""

BLACKLISTED_USERS = ["jovyan",
                     "admin",
                     "root"]

NATIVE_SUPPORT_TYPES = ["int", "float", "bool",
                        "list", "tuple", "dict", "set"]


def check_types(var,
    allowed: Union[type, list, tuple],
    err: str = CHECK_TYPES_ERR) -> Union[bool, TypeError]:
    """
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
    ##### var: object
    A variable to be checked for type.

    ##### allowed: type, list, tuple
    A single, list, or tuple of types to check the provided variable
    against.

    ##### err: str
    An additional error message to be displayed before the standard error
    should the provided variable not pass type checks.


    #### Returns
    ##### is_type: bool
    Returns boolean True if the provided variable is of the provided
    type(s).


    #### Errors
    ##### TypeError
    The provided variable was not one of the provided type(s).

    """

    # enforce types
    if not isinstance(err, str):
        raise TypeError(CHECK_TYPES_ERR.format(a=str, t=type(err), v=err))

    # convert to tuple if list passed
    if isinstance(allowed, list):
        allowed = tuple(allowed)

    # check types
    if isinstance(var, allowed):
        return True

    # format error
    if CHECK_TYPES_ERR not in err:
        err += CHECK_TYPES_ERR

    # raise error
    raise TypeError(err.format(a=allowed, t=type(var), v=var))


def check_file_exists(f: Union[str, pathlib.Path],
    err: str = CHECK_FILE_EXISTS_ERR) -> Union[bool, FileNotFoundError]:
    """
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
    ##### f: str, pathlib.Path
    A string or pathlib.Path filepath to be checked for existence.

    ##### err: str
    An additional error message to be displayed before the standard error
    should the provided variable not pass existence checks.


    #### Returns
    ##### file_exists: bool
    Returns boolean True if the provided filepath does exist.


    #### Errors
    ##### FileNotFoundError
    The provided filepath did not exist.

    """

    # enforce types
    check_types(f, [str, pathlib.Path])
    check_types(err, str)

    # convert
    f = pathlib.Path(f)

    # actual check
    if f.exists():
        return True

    # format error
    if CHECK_FILE_EXISTS_ERR not in err:
        err += CHECK_FILE_EXISTS_ERR

    # raise error
    raise FileNotFoundError(err.format(f=f))


def check_user(user: Union[str, None] = None,
    err: str = CHECK_USER_ERR) -> Union[str, ValueError]:
    """
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
    ##### user: str, None
    A string username. Default: None (getpass.getuser())

    ##### err: str
    An additional error message to be displayed before the standard error
    should the provided user not pass blacklist checks.


    #### Returns
    ##### user: str
    Returns string username if the passed or retrieved username passed
    blacklist checks.


    #### Errors
    ##### ValueError
    The provided username is in the blacklist.

    """

    # enforce types
    check_types(user, [str, type(None)])
    check_types(err, str)

    # get user
    if user is None:
        user = getpass.getuser()

    # check allowed
    if user in BLACKLISTED_USERS:

        # format error
        if CHECK_USER_ERR not in err:
            err += CHECK_USER_ERR

        # error
        raise ValueError(err.format(u=user))

    # return if did not raise
    return user


def check_ingest_error(e: Exception,
    err: str = CHECK_INGEST_ERR) -> Union[bool, TypeError]:
    """
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
    ##### e: Exception
    An error that needs to be checked for ingestion error.

    ##### err: str
    An additional error message to be displayed before the standard error
    should the provided variable not pass type checks.


    #### Returns
    ##### was_ingest: bool
    Returns boolean True if the provided error was an insertion error.


    #### Errors
    ##### TypeError
    The provided error was not an insertion error.

    """

    # enforce types
    check_types(err, str)

    if "UNIQUE constraint failed:" in str(e) or \
        "duplicate key value violates unique constraint" in str(e):
        return True

    # format error
    if CHECK_INGEST_ERR not in err:
        err += CHECK_INGEST_ERR

    # raise error
    raise TypeError(err.format(e=e))
