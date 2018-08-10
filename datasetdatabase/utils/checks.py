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


def validate_dataset_types(dataset: pd.DataFrame,
    type_map: Union[dict, None] = None):
    # enforce types
    check_types(dataset, pd.DataFrame)
    check_types(type_map, [dict, type(None)])

    err = "Dataset failed type check at:\n\tcolumn: {c}"

    # enforce data types
    if type_map is not None:
        bar = ProgressBar(len(dataset) * len(type_map))
        for key, ctype in type_map.items():
            err = err.format(c=key)
            dataset[key].apply(
            lambda x: bar.apply_and_update(check_types,
                                           var=x, allowed=ctype, err=err))


def validate_dataset_files(dataset: pd.DataFrame,
    filepath_columns: Union[str, list, None]= None):
    # enforce types
    check_types(dataset, pd.DataFrame)
    check_types(filepath_columns, [str, list, type(None)])

    err = "Dataset file not found at:\n\tcolumn: {c}"

    # convert types
    if isinstance(filepath_columns, str):
        filepath_columns = [filepath_columns]

    if filepath_columns is not None:
        bar = ProgressBar(len(dataset) * len(filepath_columns))
        for key in filepath_columns:
            err = err.format(c=key)
            dataset[key].apply(
            lambda x: bar.apply_and_update(check_file_exists, f=x, err=err))


def _assert_value(f, val, err):
    # get pass fail
    assert f(val), err


def validate_dataset_values(dataset: pd.DataFrame,
                            validation_map: Union[dict, None]):
    # enforce types
    check_types(dataset, pd.DataFrame)
    check_types(validation_map, [dict, type(None)])

    # standard error
    err = "Dataset failed data check at:\n\tcolumn: {c}"

    # enforce all dataset values to meet the lambda requirements
    if validation_map is not None:
        bar = ProgressBar(len(dataset) * len(validation_map))
        for key, func in validation_map.items():
            dataset[key].apply(
            lambda x: bar.apply_and_update(_assert_value,
                                    f=func, val=x, err=err.format(c=key)))


def cast(value: str, value_type: str, **kwargs):
    # check types
    check_types(value, str)
    check_types(value_type, str)

    # prep the type string
    value_type = value_type.replace("<class '", "")[:-2]

    # cast
    if "numpy.ndarray" in value_type:
        value = value.replace("[", "")
        value = value.replace("]", "")
        result = np.fromstring(value, sep=" ", **kwargs)
    elif "str" in value_type:
        result = eval(value_type + "('{v}')".format(v=value))
    elif value_type in NATIVE_SUPPORT_TYPES:
        result = eval(value_type + "({v})".format(v=value))
    else:
        result = value

    return result


def quick_cast(value, cast_type, info=None):
    try:
        if not isinstance(value, cast_type):
            return cast_type(value)
    except ValueError:
        raise ValueError("Could not cast:", value,
                         "to:", cast_type,
                         "\ninfo:", info)

    return value


def format_dataset(dataset: pd.DataFrame,
                   type_map: Union[dict, None] = None) -> pd.DataFrame:
    # enforce types
    check_types(dataset, pd.DataFrame)
    check_types(type_map, [dict, type(None)])

    # format data if type_map was passed
    if type_map is not None:
        bar = ProgressBar(len(dataset) * len(type_map))
        for key, ctype in type_map.items():
            dataset[key] = dataset[key].apply(
                lambda v: bar.apply_and_update(quick_cast,
                            value=v, cast_type=ctype, info="column: " + key))

    return dataset


def format_path(fp: str, replace_items: dict) -> str:
    # enforce types
    check_types(fp, str)
    check_types(replace_items, dict)

    # keep replacing until no changes remain
    for cur, new in replace_items.items():
        fp = fp.replace(cur, new)

    return fp


def format_paths(dataset: pd.DataFrame,
                 filepath_columns: Union[str, list, None],
                 replace_paths: Union[dict, None]) -> pd.DataFrame:
    # enforce types
    check_types(dataset, pd.DataFrame)
    check_types(filepath_columns, [str, list, type(None)])
    check_types(replace_paths, [dict, type(None)])

    # early return
    if filepath_columns is None or replace_paths is None:
        return dataset

    # convert types
    if isinstance(filepath_columns, str):
        filepath_columns = [filepath_columns]

    bar = ProgressBar(len(self.df) * len(filepath_columns))

    # apply changes
    dataset[filepath_columns] = dataset[filepath_columns].applymap(
        lambda fp: bar.apply_and_update(format_path,
                                        fp=fp, replace_items=replace_paths))

    return dataset
