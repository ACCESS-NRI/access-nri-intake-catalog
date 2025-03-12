from functools import wraps
from typing import Callable

import pandas as pd


def _to_tuple(series: pd.Series) -> pd.Series:
    """
    Make each entry in the provided series a tuple

    Parameters
    ----------
    series: :py:class:`~pandas.Series`
        A pandas Series or another object with an `apply` method
    """
    return series.apply(lambda x: (x,))


def tuplify_series(func: Callable) -> Callable:
    """
    Decorator that wraps a function that returns a pandas Series and converts
    each entry in the series to a tuple
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        series = func(*args, **kwargs)
        return _to_tuple(series)

    return wrapper


def trace_failure(func: Callable) -> Callable:
    """
    Decorator that wraps a function and prints a message if it raises an exception
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        func_name = func.__name__
        colname = func_name[1:].split("_")[0]
        # Check that the first argument is a DefaultTranslator (or subclass) instance
        # This is hacky, but I want the decorators outside the main translators module
        if not str(type(args[0]).mro()).__contains__("DefaultTranslator"):
            raise TypeError("Decorator can only be applied to translator class methods")

        try:
            return func(*args, **kwargs)
        except KeyError as exc:
            raise KeyError(
                f"Unable to translate '{colname}' column with translator '{args[0].__class__.__name__}'"
            ) from exc

    return wrapper
