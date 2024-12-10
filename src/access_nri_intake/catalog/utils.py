from functools import partial, wraps
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

    def wrapper(*args, **kwargs):
        series = func(*args, **kwargs)
        return _to_tuple(series)

    return wrapper


def trace_failure(func: Callable) -> Callable:
    """
    Decorator that wraps a function and prints a message if it raises an exception
    """
    func_name = func.__name__
    colname = func_name[1:].split("_")[0]

    @wraps(func)
    def wrapper(*args, **kwargs):
        # Ensure the first argument is an instance of the class
        if not hasattr(args[0], "__class__"):
            raise TypeError("Decorator can only be applied to class methods")

        dispatch_key = getattr(args[0]._dispatch_keys, colname)

        try:
            return func(*args, **kwargs)
        except KeyError as exc:
            raise KeyError(
                f"Unable to find {colname} column '{dispatch_key}' with translator {args[0].__class__.__name__}"
            ) from exc

    return partial(wrapper, colname)
