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
