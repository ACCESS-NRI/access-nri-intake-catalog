# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Shared utilities for writing Intake-ESM builders and their parsers"""

import pickle
import re
import warnings
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path

import cftime
import numpy as np
import polars as pl
import xarray as xr
import xxhash
from dateutil.relativedelta import relativedelta
from frozendict import frozendict
from pandas.api.types import is_object_dtype

FREQUENCY_STATIC = "fx"


# Note the ordering of this dictionary - we are deliberately searching for
# the 'rarer' frequency descriptors first, because, e.g., "mon" may appear as
# part of a totally unrelated word in the filename
FILENAME_TO_FREQ = {
    "annual": "yr",
    "yearly": "yr",
    "hourly": "hr",
    "hour": "hr",
    "monthly": "mon",
    "month": "mon",
    "daily": "day",
    "day": "day",
    "year": "yr",
    "yr": "yr",
    "mth": "mon",
    "mon": "mon",
    "hr": "hr",
}


class EmptyFileError(Exception):
    pass


@dataclass
class _NCFileInfo:
    """
    Holds information about a NetCDF file that is used to create an intake-esm
    catalog entry.

    Notes
    -----
    Use of both path and filename seems redundant, but constructing filename from
    the path using a __post_init__ method makes testing more difficult. On balance,
    more explicit tests are probably more important than the slight redundancy.
    """

    filename: str | Path
    path: str
    file_id: str
    frequency: str
    start_date: str
    end_date: str
    variable: list[str]
    variable_long_name: list[str]
    variable_standard_name: list[str]
    variable_cell_methods: list[str]
    variable_units: list[str]
    realm: str = ""
    temporal_label: str = field(default_factory=str)

    def __post_init__(self):
        """
        Take the `variable_cell_methods` list and turn it into the time_aggregation
        string by looking for known time aggregation methods.

        Variable cell methods may look like:
        'time: mean', 'time: sum', 'area: mean time: mean', etc.

        the `time_aggregation` string should be a comma-separated list of unique
        time aggregation methods found in the `variable_cell_methods` list.
        """

        self.temporal_label = _parse_variable_cell_methods(self.variable_cell_methods)

    def to_dict(self) -> dict[str, str | list[str]]:
        """
        Return a dictionary representation of the NcFileInfo object
        """
        d = asdict(self)

        d_sortable = {
            key: val
            for key, val in d.items()
            if key
            in [
                "variable",
                "variable_long_name",
                "variable_standard_name",
                "variable_cell_methods",
                "variable_units",
            ]
        }

        df_sorted = (
            pl.DataFrame(d_sortable)
            .sort("variable")
            .with_columns(
                [
                    pl.col(colname)
                    .str.replace_all(r'"', r"")
                    .str.replace_all(r"'", r"")
                    for colname in d_sortable.keys()
                ]
                # This is a hack to remove extra quotes inside strings, which break
                # json encoding/decoding in intake_esm. TODO: work out a better fix.
            )
        )

        d_sorted = df_sorted.to_dict(as_series=False)

        for key, val in d_sorted.items():
            d[key] = val

        return d


@dataclass
class _VarInfo:
    """
    Holds information about the variables in a NetCDF file that is used to
    create an intake-esm catalog entry.
    """

    variable_list: list[str] = field(default_factory=list)
    long_name_list: list[str] = field(default_factory=list)
    standard_name_list: list[str] = field(default_factory=list)
    cell_methods_list: list[str] = field(default_factory=list)
    units_list: list[str] = field(default_factory=list)

    def append_attrs(self, var: str, attrs: dict) -> None:
        """
        Append attributes to the _VarInfo object, if the attribute has a
        'long_name' key.
        """
        if "long_name" not in attrs:
            return None

        self.variable_list.append(var)
        self.long_name_list.append(attrs["long_name"])
        self.standard_name_list.append(attrs.get("standard_name", ""))
        self.cell_methods_list.append(attrs.get("cell_methods", ""))
        self.units_list.append(attrs.get("units", ""))

    def to_var_info_dict(self) -> dict[str, list[str]]:
        """
        Return a dictionary representation of the _VarInfo object. Fields are
        defined explicitly for use in the _AccessNCFileInfo constructor.
        """
        return {
            "variable": self.variable_list,
            "variable_long_name": self.long_name_list,
            "variable_standard_name": self.standard_name_list,
            "variable_cell_methods": self.cell_methods_list,
            "variable_units": self.units_list,
        }


class HashableIndexes:
    """
    Consumes either an xarray dataset or its _indexes attribute, and creates a
    hashable representation of the indexes. Can be used to compare datasets & whether
    they are mergeable based on their indexes, and potentially for labelling grids
    in a catalog.
    """

    def __init__(
        self,
        *,
        ds: xr.Dataset | None = None,
        _indexes: dict | None = None,
        drop_indices: Iterable[str] | None = None,
    ):
        if ds is not None and _indexes is not None:
            raise TypeError(
                "Can only initialise HashableIndexes with either an xarray dataset (ds) or its _indexes (_indexes), not both"
            )
        elif ds is not None:
            _indexes = ds._indexes

        drop_indices = drop_indices or []
        self.dict = frozendict(
            {
                key: val.index.values
                for key, val in _indexes.items()  # type: ignore[union-attr]
                if not is_object_dtype(val.coord_dtype) and key not in drop_indices
            }
        )

        self._bytedict = {key: val.tobytes() for key, val in self.dict.items()}

        bytestream = pickle.dumps(self._bytedict, protocol=pickle.HIGHEST_PROTOCOL)
        self.xxh = xxhash.xxh3_64(bytestream).hexdigest()

    def __repr__(self):
        return str(self.xxh)

    def __eq__(self, other) -> bool:
        if not isinstance(other, HashableIndexes):
            return False
        if other.xxh == self.xxh:
            return True
        return False

    def __hash__(self):
        return int(self.xxh, 16)

    def __and__(self, other) -> set:
        """
        Return all keys which are:
        - In both objects
        - Have the same hashes

        Useful for determining why two mergeable datasets have differing hashes.
        """

        if not isinstance(other, HashableIndexes):
            raise TypeError(
                f"Cannot compare HashableIndexes with type {other.__class__.__name__}"
            )
        shared_keys = self.keys() & other.keys()

        return {key for key in shared_keys if self.dict[key] == other.dict[key]}

    def __xor__(self, other):
        """
        Return all keys which are:
        - In both objects
        - Have differing hashes

        Useful for determining two griß with differing hashes are mergeable
        """
        if not isinstance(other, HashableIndexes):
            raise TypeError(
                f"Cannot compare HashableIndexes with type {other.__class__.__name__}"
            )
        shared_keys = self.keys() & other.keys()

        return {key for key in shared_keys if self.dict[key] != other.dict[key]}

    def keys(self):
        return self.dict.keys()


def _add_month_start(time, n: int):
    """Add months to cftime datetime and truncate to start"""
    year = time.year + ((time.month + n - 1) // 12)
    month = (time.month + n - 1) % 12 + 1
    return time.replace(
        year=year, month=month, day=1, hour=0, minute=0, second=0, microsecond=0
    )


def _add_year_start(time, n: int):
    """Add years to cftime datetime and truncate to start"""
    return time.replace(
        year=time.year + n, month=1, day=1, hour=0, minute=0, second=0, microsecond=0
    )


def _guess_start_end_dates(ts, te, frequency):
    """Guess the start and end bounded times for a given frequency"""
    warnings.warn(
        "Time coordinate does not include bounds information. Guessing "
        "start and end times."
    )
    num, unit = frequency
    if unit == "yr":
        step_back = -int(num / 2)
        step_fwd = num + step_back
        ts = _add_year_start(ts, step_back)
        te = _add_year_start(te, step_fwd)
    elif unit == "mon":
        step_back = -int(num / 2)
        step_fwd = num + step_back
        ts = _add_month_start(ts, step_back)
        te = _add_month_start(te, step_fwd)
    elif unit == "day":
        dt = timedelta(days=num) / 2
        ts = ts - dt
        te = te + dt
    elif unit == "hr":
        dt = timedelta(hours=num) / 2
        ts = ts - dt
        te = te + dt
    else:
        warnings.warn("Cannot infer start and end times for subhourly frequencies.")
    return ts, te


def get_timeinfo(  # noqa: PLR0912, PLR0915 # Allow this func to be long and branching
    ds: xr.Dataset,
    filename_frequency: str | None,
    time_dim: str,
) -> tuple[str, str, str]:
    """
    Get start date, end date and frequency of a xarray dataset. Stolen and adapted from the
    cosima cookbook, see
    https://github.com/COSIMA/cosima-cookbook/blob/master/cosima_cookbook/database.py#L565

    Parameters
    ----------
    ds: :py:class:`xarray.Dataset`
        The dataset to parse the time info from
    filename_frequency: str
        Frequency as determined from the filename
    time_dim: str
        The name of the time dimension

    Returns
    -------
    start_date: str
        The start date of the dataset
    end_date: str
        The end date of the dataset
    frequency: str
        The frequency of the dataset

    Raises
    ------
    EmptyFileError
        If the dataset has a valid unlimited dimension, but no data
    """

    def _todate_monthly_nocalendar(time_var):
        """
        Convert time coordinates with units "months since X" to datetimes assuming a
        standard Georgian calendar where calendar is not otherwise defined.
        """
        # Get the reference date from the units
        ref_date = datetime.strptime(time_var.units, "months since %Y-%m-%d %H:%M:%S")

        # Calculate the date in question
        # relativedelta only takes integer months
        # relativedelta multiplication floors the multiplier
        dts = ref_date + time_var.to_numpy() * relativedelta(months=1)

        # Deal with partial months
        remainders = time_var.to_numpy() % 1
        if any(remainders != 0):
            dt_start_of_months = dts
            dt_end_of_months = dt_start_of_months + relativedelta(months=1)
            dt_month_offsets = (dt_end_of_months - dt_start_of_months) * remainders

            dts += dt_month_offsets

        # cftime.num2date returns a datetime or a numpy array of datetimes
        return dts if len(dts) > 1 else dts[0]

    def _todate(t):
        try:
            cal = time_var.calendar
        except AttributeError as e:
            # Some time data doesn't have a calendar specified but can still be
            # converted to datetimes - e.g. WOA23
            if "months since" in time_var.units:
                return _todate_monthly_nocalendar(time_var)

            raise e

        return cftime.num2date(t, time_var.units, calendar=cal)

    # Time format should be yyyy-mm-dd, hh:mm:ss
    time_format = "%Y-%m-%d, %H:%M:%S"
    # If year<1000, the leading zeros are usually missing
    time_str_expected_len = 20

    ts = None
    te = None
    frequency: str | tuple[int | None, str] = FREQUENCY_STATIC
    has_time = time_dim in ds

    if has_time:
        time_var = ds[time_dim]

        if len(time_var) == 0:
            raise EmptyFileError(
                "This file has a valid unlimited dimension, but no data"
            )

        has_bounds = hasattr(time_var, "bounds") and time_var.bounds in ds.variables
        if has_bounds:
            bounds_var = ds.variables[time_var.bounds]
            if np.isnan(bounds_var).all():
                has_bounds = False
            else:
                ts = _todate(bounds_var[0, 0])
                te = _todate(bounds_var[-1, 1])

        if ts is None and te is None:
            ts = _todate(time_var[0])
            te = _todate(time_var[-1])

        if len(time_var) > 1 or has_bounds:
            if has_bounds:
                t1 = _todate(bounds_var[0, 1])
            else:
                t1 = _todate(time_var[1])

            dt = t1 - ts
            # TODO: This is not a very good way to get the frequency
            if dt.days >= 365:  # noqa: PLR2004 # Allow magic number here
                years = round(dt.days / 365)
                frequency = (years, "yr")
            elif dt.days >= 28:  # noqa: PLR2004 # Allow magic number here
                months = round(dt.days / 30)
                frequency = (months, "mon")
            elif dt.days >= 1:
                frequency = (dt.days, "day")
            elif dt.seconds >= 3600:  # noqa: PLR2004 # Allow magic number here
                hours = round(dt.seconds / 3600)
                frequency = (hours, "hr")
            else:
                frequency = (None, "subhr")

    if filename_frequency:
        if filename_frequency != frequency:
            msg = (
                f"The frequency '{filename_frequency}' determined from filename does not "
                f"match the frequency '{frequency}' determined from the file contents."
            )
            if frequency == FREQUENCY_STATIC:
                frequency = filename_frequency
            warnings.warn(f"{msg} Using '{frequency}'.")

    if has_time & (frequency != FREQUENCY_STATIC):
        if not has_bounds:
            ts, te = _guess_start_end_dates(ts, te, frequency)

    if ts is None:
        start_date = "none"
    else:
        start_date = ts.strftime(time_format).rjust(time_str_expected_len, "0")

    if te is None:
        end_date = "none"
    else:
        end_date = te.strftime(time_format).rjust(time_str_expected_len, "0")

    if frequency[0]:
        frequency = f"{str(frequency[0])}{frequency[1]}"
    else:
        frequency = frequency[1]

    return start_date, end_date, frequency


@lru_cache(maxsize=10)
def open_dataset_cached(*args, **kwargs) -> xr.Dataset:
    """
    Cache xarray open dataset so that multiple opens of the same file can reuse
    the returned object. As we don't currently access the data variable data then
    we shouldn't need to worry about xarray's laziness and the Dataset object
    should have the needed info even if close()ed.
    """
    return xr.open_dataset(*args, **kwargs)


def _parse_variable_cell_methods(cell_methods: list[str]) -> str:
    """
    Take the `variable_cell_methods` list and turn it into the time_aggregation
    string by looking for known time aggregation methods.

    Variable cell methods may look like:
    'time: mean', 'time: sum', 'area: mean time: mean', etc.

    the `time_aggregation` string should be a comma-separated list of unique
    time aggregation methods found in the `variable_cell_methods` list.
    """
    time_aggs = set()

    for cell_method in cell_methods:
        # Match pattern: "time: <method>" where method includes parentheses and other characters
        matches = re.findall(r"\btime:\s*(\S+)", cell_method)
        time_aggs.update(matches)

    # Join unique aggregation methods, sorted for consistency
    ret = ",".join(sorted(time_aggs)) if time_aggs else "unknown"
    return ret
