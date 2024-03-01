# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Shared utilities for writing Intake-ESM builders and their parsers """

import re
import warnings
from datetime import timedelta
from pathlib import Path

import cftime
import xarray as xr


class EmptyFileError(Exception):
    pass


def _add_month_start(time, n):
    """Add months to cftime datetime and truncate to start"""
    year = time.year + ((time.month + n - 1) // 12)
    month = (time.month + n - 1) % 12 + 1
    return time.replace(
        year=year, month=month, day=1, hour=0, minute=0, second=0, microsecond=0
    )


def _add_year_start(time, n):
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


def get_timeinfo(ds, filename_frequency, time_dim):
    """
    Get start date, end date and frequency of a xarray dataset. Stolen and adapted from the
    cosima cookbook, see
    https://github.com/COSIMA/cosima-cookbook/blob/master/cosima_cookbook/database.py#L565

    Parameters
    ----------
    ds: :py:class:`xarray.Dataset`
        The dataset to parse the time info from
    time_dim: str
        The name of the time dimension
    """

    def _todate(t):
        return cftime.num2date(t, time_var.units, calendar=time_var.calendar)

    time_format = "%Y-%m-%d, %H:%M:%S"
    ts = None
    te = None
    frequency = "fx"
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
            ts = _todate(bounds_var[0, 0])
            te = _todate(bounds_var[-1, 1])
        else:
            ts = _todate(time_var[0])
            te = _todate(time_var[-1])

        if len(time_var) > 1 or has_bounds:
            if has_bounds:
                t1 = _todate(bounds_var[0, 1])
            else:
                t1 = _todate(time_var[1])

            dt = t1 - ts
            # TODO: This is not a very good way to get the frequency
            if dt.days >= 365:
                years = round(dt.days / 365)
                frequency = (years, "yr")
            elif dt.days >= 28:
                months = round(dt.days / 30)
                frequency = (months, "mon")
            elif dt.days >= 1:
                frequency = (dt.days, "day")
            elif dt.seconds >= 3600:
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
            if frequency == "fx":
                frequency = filename_frequency
            warnings.warn(f"{msg} Using '{frequency}'.")

    if has_time & (frequency != "fx"):
        if not has_bounds:
            ts, te = _guess_start_end_dates(ts, te, frequency)

    if ts is None:
        start_date = "none"
    else:
        start_date = ts.strftime(time_format)

    if te is None:
        end_date = "none"
    else:
        end_date = te.strftime(time_format)

    if frequency[0]:
        frequency = f"{str(frequency[0])}{frequency[1]}"
    else:
        frequency = frequency[1]

    return start_date, end_date, frequency


def parse_access_filename(filename):
    """
    Parse an ACCESS model filename and return a file id and any time information

    Parameters
    ----------
    filename: str
        The filename to parse with the extension removed

    Returns
    -------
    file_id: str
        The file id constructed by redacting time information and replacing non-python characters
        with underscores
    timestamp: str
        A string of the redacted time information (e.g. "1990-01")
    frequency: str
        The frequency of the file if available in the filename
    """

    # ACCESS output file patterns
    # TODO: these should be defined per driver to prevent new patterns from breaking old drivers
    not_multi_digit = "(?:\\d(?!\\d)|[^\\d](?=\\d)|[^\\d](?!\\d))"
    om3_components = "(?:cice|mom6|ww3)"
    ymds = "\\d{4}[_,-]\\d{2}[_,-]\\d{2}[_,-]\\d{5}"
    ymd = "\\d{4}[_,-]\\d{2}[_,-]\\d{2}"
    ym = "\\d{4}[_,-]\\d{2}"
    y = "\\d{4}"
    patterns = [
        rf"^iceh.*\.({ymd}|{ym})$",  # ACCESS-ESM1.5/OM2 ice
        rf"^iceh.*\.({ym})-{not_multi_digit}.*",  # ACCESS-CM2 ice
        rf"^iceh.*\.(\d{{3}})-{not_multi_digit}.*",  # ACCESS-OM2 ice
        rf"^ocean.*[_,-](?:ymd|ym|y)_({ymd}|{ym}|{y})(?:$|[_,-]{not_multi_digit}.*)",  # ACCESS-OM2 ocean
        r"^ocean.*[^\d]_(\d{2})$",  # A few wierd files in ACCESS-OM2 01deg_jra55v13_ryf9091
        r"^.*\.p.(\d{6})_.*",  # ACCESS-CM2 atmosphere
        r"^.*\.p.-(\d{6})_.*",  # ACCESS-ESM1.5 atmosphere
        rf"[^\.]*\.{om3_components}\..*({ymds}|{ymd}|{ym})$",  # ACCESS-OM3
    ]
    # Frequency translations
    frequencies = {
        "daily": (1, "day"),
        "_dai$": (1, "day"),
        "month": (1, "mon"),
        "_mon$": (1, "mon"),
        "yearly": (1, "yr"),
        "_ann$": (1, "yr"),
    }
    redaction_fill = "X"

    # Try to determine frequency
    frequency = None
    for pattern, freq in frequencies.items():
        if re.search(pattern, filename):
            frequency = freq
            break

    # Parse file id
    file_id = filename
    timestamp = None
    for pattern in patterns:
        match = re.match(pattern, file_id)
        if match:
            timestamp = match.group(1)
            redaction = re.sub(r"\d", redaction_fill, timestamp)
            file_id = file_id[: match.start(1)] + redaction + file_id[match.end(1) :]
            break

    # Remove non-python characters from file ids
    file_id = re.sub(r"[-.]", "_", file_id)
    file_id = re.sub(r"_+", "_", file_id).strip("_")

    return file_id, timestamp, frequency


def parse_access_ncfile(file, time_dim="time"):
    """
    Get Intake-ESM datastore entry info from an ACCESS netcdf file

    Parameters
    ----------
    file: str
        The path to the netcdf file
    time_dim: str
        The name of the time dimension

    Returns
    -------
    """

    file = Path(file)
    filename = file.name

    file_id, filename_timestamp, filename_frequency = parse_access_filename(file.stem)

    with xr.open_dataset(
        file,
        chunks={},
        decode_cf=False,
        decode_times=False,
        decode_coords=False,
    ) as ds:
        variable_list = []
        variable_long_name_list = []
        variable_standard_name_list = []
        variable_cell_methods_list = []
        variable_units_list = []
        for var in ds.data_vars:
            attrs = ds[var].attrs
            if "long_name" in attrs:
                variable_list.append(var)
                variable_long_name_list.append(attrs["long_name"])
                if "standard_name" in attrs:
                    variable_standard_name_list.append(attrs["standard_name"])
                else:
                    variable_standard_name_list.append("")
                if "cell_methods" in attrs:
                    variable_cell_methods_list.append(attrs["cell_methods"])
                else:
                    variable_cell_methods_list.append("")
                if "units" in attrs:
                    variable_units_list.append(attrs["units"])
                else:
                    variable_units_list.append("")

        start_date, end_date, frequency = get_timeinfo(ds, filename_frequency, time_dim)

    if not variable_list:
        raise EmptyFileError("This file contains no variables")

    outputs = (
        filename,
        file_id,
        filename_timestamp,
        frequency,
        start_date,
        end_date,
        variable_list,
        variable_long_name_list,
        variable_standard_name_list,
        variable_cell_methods_list,
        variable_units_list,
    )

    return outputs
