# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Shared utilities for writing Intake-ESM builders and their parsers """

import re
import warnings
from pathlib import Path

import cftime
import xarray as xr


class EmptyFileError(Exception):
    pass


def get_timeinfo(ds, time_dim="time"):
    """
    Get start time, end time and frequency of a xarray dataset. Stolen and slightly adapted
    from cosima cookbook, see
    https://github.com/COSIMA/cosima-cookbook/blob/master/cosima_cookbook/database.py#L565

    Parameters
    ----------
    ds: :py:class:`xarray.Dataset`
        The dataset to parse the time info from
    time_dim: str
        The name of the time dimension
    """

    time_var = ds[time_dim]
    has_bounds = hasattr(time_var, "bounds") and time_var.bounds in ds.variables

    if len(time_var) == 0:
        raise EmptyFileError("This file has a valid unlimited dimension, but no data")

    def _todate(t):
        return cftime.num2date(t, time_var.units, calendar=time_var.calendar)

    if has_bounds:
        bounds_var = ds.variables[time_var.bounds]
        start_time = _todate(bounds_var[0, 0])
        end_time = _todate(bounds_var[-1, 1])
    else:
        start_time = _todate(time_var[0])
        end_time = _todate(time_var[-1])

    if len(time_var) > 1 or has_bounds:
        if has_bounds:
            next_time = _todate(bounds_var[0, 1])
        else:
            next_time = _todate(time_var[1])

        dt = next_time - start_time
        # TODO: This is not a very good way to get the frequency
        if dt.days >= 365:
            years = round(dt.days / 365)
            frequency = f"{years}yr"
        elif dt.days >= 28:
            months = round(dt.days / 30)
            frequency = f"{months}mon"
        elif dt.days >= 1:
            frequency = f"{dt.days}day"
        else:
            frequency = f"{dt.seconds // 3600}hr"
    else:
        # single time value in this file and no averaging
        frequency = "fx"

    return (
        start_time.strftime("%Y-%m-%d, %H:%M:%S"),
        end_time.strftime("%Y-%m-%d, %H:%M:%S"),
        frequency,
    )


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
    patterns = {
        r"^iceh.*\.(\d{4}-\d{2}-\d{2})$",
        r"^iceh.*\.(\d{4}-\d{2})$",
        r"^iceh.*\.(\d{4}-\d{2})-.[^\d].*",
        r"^iceh.*\.(\d{3})-.[^\d].*",
        r"^ocean.*[^\d]_(\d{4}_\d{2}_\d{2})$",
        r"^ocean.*[^\d]_(\d{4}_\d{2})$",
        r"^ocean.*[^\d]_(\d{4})$",
        r"^ocean.*[^\d]_(\d{2})$",
        r"^.*\.p.(\d{6})_.*",
        r"^.*\.p.-(\d{6})_.*",
    }
    # Frequency translations
    frequencies = {
        "daily": "1day",
        "_dai$": "1day",
        "month": "1mon",
        "_mon$": "1mon",
        "yearly": "1yr",
        "_ann$": "1yr",
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


def parse_access_ncfile(file):
    """
    Get Intake-ESM datastore entry info from an ACCESS netcdf file

    Parameters
    ----------
    file: str
        The path to the netcdf file

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
        for var in ds.data_vars:
            attrs = ds[var].attrs
            if "long_name" in attrs:
                variable_list.append(var)
                variable_long_name_list.append(attrs["long_name"])
            if "standard_name" in attrs:
                variable_standard_name_list.append(attrs["standard_name"])
            if "cell_methods" in attrs:
                variable_cell_methods_list.append(attrs["cell_methods"])

        start_date, end_date, frequency = get_timeinfo(ds)

    if filename_frequency:
        if filename_frequency != frequency:
            msg = (
                f"The frequency '{filename_frequency}' determined from filename {filename} does not "
                f"match the frequency '{frequency}' determined from the file contents."
            )
            if frequency == "fx":
                frequency = filename_frequency
            warnings.warn(f"{msg} Using '{frequency}'.")

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
    )

    return outputs
