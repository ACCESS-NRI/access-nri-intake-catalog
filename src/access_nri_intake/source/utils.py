# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Shared utilities for writing Intake-ESM builders and their parsers """

import re

import cftime


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

    if time_dim is None:
        return None

    time_var = ds[time_dim]
    has_bounds = hasattr(time_var, "bounds") and time_var.bounds in ds.variables

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


def redact_time_stamps(string, fill="X"):
    """
    Sequentially try to redact time stamps from a filename string, starting from the right hand side.
    Then replace any "-" and "." with "_". E.g. "bz687a.pm107912_mon.nc" is redacted to
    bz687a.pmXXXXXX_mon.nc

    Parameters
    ----------
    string: str
        A filename with the suffix (e.g. .nc) removed
    fill: str, optional
        The string to replace the digits in the time stamp with
    """

    # TODO: this function is a horrible hack

    # Patterns are removed in this order. Matching stops once a match is made
    patterns = [
        r"\d{4}[-_]\d{2}[-_]\d{2}",
        r"\d{4}[-_]\d{2}",
        r"\d{8}",
        r"\d{6}",
        r"\d{4}",
        r"\d{3}",
        r"\d{2}",
    ]

    # Strip first matched pattern
    stripped = string
    for pattern in patterns:
        match = re.match(rf"^.*({pattern}(?!.*{pattern})).*$", stripped)
        if match:
            replace = re.sub(r"\d", fill, match.group(1))
            stripped = stripped[: match.start(1)] + replace + stripped[match.end(1) :]
            break

    # Enforce Python characters
    stripped = re.sub(r"[-.]", "_", stripped)

    # Remove any double or dangling _
    return re.sub(r"__", "_", stripped).strip("_")
