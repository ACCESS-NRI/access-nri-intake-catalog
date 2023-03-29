# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Shared utilities for writing intake-esm parsers """

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
    # TODO: This function needs work

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
        if dt.days >= 365:
            years = round(dt.days / 365)
            frequency = f"{years}_yearly"
        elif dt.days >= 28:
            months = round(dt.days / 30)
            frequency = f"{months}_monthly"
        elif dt.days >= 1:
            frequency = f"{dt.days}_daily"
        else:
            frequency = f"{dt.seconds // 3600}_hourly"
    else:
        # single time value in this file and no averaging
        frequency = "static"

    return (
        start_time.strftime("%Y-%m-%d, %H:%M:%S"),
        end_time.strftime("%Y-%m-%d, %H:%M:%S"),
        frequency,
    )
