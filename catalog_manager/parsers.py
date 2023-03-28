# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import os
import re
import traceback

import cftime

import xarray as xr

from ecgtools.builder import INVALID_ASSET, TRACEBACK

# Parsers should return a dictionary containing at least the following
# keys:
#
# "path":       the file path
# "realm":      e.g. ocean, atmosphere...
# "variable":   the variable(s) in the file
# "frequency":  the temporal frequency of the data
# "start_date": the start date of the data as %Y-%m-%d, %H:%M:%S
# "end_date":   the end date of the data as %Y-%m-%d, %H:%M:%S
#
# TODO: this should be explicitly checked


def _get_timeinfo(ds):
    """
    Stolen and slightly adapted from cosima cookbook
    https://github.com/COSIMA/cosima-cookbook/blob/master/cosima_cookbook/database.py#L565
    """
    time_dim = "time"  # TODO: this probably shouldn't be hardcoded
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


def cosima_parser(file):
    """Parser for COSIMA datasets"""
    try:
        filename = os.path.basename(file)
        match_groups = re.match(
            r".*/([^/]*)/([^/]*)/output\d+/([^/]*)/.*\.nc", file
        ).groups()
        # configuration = match_groups[0]
        # experiment = match_groups[1]
        realm = match_groups[2]

        with xr.open_dataset(file, chunks={}, decode_times=False) as ds:
            variable_list = [var for var in ds if "long_name" in ds[var].attrs]

        info = {
            "path": str(file),
            "realm": realm,
            "variable": variable_list,
            "filename": filename,
        }

        # match = re.match(".*cycle(\d)", experiment)
        # if match is not None:
        #     info["cycle"] = int(match.groups()[0])

        info["start_date"], info["end_date"], info["frequency"] = _get_timeinfo(ds)

        return info

    except Exception:
        return {INVALID_ASSET: file, TRACEBACK: traceback.format_exc()}
