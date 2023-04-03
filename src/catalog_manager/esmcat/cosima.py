# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Parser for generating an intake-esm catalog from COSIMA data """

import os
import re
import traceback

import xarray as xr

from ecgtools.builder import INVALID_ASSET, TRACEBACK

from .base import BaseBuilder
from .utils import get_timeinfo


class CosimaBuilder(BaseBuilder):
    """Intake-esm catalog builder for COSIMA datasets"""

    def __init__(self, paths):
        """
        Initialise a CosimaBuilder

        Parameters
        ----------
        paths : list of str
            List of paths to crawl for assets/files.
        """

        kwargs = dict(
            paths=paths,
            depth=3,
            exclude_patterns=["*/restart*/*", "*o2i.nc"],
            include_patterns=["*.nc"],
            data_format="netcdf",
            groupby_attrs=["realm", "frequency"],
            aggregations=[
                {
                    "type": "join_existing",
                    "attribute_name": "start_date",
                    "options": {
                        "dim": "time",
                        "combine": "by_coords",
                    },
                }
            ],
        )

        super().__init__(**kwargs)

    @staticmethod
    def parser(file):
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

            info["start_date"], info["end_date"], info["frequency"] = get_timeinfo(ds)

            return info

        except Exception:
            return {INVALID_ASSET: file, TRACEBACK: traceback.format_exc()}
