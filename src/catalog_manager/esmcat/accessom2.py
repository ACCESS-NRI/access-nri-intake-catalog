# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Builder for generating an intake-esm catalog from ACCESS-OM2 COSIMA data """

import re
import traceback
from pathlib import Path

import xarray as xr

from ecgtools.builder import INVALID_ASSET, TRACEBACK

from .base import BaseBuilder
from .utils import get_timeinfo, strip_pattern_rh


class AccessOm2Builder(BaseBuilder):
    """Intake-esm catalog builder for ACCESS-OM2 COSIMA datasets"""

    def __init__(self, path):
        """
        Initialise a AccessOm2Builder

        Parameters
        ----------
        path : str or list of str
            Path or list of paths to crawl for assets/files.
        """

        kwargs = dict(
            path=path,
            depth=3,
            exclude_patterns=["*restart*", "*o2i.nc"],
            include_patterns=["*.nc"],
            data_format="netcdf",
            groupby_attrs=["file_id", "frequency"],
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
            filename = Path(file).stem

            # Get file id without any dates
            # - ocean-3d-v-1-monthly-pow02-ym_1958_04.nc
            # - iceh.057-daily.nc
            # - oceanbgc-3d-caco3-1-yearly-mean-y_2015.nc
            file_id = strip_pattern_rh(
                [r"\d{4}[-_]\d{2}", r"\d{4}", r"\d{3}"], filename
            )

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
                "file_id": file_id,
            }

            info["start_date"], info["end_date"], info["frequency"] = get_timeinfo(ds)

            return info

        except Exception:
            return {INVALID_ASSET: file, TRACEBACK: traceback.format_exc()}
