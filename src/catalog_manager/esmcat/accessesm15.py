# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Builder for generating an intake-esm catalog from ACCESS-ESM1.5 data """

import re
import traceback
from pathlib import Path

import xarray as xr

from ecgtools.builder import INVALID_ASSET, TRACEBACK

from catalog_manager.esmcat.base import BaseBuilder, ParserError
from catalog_manager.esmcat.utils import get_timeinfo, strip_pattern_rh


class AccessEsm15Builder(BaseBuilder):
    """Intake-esm catalog builder for ACCESS-ESM1.5 datasets"""

    def __init__(self, path):
        """
        Initialise a AccessEsm15Builder

        Parameters
        ----------
        path : str or list of str
            Path or list of paths to crawl for assets/files.
        """

        kwargs = dict(
            path=path,
            depth=3,
            exclude_patterns=["*restart*"],
            include_patterns=["*.nc*"],
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
            # - iceh_m.2014-06.nc
            # - bz687a.pm107912_mon.nc
            # - bz687a.p7107912_mon.nc
            # - PI-GWL-B2035.pe-109904_dai.nc
            # - PI-1pct-02.pe-011802_dai.nc_dai.nc
            # - ocean_daily.nc-02531231
            file_id = strip_pattern_rh(["\d{4}[-_]\d{2}", "\d{6}", "\d{8}"], filename)

            match_groups = re.match(r".*/([^/]*)/history/([^/]*)/.*\.nc", file).groups()
            # experiment = match_groups[0]
            realm = match_groups[1]
            if realm == "atm":
                realm = "atmos"
            elif realm == "ocn":
                realm = "ocean"
            elif realm != "ice":
                raise ParserError(f"Could not translate {realm} to a realm")

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
