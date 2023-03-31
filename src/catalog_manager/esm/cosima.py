# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Parser for generating an intake-esm catalog from COSIMA data """

import os
import re
import traceback

import xarray as xr

from ecgtools.builder import INVALID_ASSET, TRACEBACK

from .base import BaseParser
from .utils import get_timeinfo


class CosimaParser(BaseParser):
    """Parser for COSIMA datasets"""

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
