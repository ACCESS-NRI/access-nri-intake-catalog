# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Parser for generating an intake-esm catalog from COSIMA data """

import os
import re
import traceback

import xarray as xr

from ecgtools.builder import INVALID_ASSET, TRACEBACK

from . import BaseParser, CORE_INFO
from .utils import get_timeinfo


class CosimaParser(BaseParser):
    """Parser for COSIMA datasets"""

    @staticmethod
    def parser(file):
        try:
            # Use correct names for core columns
            path_name = CORE_INFO["path_column"]["name"]
            realm_name = CORE_INFO["realm_column"]["name"]
            variable_name = CORE_INFO["variable_column"]["name"]
            start_date_name = CORE_INFO["start_date_column"]["name"]
            end_date_name = CORE_INFO["end_date_column"]["name"]
            frequency_name = CORE_INFO["frequency_column"]["name"]

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
                path_name: str(file),
                realm_name: realm,
                variable_name: variable_list,
                "filename": filename,
            }

            (
                info[start_date_name],
                info[end_date_name],
                info[frequency_name],
            ) = get_timeinfo(ds)

            return info

        except Exception:
            return {INVALID_ASSET: file, TRACEBACK: traceback.format_exc()}
