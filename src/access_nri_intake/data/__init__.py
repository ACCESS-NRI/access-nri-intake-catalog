# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import warnings

import intake
import intake.catalog
from IPython import get_ipython
from IPython.core.magic import register_line_magic

from access_nri_intake.utils import get_catalog_fp

from .telemetry import capture_datastore_searches

CATALOG_NAME_FORMAT = (
    r"^v(?P<yr>2[0-9]{3})\-(?P<mon>1[0-2]|0[1-9])\-(?P<day>0[1-9]|[1-2][0-9]|3[0-1])$"
)


def load_ipython_extension(ipython):
    @register_line_magic("capture_func_calls")
    def capture_func_calls(info):
        """
        Returns the function calls from the code in the cell
        """
        ipython.events.register("pre_run_cell", capture_datastore_searches)
        print("Function calls will be captured for the next cell execution")


# Register the extension
ip = get_ipython()
if ip:
    load_ipython_extension(ip)
    ip.run_line_magic("capture_func_calls", "")

try:
    data = intake.open_catalog(get_catalog_fp()).access_nri
except FileNotFoundError:
    warnings.warn(
        "Unable to access a default catalog location. Calling intake.cat.access_nri will not work.",
        RuntimeWarning,
        stacklevel=2,
    )
    data = intake.catalog.Catalog()
