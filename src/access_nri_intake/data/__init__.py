# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import warnings

import intake
import intake.catalog

from access_nri_intake.utils import get_catalog_fp

CATALOG_NAME_FORMAT = r"^v(?P<yr>[0-9]{4})\-(?P<mon>[0-9]{2})\-(?P<day>[0-9]{2})$"
CATALOG_LATEST_FORMAT = r"^latest-(?P<major_vers>[0-9]*)$"

try:
    data = intake.open_catalog(get_catalog_fp()).access_nri
except FileNotFoundError:
    warnings.warn(
        "Unable to access catalog location. Calling intake.cat.access_nri will not work.",
        RuntimeWarning,
    )
    data = intake.catalog.Catalog()
