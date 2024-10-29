# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import warnings

import intake
import intake.catalog

from access_nri_intake.utils import get_catalog_fp

try:
    data = intake.open_catalog(get_catalog_fp()).access_nri
except FileNotFoundError:
    warnings.warn(
        "Unable to access catalog location. Calling intake.cat.access_nri will not work.",
        RuntimeWarning,
    )
    data = intake.catalog.Catalog()
