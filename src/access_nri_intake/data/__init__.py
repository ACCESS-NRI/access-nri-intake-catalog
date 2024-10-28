# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import warnings

import intake

from access_nri_intake.utils import get_catalog_fp

try:
    data = intake.open_catalog(get_catalog_fp()).access_nri
except FileNotFoundError:
    warnings.warn(
        "Unable to load catalog. Calling intake.cat.access_nri will not work.",
        RuntimeWarning,
    )
