# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import warnings

import intake
import intake.catalog

from access_nri_intake.utils import get_catalog_fp

# FIXME remove all references to this, reference top-level version

try:
    data = intake.open_catalog(get_catalog_fp()).access_nri
except FileNotFoundError:
    warnings.warn(
        "Unable to access a default catalog location. Calling intake.cat.access_nri will not work.",
        RuntimeWarning,
        stacklevel=2,
    )
    data = intake.catalog.Catalog()
