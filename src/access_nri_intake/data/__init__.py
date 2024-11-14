# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import intake

from access_nri_intake.utils import get_catalog_fp

data = intake.open_catalog(get_catalog_fp()).access_nri
