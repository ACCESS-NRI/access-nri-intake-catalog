# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import os

import intake

_here = os.path.abspath(os.path.dirname(__file__))
data = intake.open_catalog(os.path.join(_here, "catalog.yaml")).access_nri
