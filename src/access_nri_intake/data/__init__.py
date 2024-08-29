# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import importlib

import intake

data = intake.open_catalog(
    importlib.resources.files("access_nri_intake").joinpath("data/catalog.yaml")
    # os.path.join(_here, "catalog.yaml")
).access_nri
