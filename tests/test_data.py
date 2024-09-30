# Copyright 2024 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import os

from access_nri_intake.utils import get_catalog_fp


def test_get_catalog_fp():
    _oneup = os.path.abspath(os.path.dirname("../"))
    assert str(get_catalog_fp()) == str(
        os.path.join(
            _oneup, "access-nri-intake-catalog/src/access_nri_intake/data/catalog.yaml"
        )
    )
