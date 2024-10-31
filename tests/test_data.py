# Copyright 2024 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path

import access_nri_intake
from access_nri_intake.utils import get_catalog_fp


def test_get_catalog_fp():
    """
    Check that we're getting the correct path to the catalog.yaml file. We need
    to ensure that this works both in editable & non-editable installs.
    """
    INSTALL_DIR = Path(access_nri_intake.__file__).parent
    expected_path = os.path.join(INSTALL_DIR, "data/catalog.yaml")

    catalog_fullpath = get_catalog_fp()

    assert str(catalog_fullpath) == expected_path
