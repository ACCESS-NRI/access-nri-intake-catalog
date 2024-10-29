# Copyright 2024 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path
from unittest import mock

import access_nri_intake
from access_nri_intake.data.utils import _get_catalog_rp
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


@mock.patch("access_nri_intake.data.utils.get_catalog_fp")
def test__get_catalog_rp(mock_get_catalog_fp, test_data):
    """
    Check that we correctly decipher to rootpath (rp) to the catalogs
    """
    mock_get_catalog_fp.return_value = test_data / "catalog/catalog-good.yaml"
    assert (
        access_nri_intake.data.utils.get_catalog_fp()
        == test_data / "catalog/catalog-good.yaml"
    ), "Mock failed"

    rp = _get_catalog_rp()
    assert (
        rp == "/this/is/root/path/"
    ), f"Computed root path {rp} != expected value /this/is/root/path/"
