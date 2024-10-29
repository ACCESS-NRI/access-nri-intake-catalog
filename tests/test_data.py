# Copyright 2024 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path
from unittest import mock

import pytest

import access_nri_intake
from access_nri_intake.data.utils import _get_catalog_rp, available_versions
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


@mock.patch("access_nri_intake.data.utils.get_catalog_fp")
@pytest.mark.parametrize(
    "cat", ["catalog/catalog-bad-path.yaml", "catalog/catalog-bad-structure.yaml"]
)
def test__get_catalog_rp_runtime_errors(mock_get_catalog_fp, test_data, cat):
    """
    Check that we correctly decipher to rootpath (rp) to the catalogs
    """
    mock_get_catalog_fp.return_value = test_data / cat
    assert (
        access_nri_intake.data.utils.get_catalog_fp() == test_data / cat
    ), "Mock failed"

    with pytest.raises(RuntimeError):
        _get_catalog_rp()


@mock.patch("access_nri_intake.data.utils._get_catalog_rp")
def test_available_versions(mock__get_catalog_rp, test_data):
    mock__get_catalog_rp.return_value = test_data / "catalog/catalog-dirs"
    cats = available_versions(pretty=False)
    assert cats == [
        "v2024-01-01",
        "v2024-06-19",
        "v2025-02-28",
    ], "Did not get expected catalog list"
