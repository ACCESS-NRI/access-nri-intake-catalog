# Copyright 2024 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from unittest import mock

import pytest

import access_nri_intake
from access_nri_intake.data.utils import _get_catalog_rp, available_versions


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
        "v2025-02-28(latest-2,latest-1)",
        "v2024-06-19(latest-0)",
        "v2024-01-01",
    ], "Did not get expected catalog list"


@mock.patch("access_nri_intake.data.utils._get_catalog_rp")
def test_available_versions_pretty(mock__get_catalog_rp, test_data, capfd):
    mock__get_catalog_rp.return_value = test_data / "catalog/catalog-dirs"
    available_versions(pretty=True)
    captured, _ = capfd.readouterr()
    assert (
        captured
        == "v2025-02-28(latest-2,latest-1)\nv2024-06-19(latest-0)\nv2024-01-01\n"
    ), "Did not get expected catalog printout"
