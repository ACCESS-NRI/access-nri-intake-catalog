# Copyright 2024 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import re
from unittest import mock

import pytest

import access_nri_intake
from access_nri_intake.data import CATALOG_NAME_FORMAT
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
        "v2025-02-28",
        "v2024-06-19",
        "v2024-01-01",
        "v2019-02-02",
    ], "Did not get expected catalog list"


@mock.patch("access_nri_intake.data.utils._get_catalog_rp")
def test_available_versions_pretty(mock__get_catalog_rp, test_data, capfd):
    mock__get_catalog_rp.return_value = test_data / "catalog/catalog-dirs"
    available_versions(pretty=True)
    captured, _ = capfd.readouterr()
    assert (
        captured == "v2025-02-28\nv2024-06-19\nv2024-01-01\nv2019-02-02(-->vN.N.N)\n"
    ), "Did not get expected catalog printout"


@pytest.mark.parametrize(
    "name",
    [
        "v2024-01-12",
        "v2024-02-18",
        "v2024-03-07",
        "v2024-04-22",
        "v2024-05-14",
        "v2024-06-19",
        "v2024-07-18",
        "v2024-08-01",
        "v2024-09-17",
        "v2024-10-11",
        "v2024-11-13",
        "v2024-12-29",
        "v2024-02-31",  # Regex not strict enough to avoid slightly wrong dates
        "v2024-04-31",  # Regex not strict enough to avoid slightly wrong dates
    ],
)
def test_catalog_name_format(name):
    m = re.match(CATALOG_NAME_FORMAT, name)
    assert m is not None, f"Failed to match correct version {name}"


@pytest.mark.parametrize(
    "name",
    [
        "2024-01-01",  # Forgot the leading v
        "v2024_01_01",  # Underscores instead of spaces
        "v1999-02-01",  # Regex only accepts dates > 2000-01-01
        "v2024-01-32",  # Out of bounds day (not month-sensitive)
        "v2024-01-00",  # Out of bounds day
        "v2024-13-02",  # Out of bounds month
        "v2024-00-02",  # Out of bounds month
        "v2024-01-01-243",  # Trailing gumph
        "v2024-01-02-v2",  # Trailing gumph
        "v0.1.2",  # Old-style version numbers
    ],
)
def test_catalog_name_format_bad(name):
    m = re.match(CATALOG_NAME_FORMAT, name)
    assert m is None, f"Incorrectly matched to bad version {name}"
