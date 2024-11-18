# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import os
import warnings
from pathlib import Path

from pytest import fixture

here = os.path.abspath(os.path.dirname(__file__))


def _get_xfail():
    """
    Get the XFAILS environment variable. We use a default of 1, indicating we expect
    to add xfail marker to `test_parse_access_ncfile[AccessOm2Builder-access-om2/output000/ocean/ocean_grid.nc-expected0-True]`
    unless specified.
    """
    xfails_default = 1

    try:
        return int(os.environ["XFAILS"])
    except KeyError:
        warnings.warn(
            message=(
                "XFAILS enabled by default as coordinate discovery disabled by default. "
                "This will be deprecated when coordinate discovery is enabled by default"
            ),
            category=PendingDeprecationWarning,
        )
        return xfails_default


_add_xfail = _get_xfail()


@fixture(scope="session")
def test_data():
    return Path(os.path.join(here, "data"))


@fixture(scope="session")
def BASE_DIR(tmp_path_factory):
    yield tmp_path_factory.mktemp("catalog-dir")


def pytest_collection_modifyitems(config, items):
    """
    This function is called by pytest to modify the items collected during test
    collection. We use it here to mark the xfail tests in
    test_builders::test_parse_access_ncfile when we check the file contents & to
    ensure we correctly get xfails if we don't have cordinate discovery enabled
    in intake-esm.
    """
    for item in items:
        if (
            item.name
            in (
                "test_parse_access_ncfile[AccessOm2Builder-access-om2/output000/ocean/ocean_grid.nc-expected0-True]",
            )
            and _add_xfail
        ):
            item.add_marker("xfail")
