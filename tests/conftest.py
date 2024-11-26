# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import os
import warnings
from datetime import datetime
from pathlib import Path

from pytest import fixture

here = Path(__file__).parent


@fixture(scope="session")
def test_data():
    return Path(here / "data")


@fixture(scope="session")
def config_dir():
    return Path(here / "e2e/configs")


@fixture(scope="session")
def BASE_DIR(tmp_path_factory):
    yield tmp_path_factory.mktemp("catalog-dir")


@fixture(scope="session")
def v_num():
    return datetime.now().strftime("v%Y-%m-%d")


def pytest_addoption(parser):
    parser.addoption(
        "--e2e",
        action="store_true",
        default=False,
        help="Run end-to-end tests",
        dest="e2e",
    )


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


def pytest_collection_modifyitems(config, items):
    """
    This function is called by pytest to modify the items collected during test
    collection. We use it here to mark the xfail tests in
    test_builders::test_parse_access_ncfile when we check the file contents & to
    ensure we correctly get xfails if we don't have cordinate discovery enabled
    in intake-esm.

    The commented out lines are tests which *could* reasonably be expected to fail,
    but don't because they don't have any unused coordinates.
    """
    for item in items:
        if (
            item.name
            in (
                "test_parse_access_ncfile[AccessOm2Builder-access-om2/output000/ocean/ocean_grid.nc-expected0-True]",
                # "test_parse_access_ncfile[AccessOm2Builder-access-om2/output000/ocean/ocean.nc-expected1-True]",
                # "test_parse_access_ncfile[AccessOm2Builder-access-om2/output000/ocean/ocean_month.nc-expected2-True]",
                # "test_parse_access_ncfile[AccessOm2Builder-access-om2/output000/ocean/ocean_month_inst_nobounds.nc-expected3-True]",
                # "test_parse_access_ncfile[AccessOm2Builder-access-om2/output000/ice/OUTPUT/iceh.1900-01.nc-expected4-True]",
                # "test_parse_access_ncfile[AccessCm2Builder-access-cm2/by578/history/atm/netCDF/by578a.pd201501_dai.nc-expected5-True]",
                # "test_parse_access_ncfile[AccessCm2Builder-access-cm2/by578/history/ice/iceh_d.2015-01.nc-expected6-True]",
                # "test_parse_access_ncfile[AccessCm2Builder-access-cm2/by578/history/ocn/ocean_daily.nc-20150630-expected7-True]",
                # "test_parse_access_ncfile[AccessCm2Builder-access-cm2/by578/history/ocn/ocean_scalar.nc-20150630-expected8-True]",
                # "test_parse_access_ncfile[AccessEsm15Builder-access-esm1-5/history/atm/netCDF/HI-C-05-r1.pa-185001_mon.nc-expected9-True]",
                # "test_parse_access_ncfile[AccessEsm15Builder-access-esm1-5/history/ice/iceh.1850-01.nc-expected10-True]",
                # "test_parse_access_ncfile[AccessEsm15Builder-access-esm1-5/history/ocn/ocean_bgc_ann.nc-18501231-expected11-True]",
                # "test_parse_access_ncfile[AccessEsm15Builder-access-esm1-5/history/ocn/ocean_bgc.nc-18501231-expected12-True]",
                # "test_parse_access_ncfile[AccessOm3Builder-access-om3/output000/GMOM_JRA_WD.mom6.h.native_1900_01.nc-expected13-True]",
                # "test_parse_access_ncfile[AccessOm3Builder-access-om3/output000/GMOM_JRA_WD.mom6.h.sfc_1900_01_02.nc-expected14-True]",
                # "test_parse_access_ncfile[AccessOm3Builder-access-om3/output000/GMOM_JRA_WD.mom6.h.static.nc-expected15-True]",
                # "test_parse_access_ncfile[AccessOm3Builder-access-om3/output000/GMOM_JRA_WD.mom6.h.z_1900_01.nc-expected16-True]",
                # "test_parse_access_ncfile[AccessOm3Builder-access-om3/output000/GMOM_JRA_WD.cice.h.1900-01-01.nc-expected17-True]",
                # "test_parse_access_ncfile[AccessOm3Builder-access-om3/output000/GMOM_JRA_WD.ww3.hi.1900-01-02-00000.nc-expected18-True]",
                "test_parse_access_ncfile[Mom6Builder-mom6/output000/19000101.ice_daily.nc-expected19-True]",
                "test_parse_access_ncfile[Mom6Builder-mom6/output000/19000101.ocean_annual_z.nc-expected20-True]",
                "test_parse_access_ncfile[Mom6Builder-mom6/output000/19000101.ocean_month_rho2.nc-expected21-True]",
                # "test_parse_access_ncfile[Mom6Builder-mom6/output000/19000101.ocean_scalar_annual.nc-expected22-True]",
                "test_parse_access_ncfile[Mom6Builder-mom6/output000/19000101.ocean_static.nc-expected23-True]",
                # "test_parse_access_ncfile[Mom6Builder-mom6/output053/20051101.ocean_daily_2005_360.nc-expected24-True]",
                "test_parse_access_ncfile[Mom6Builder-mom6/output053/20051101.ocean_daily_rho2_2005_360.nc-expected25-True]",
                "test_parse_access_ncfile[Mom6Builder-mom6/output053/20051101.ocean_daily_z_2005_360.nc-expected26-True]",
            )
            and _add_xfail
        ):
            item.add_marker("xfail")
