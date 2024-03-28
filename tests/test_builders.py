# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import intake
import pandas as pd
import pytest

from access_nri_intake.source import CORE_COLUMNS, builders


@pytest.mark.parametrize(
    "basedirs, builder, kwargs, num_assets, num_valid_assets, num_datasets",
    [
        (["access-om2"], "AccessOm2Builder", {}, 12, 12, 6),
        (
            ["access-cm2/by578", "access-cm2/by578a"],
            "AccessCm2Builder",
            {"ensemble": True},
            18,
            14,
            7,
        ),
        (["access-esm1-5"], "AccessEsm15Builder", {"ensemble": False}, 11, 11, 11),
        (["access-om3"], "AccessOm3Builder", {}, 12, 12, 6),
    ],
)
def test_builder_build(
    tmp_path,
    test_data,
    basedirs,
    builder,
    kwargs,
    num_assets,
    num_valid_assets,
    num_datasets,
):
    """
    Test the various steps of the build process
    """
    Builder = getattr(builders, builder)
    path = [str(test_data / Path(basedir)) for basedir in basedirs]
    builder = Builder(path, **kwargs)

    builder.get_assets()
    assert isinstance(builder.assets, list)
    assert len(builder.assets) == num_assets

    builder.build()
    assert isinstance(builder.df, pd.DataFrame)
    assert len(builder.df) == num_valid_assets
    assert all([col in builder.df.columns for col in CORE_COLUMNS])

    builder.save(name="test", description="test datastore", directory=str(tmp_path))

    cat = intake.open_esm_datastore(
        str(tmp_path / "test.json"),
        columns_with_iterables=builder.columns_with_iterables,
    )
    assert len(cat.df) == num_valid_assets
    assert len(cat) == num_datasets


@pytest.mark.parametrize(
    "filename, builder, realm, member, file_id",
    [
        (
            "access-om2/output000/ocean/ocean.nc",
            "AccessOm2Builder",
            "ocean",
            None,
            "ocean",
        ),
        (
            "access-om2/output000/ice/OUTPUT/iceh.1900-01.nc",
            "AccessOm2Builder",
            "seaIce",
            None,
            "iceh_XXXX_XX",
        ),
        (
            "access-cm2/by578/history/atm/netCDF/by578a.pd201501_dai.nc",
            "AccessCm2Builder",
            "atmos",
            "by578",
            "a_pdXXXXXX_dai",
        ),
        (
            "access-cm2/by578a/history/atm/netCDF/by578aa.pd201501_dai.nc",
            "AccessCm2Builder",
            "atmos",
            "by578a",
            "a_pdXXXXXX_dai",
        ),
        (
            "access-cm2/by578/history/ice/iceh_d.2015-01.nc",
            "AccessCm2Builder",
            "seaIce",
            "by578",
            "iceh_d_XXXX_XX",
        ),
        (
            "access-cm2/by578/history/ocn/ocean_daily.nc-20150630",
            "AccessCm2Builder",
            "ocean",
            "by578",
            "ocean_daily",
        ),
        (
            "access-om3/output000/GMOM_JRA_WD.mom6.h.sfc_1900_01_02.nc",
            "AccessOm3Builder",
            "ocean",
            None,
            "GMOM_JRA_WD_mom6_h_sfc_XXXX_XX_XX",
        ),
        (
            "access-om3/output000/GMOM_JRA_WD.cice.h.1900-01-01.nc",
            "AccessOm3Builder",
            "seaIce",
            None,
            "GMOM_JRA_WD_cice_h_XXXX_XX_XX",
        ),
        (
            "access-om3/output000/GMOM_JRA_WD.ww3.hi.1900-01-02-00000.nc",
            "AccessOm3Builder",
            "wave",
            None,
            "GMOM_JRA_WD_ww3_hi_XXXX_XX_XX_XXXXX",
        ),
    ],
)
def test_builder_parser(test_data, filename, builder, realm, member, file_id):
    Builder = getattr(builders, builder)
    info = Builder.parser(str(test_data / filename))
    assert info["realm"] == realm
    if member:
        assert info["member"] == member
    assert info["file_id"] == file_id


def test_builder_columns_with_iterables(test_data):
    builder = builders.AccessOm2Builder(str(test_data / "access-om2"))
    assert not builder.columns_with_iterables
    builder.build()
    assert sorted(list(builder.columns_with_iterables)) == sorted(
        [
            col
            for col, val in builder.df.applymap(type)
            .isin([list, tuple, set])
            .any()
            .items()
            if val
        ]
    )
