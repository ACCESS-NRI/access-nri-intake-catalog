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
