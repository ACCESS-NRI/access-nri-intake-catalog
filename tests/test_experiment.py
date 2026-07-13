# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0


import shutil
import warnings
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest
from intake_esm import esm_datastore

from access_nri_intake.experiment.core import find_esm_datastore, use_datastore
from access_nri_intake.experiment.utils import (
    DataStoreWarning,
    LoginNodeWarning,
    MultipleDataStoreError,
    parse_kwarg,
    validate_args,
    verify_ds_current,
    warn_if_login_node,
)
from access_nri_intake.source import builders
from access_nri_intake.source.builders import Builder


@pytest.mark.parametrize(
    "subdir, datastore_name ,expected",
    [
        ("single_match", "ds", True),
        ("single_match", "wrong_name", False),
        ("multi_matches", "ds", "err"),
        ("no_matches", "ds", False),
        ("multi_json_single_csv", "experiment_datastore", True),
    ],
)
def test_find_esm_datastore(test_data, subdir, datastore_name, expected):
    dir = test_data / "experiment_dirs" / subdir

    if expected != "err":
        ds = find_esm_datastore(dir, datastore_name)
        assert bool(ds) == expected
    else:
        with pytest.raises(MultipleDataStoreError):
            find_esm_datastore(dir, datastore_name)


@pytest.mark.parametrize(
    "basedir, builder, kwargs, num_assets",
    [
        ("access-om2", "AccessOm2Builder", {}, 12),
        (
            "access-cm2",
            "AccessCm2Builder",
            {"ensemble": True},
            10,
        ),  # This was 18 - changed it to ten, I think because of crawl-depth & by578 / by578a
        ("access-esm1-5", "AccessEsm15Builder", {"ensemble": False}, 11),
        ("access-om3", "AccessOm3Builder", {}, 14),
        ("mom6", "Mom6Builder", {}, 27),
    ],
)
@pytest.mark.parametrize(
    "open_ds, return_type", [(True, esm_datastore), (False, type(None))]
)
@pytest.mark.parametrize("use_path", [True, False])
@pytest.mark.filterwarnings("ignore:Unable to parse 1 assets")
def test_use_datastore(
    test_data: Path,
    basedir,
    builder,
    kwargs,
    num_assets,
    tmp_path,
    open_ds,
    return_type,
    use_path,
    capsys,
):
    """
    Run the `use_datastore` function on a bunch of different builders to make sure
    they all work as expected.
    """
    srcdir, destdir = test_data / basedir, tmp_path / "tests" / "data" / basedir

    shutil.copytree(src=srcdir, dst=destdir)

    invalid_asset = destdir / "invalid_asset.nc"
    invalid_asset.touch()  # Create an empty file to simulate an invalid asset

    basedir = [str(destdir)]
    # I think the str wrapper here is a bug- type hint implies we can pass a single string
    builder_type: Builder = getattr(builders, builder)
    builder = builder_type(basedir, **kwargs)
    builder.build()

    assert isinstance(builder.assets, list)
    assert len(builder.assets) == num_assets + 1  # +1 for the invalid asset

    exptdir = Path(basedir[0]) if use_path else basedir[0]
    # This creates a bunch of datastoers that we don't actually want here.
    ret = use_datastore(
        experiment_dir=exptdir,
        builder=builder_type,
        open_ds=open_ds,
        builder_kwargs=kwargs,
    )
    assert isinstance(ret, return_type)

    assert len(builder.invalid_assets) == 1
    assert len(builder.assets) - len(builder.invalid_assets) == num_assets
    # ^ This check looks really stupid, but we don't have a `builder.valid_assets`
    # property. I think we want to add this tbh...

    captured = capsys.readouterr()
    assert "generating new datastore" in captured.out

    # The invalid asset should be reported to the user in a CSV file.
    invalid_files = list(destdir.glob("experiment_datastore_invalid_assets_*.csv"))
    assert len(invalid_files) == 1
    invalid_df = pd.read_csv(invalid_files[0])
    assert len(invalid_df) == 1
    assert Path(invalid_df.loc[0, "INVALID_ASSET"]).stem == "invalid_asset"
    assert "Some assets were not included in the datastore" in captured.out

    if not open_ds:
        assert "To open the datastore" in captured.out
    else:
        assert "To open the datastore" not in captured.out


def test_use_datastore_existing(
    test_data: Path,
    tmp_path,
    capsys,
):
    """
    Run the `use_datastore` function on a bunch of different builders to make sure
    they all work as expected.
    """
    srcdir, destdir = (
        test_data / "access-om2",
        tmp_path / "tests" / "data" / "access-om2",
    )

    shutil.copytree(src=srcdir, dst=destdir)
    basedir = [str(destdir)]
    # I think the str wrapper here is a bug- type hint implies we can pass a single string
    builder_type: Builder = getattr(builders, "AccessOm2Builder")
    builder = builder_type(basedir, **{})
    builder.get_assets()

    assert isinstance(builder.assets, list)

    # This creates a bunch of datastoers that we don't actually want here.
    ret = use_datastore(
        experiment_dir=Path(basedir[0]),
        builder=builder_type,
        open_ds=False,
        builder_kwargs={},
    )
    capsys.readouterr()  # Clear the buffer so we only assert on the second run

    # Run it again so that we can test the case where the datastore already exists
    ret = use_datastore(
        experiment_dir=Path(basedir[0]),
        builder=builder_type,
        open_ds=True,
        builder_kwargs={},
    )
    assert isinstance(ret, esm_datastore)

    captured = capsys.readouterr()

    assert "Datastore found in " in captured.out
    assert "Datastore integrity verified!" in captured.out
    assert "Saving esm-datastore" not in captured.out  # Save must be skipped


def test_use_datastore_broken_existing(
    test_data: Path,
    tmp_path,
    capsys,
):
    """
    Run the `use_datastore` function on a bunch of different builders to make sure
    they all work as expected.
    """
    srcdir, destdir = (
        test_data / "access-om2",
        tmp_path / "tests" / "data" / "access-om2",
    )

    shutil.copytree(src=srcdir, dst=destdir)
    basedir = [str(destdir)]
    # I think the str wrapper here is a bug- type hint implies we can pass a single string
    builder_type: Builder = getattr(builders, "AccessOm2Builder")
    builder = builder_type(basedir, **{})
    builder.get_assets()

    assert isinstance(builder.assets, list)

    # This creates a bunch of datastores that we don't actually want here.
    ret = use_datastore(
        experiment_dir=Path(basedir[0]),
        builder=builder_type,
        open_ds=False,
        builder_kwargs={},
    )

    # Now break the catalog - reserialise as feather, which is not a valid format
    pd.read_csv(
        destdir / "experiment_datastore.csv",
        index_col=0,
    ).to_feather(
        destdir / "experiment_datastore.csv",
    )

    with pytest.warns(DataStoreWarning, match="but it is invalid. Regenerating..."):
        ret = use_datastore(
            experiment_dir=Path(basedir[0]),
            builder=builder_type,
            open_ds=True,
            builder_kwargs={},
        )
    assert isinstance(ret, esm_datastore)

    captured = capsys.readouterr()

    assert "generating new datastore" in captured.out
    assert "Datastore integrity verified!" not in captured.out


def test_verify_ds_current_mismatch(test_data):
    """
    An existing datastore compared against a non-matching dummy dataframe is not
    current, so verify_ds_current returns False.
    """
    existing_datastore = esm_datastore(
        str(test_data / "esm_datastore" / "cmip5-al33.json")
    )
    dummy_df = pd.DataFrame({"path": ["does_not_match.nc"]})

    assert verify_ds_current(dummy_df, existing_datastore) is False


@pytest.mark.parametrize(
    "builder, kwargs, fails, err_msg",
    [
        ("AccessOm2Builder", {}, False, ""),
        ("AccessOm2Builder", {"ensemble": True}, True, "Builder does not accept"),
        ("AccessEsm15Builder", {}, False, ""),
        ("AccessEsm15Builder", {"ensemble": True}, False, ""),
        ("AccessEsm15Builder", {"ensemble": "nonsense"}, True, "must be of type"),
        ("AccessEsm15Builder", {"esnmebel": True}, True, "Builder does not accept"),
    ],
)
def test_validate_args(builder: str, kwargs, fails, err_msg):
    builder_type: Builder = getattr(builders, builder)

    if not fails:
        validate_args(builder_type, kwargs)
        assert True
        return None

    with pytest.raises(TypeError, match=err_msg):
        validate_args(builder_type, kwargs)


@pytest.mark.parametrize(
    "kwarg, fails, expected",
    [
        (
            "ensemble=True",
            False,
            ("ensemble", True),
        ),
        (
            "ensemble=False",
            False,
            ("ensemble", False),
        ),
        (
            "ensemble=false",
            False,
            ("ensemble", False),
        ),
        (
            "ensemble=nonsense",
            True,
            None,
        ),
        (
            "ensemble=1",
            True,
            None,
        ),
        (
            "esnmebel=True",
            False,
            ("esnmebel", "True"),
        ),
    ],
)
def test_parse_kwarg(kwarg, fails, expected):
    if not fails:
        assert parse_kwarg(kwarg) == expected
    else:
        with pytest.raises(TypeError):
            parse_kwarg(kwarg)


@pytest.mark.parametrize(
    "mock_socket, warns",
    [("gadi-login-08.gadi.nci.org.au", True), ("anything_else", False)],
)
def test_login_node_warning(mock_socket, warns):
    """If we are running on a login node, assert a warning is raised about the
    scheduler potentially killing the job"""
    with mock.patch("socket.gethostname", return_value=mock_socket):
        if warns:
            with pytest.warns(LoginNodeWarning):
                warn_if_login_node()
        else:
            with warnings.catch_warnings():
                warnings.simplefilter("error", LoginNodeWarning)
                warn_if_login_node()
