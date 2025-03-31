# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0


import shutil
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest
import yamanifest
from intake_esm import esm_datastore

from access_nri_intake.experiment.main import find_esm_datastore, use_datastore
from access_nri_intake.experiment.utils import (
    DatastoreInfo,
    DataStoreInvalidCause,
    DataStoreWarning,
    MultipleDataStoreError,
    hash_catalog,
    parse_kwarg,
    validate_args,
    verify_ds_current,
)
from access_nri_intake.source import builders
from access_nri_intake.source.builders import Builder


@pytest.mark.parametrize(
    "json_name, csv_name, validity, invalid_ds_cause",
    [
        (
            "malformed/missing_attribute.json",
            "malformed/missing_attribute.csv",
            False,
            DataStoreInvalidCause.COLUMN_MISMATCH,
        ),
        (
            "malformed/missing_csv_col.json",
            "malformed/missing_attribute.csv",
            False,
            DataStoreInvalidCause.MISMATCH_NAME,
        ),
        (
            "malformed/missing_csv_col.json",
            "malformed/missing_csv_col.csv",
            False,
            DataStoreInvalidCause.COLUMN_MISMATCH,
        ),
        (
            "malformed/corrupted.json",
            "malformed/corrupted.csv",
            False,
            DataStoreInvalidCause.JSON_CORRUPTED,
        ),
        (
            "malformed/wrong_fname.json",
            "malformed/wrong_fname.csv",
            False,
            DataStoreInvalidCause.CATALOG_MISMATCH,
        ),
        (
            "malformed/wrong_path.json",
            "malformed/wrong_path.csv",
            False,
            DataStoreInvalidCause.PATH_MISMATCH,
        ),
        (
            "cmip6-oi10.json",
            "cmip6-oi10.csv",
            True,
            DataStoreInvalidCause.NO_ISSUE,
        ),
    ],
)
def test_datastore_info(json_name, csv_name, validity, invalid_ds_cause, test_data):
    base_path = test_data / "esm_datastore"

    ds_info = DatastoreInfo(base_path / json_name, base_path / csv_name)

    assert ds_info.valid == validity
    assert ds_info.invalid_ds_cause == invalid_ds_cause


@pytest.mark.parametrize(
    "args, expected",
    [
        (["malformed/missing_attribute.json", "malformed/missing_attribute.csv"], True),
        (["malformed/missing_csv_col.json", "malformed/missing_csv_col.csv"], True),
        (["malformed/wrong_fname.json", "malformed/wrong_fname.csv"], True),
        (["malformed/wrong_path.json", "malformed/wrong_path.csv"], True),
        (["cmip6-oi10.json", "cmip6-oi10.csv"], True),
        (["", "", False, ""], False),
    ],
)
def test_DatastoreInfo_bool(test_data, args, expected):
    """
    Check that the __bool__ method of the DatastoreInfo class works as expected.
    """
    base_path = test_data / "esm_datastore"

    if expected:
        args = [base_path / arg for arg in args]

    ds_info = DatastoreInfo(*args)

    assert bool(ds_info) == expected


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
    "ds_name, warning_str",
    [
        ("ccam-hq89", "extra files in datastore"),
        ("cmip5-al33", "missing files from datastore"),
        ("cordex-ig45", "No hash file found for datastore"),
    ],
)
def test_verify_ds_current_fail_wrong_fileno(test_data, ds_name, warning_str):
    """
    We have the following hashes here:
    - ccam-hq89: This should contain a hash that is not in the csv/json files (stolen from Barpa)
    - cmip5-al33: This should miss a hash that is in the csv/json files
    - cmip6-oi10: This should match up but have a different hash
    - cordex-ig45: No hash, so should be rebuilt

    # To make sure this works, we will need to grab & subset all the netcdf files
    # that are in these datastores & hash them.
    """

    dir = test_data / "esm_datastore" / "local_paths"
    experiment_files = set((dir / "nc_files" / ds_name).glob("*.nc"))
    ds_info = DatastoreInfo(dir / f"{ds_name}.json", dir / f"{ds_name}.csv")

    with pytest.warns(DataStoreWarning, match=warning_str):
        ds_current_bool = verify_ds_current(
            ds_info,
            experiment_files,
        )

    assert not ds_current_bool


@mock.patch("access_nri_intake.source.builders.Builder")
def test_verify_ds_current_valid(mock_builder, test_data, tmpdir):
    """
    We have the following hashes here:
    - barpa-py18: This should match up.

    We can't guarantee that filesystem information will be the same across the
    various systems this test might be run on, so to circumvent that issue, we
    will write the hash out to a temporary file and verify against that.
    """
    shutil.copytree(test_data / "esm_datastore" / "local_paths", tmpdir / "local_paths")

    experiment_files = [
        str(file)
        for file in Path(tmpdir / "local_paths" / "nc_files" / "barpa-py18")
        .resolve()
        .glob("*.nc")
    ]

    # Mock the builder instance to have a df.path.tolist() method that returns
    # the experiment files.
    mock_builder.df.path.tolist.return_value = experiment_files

    hash_catalog(tmpdir / "local_paths", "barpa-py18", mock_builder)

    dir = tmpdir / "local_paths"
    ds_info = DatastoreInfo(dir / "barpa-py18.json", dir / "barpa-py18.csv")

    ds_current_bool = verify_ds_current(
        ds_info,
        set(experiment_files),
    )

    assert ds_current_bool


@mock.patch("access_nri_intake.source.builders.Builder")
def test_verify_ds_current_fail_differing_hashes(mock_builder, test_data, tmpdir):
    """
    We have the following hashes here:
    - cmip6-oi10: This should match up but have a different hash

    We can't guarantee that filesystem information will be the same across the
    various systems this test might be run on, so to circumvent that issue, we
    will write the hash out to a temporary file and verify against that.
    """

    shutil.copytree(test_data / "esm_datastore" / "local_paths", tmpdir / "local_paths")

    experiment_files = [
        str(file)
        for file in Path(tmpdir / "local_paths" / "nc_files" / "cmip6-oi10")
        .resolve()
        .glob("*.nc")
    ]

    ds_dir = tmpdir / "local_paths"

    # Mock the builder instance to have a df.path.tolist() method that returns
    # the experiment files.
    mock_builder.df.path.tolist.return_value = experiment_files

    hash_catalog(ds_dir, "cmip6-oi10", mock_builder)
    # Now we need to open the hash file and change the hash to something else

    manifest = yamanifest.Manifest(str(ds_dir / ".cmip6-oi10.hash")).load()

    for bh in manifest.data.values():
        bh["hashes"]["binhash"] = "0" * len(bh["hashes"]["binhash"])

    manifest.dump()

    ds_info = DatastoreInfo(ds_dir / "cmip6-oi10.json", ds_dir / "cmip6-oi10.csv")

    with pytest.warns(DataStoreWarning, match="differing hashes"):
        ds_current_bool = verify_ds_current(
            ds_info,
            set(experiment_files),
        )

    assert not ds_current_bool


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
        ("access-om3", "AccessOm3Builder", {}, 12),
        ("mom6", "Mom6Builder", {}, 27),
    ],
)
@pytest.mark.parametrize(
    "open_ds, return_type", [(True, esm_datastore), (False, type(None))]
)
@pytest.mark.parametrize("use_path", [True, False])
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
    basedir = [str(destdir)]
    # I think the str wrapper here is a bug- type hint implies we can pass a single string
    builder_type: Builder = getattr(builders, builder)
    builder = builder_type(basedir, **kwargs)
    builder.get_assets()

    assert isinstance(builder.assets, list)
    assert len(builder.assets) == num_assets

    exptdir = Path(basedir[0]) if use_path else basedir[0]
    # This creates a bunch of datastoers that we don't actually want here.
    ret = use_datastore(
        experiment_dir=exptdir,
        builder=builder_type,
        open_ds=open_ds,
        builder_kwargs=kwargs,
    )
    assert isinstance(ret, return_type)

    captured = capsys.readouterr()
    assert "Generating esm-datastore for" in captured.out
    assert "Hashing catalog" in captured.out
    assert (
        "Please note that this has not added the datastore to the access-nri-intake catalog"
        in captured.out
    )
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

    # This creates a bunch of datastoers that we don't actually want here.
    ret = use_datastore(
        experiment_dir=Path(basedir[0]),
        builder=builder_type,
        open_ds=False,
        builder_kwargs={},
    )

    # Now break the catalog - we can just remove a column
    pd.read_csv(
        destdir / "experiment_datastore.csv.gz",
        index_col=0,
    ).to_csv(
        destdir / "experiment_datastore.csv.gz",
        index=False,
    )

    # Run it again so that we can test the case where the datastore already exists
    with pytest.warns(DataStoreWarning, match="columns specified in JSON do not match"):
        ret = use_datastore(
            experiment_dir=Path(basedir[0]),
            builder=builder_type,
            open_ds=True,
            builder_kwargs={},
        )
    assert isinstance(ret, esm_datastore)

    captured = capsys.readouterr()

    assert "Building esm-datastore" in captured.out


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
