# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import argparse
import os
import tempfile
from pathlib import Path
from unittest import mock

import intake
import pytest
import yaml

from access_nri_intake.cli import (
    MetadataCheckError,
    _check_build_args,
    build,
    configure,
    metadata_template,
    metadata_validate,
)


@pytest.fixture
def temp_home():
    with tempfile.TemporaryDirectory() as temp_dir:
        with mock.patch("pathlib.Path.home", return_value=Path(temp_dir)):
            yield temp_dir


def test_entrypoint():
    """
    Check that entry point works
    """
    exit_status = os.system("catalog-build --help")
    assert exit_status == 0

    exit_status = os.system("metadata-validate --help")
    assert exit_status == 0

    exit_status = os.system("metadata-template --help")
    assert exit_status == 0

    exit_status = os.system("catalog-configure --help")
    assert exit_status == 0


@pytest.mark.parametrize(
    "args, raises",
    [
        (
            [
                {
                    "name": "exp0",
                    "metadata": {
                        "experiment_uuid": "214e8e6d-3bc5-4353-98d3-b9e9a5507d4b"
                    },
                },
                {
                    "name": "exp1",
                    "metadata": {
                        "experiment_uuid": "7b0bc2c6-7cbb-4d97-8eb9-b0255c16d910"
                    },
                },
            ],
            False,
        ),
        (
            [
                {
                    "name": "exp0",
                    "metadata": {
                        "experiment_uuid": "214e8e6d-3bc5-4353-98d3-b9e9a5507d4b"
                    },
                },
                {
                    "name": "exp0",
                    "metadata": {
                        "experiment_uuid": "7b0bc2c6-7cbb-4d97-8eb9-b0255c16d910"
                    },
                },
            ],
            True,
        ),
        (
            [
                {
                    "name": "exp0",
                    "metadata": {
                        "experiment_uuid": "214e8e6d-3bc5-4353-98d3-b9e9a5507d4b"
                    },
                },
                {
                    "name": "exp1",
                    "metadata": {
                        "experiment_uuid": "214e8e6d-3bc5-4353-98d3-b9e9a5507d4b"
                    },
                },
            ],
            True,
        ),
    ],
)
def test_check_build_args(args, raises):
    """
    Check that non-unique names and uuids return an error
    """
    if raises:
        with pytest.raises(MetadataCheckError) as excinfo:
            _check_build_args(args)
        assert "exp0" in str(excinfo.value)
    else:
        _check_build_args(args)


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(
        config_yaml=[
            "./tests/data/config/access-om2.yaml",
            "./tests/data/config/cmip5.yaml",
        ],
        build_base_path=tempfile.TemporaryDirectory().name,  # Use pytest fixture here?
        catalog_file="cat.csv",
        version="v0.0.0",
        no_update=True,
    ),
)
def test_build(mockargs):
    """Test full catalog build process from config files"""
    build()

    # Try to open the catalog
    build_path = (
        Path(mockargs.return_value.build_base_path)
        / mockargs.return_value.version
        / mockargs.return_value.catalog_file
    )
    cat = intake.open_df_catalog(build_path)
    assert len(cat) == 2


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(
        file=["./tests/data/access-om2/metadata.yaml"],
    ),
)
def test_metadata_validate(mockargs):
    """Test metadata_validate"""
    metadata_validate()


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(
        file=[
            "./tests/data/access-om2/metadata.yaml",
            "./tests/data/access-om3/metadata.yaml",
        ],
    ),
)
def test_metadata_validate_multi(mockargs):
    """Test metadata_validate"""
    metadata_validate()


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(
        file="./does/not/exist.yaml",
    ),
)
def test_metadata_validate_no_file(mockargs):
    """Test metadata_validate"""
    with pytest.raises(FileNotFoundError) as excinfo:
        metadata_validate()
    assert "No such file(s)" in str(excinfo.value)


def test_metadata_template():
    metadata_template()


@pytest.mark.parametrize(
    "call_args, expected_exits",
    (
        ([[], []], [0, 1]),
        ([[], ["-f"]], [0, 0]),
        ([[], ["--force"]], [0, 0]),
        ([["-f"], []], [0, 1]),
    ),
)
def test_configure_force(temp_home, call_args, expected_exits):
    """
    Test that the configure command creates a config file if none found, fails
    if one is found, unless --force/-f is used.
    """
    call_args_1, call_args_2 = call_args
    expected_exit_1, expected_exit_2 = expected_exits
    assert not Path(temp_home, ".catalog/config.yaml").exists()
    assert configure(call_args_1) == expected_exit_1
    assert Path(temp_home, ".catalog/config.yaml").exists()
    assert configure(call_args_2) == expected_exit_2


@pytest.mark.parametrize(
    "call_args, expected",
    ((["--version", "0.0.0"], "0.0.0"), (["-v", "0.0.0"], "0.0.0"), ([], "1.0.0")),
)
def test_configure_version(temp_home, call_args, expected):
    """
    Call configure with the specified version flags & check the version is set
    correctly in the config file.
    """
    configure(call_args)

    with open(Path(temp_home, ".catalog/config.yaml")) as f:
        config = yaml.safe_load(f)

    assert config["version"] == expected


@pytest.mark.parametrize(
    "call_args",
    (
        ["--interactive", "--version", "0.0.0"],
        ["-i", "--version", "0.0.0"],
        ["-i", "-f"],
        ["-i", "--force"],
    ),
)
def test_configure_interactive(temp_home, call_args):
    """
    Make sure that specifying interative mode raises an error no matter what
    other flags are passed.
    """
    with pytest.raises(SystemExit):
        configure(call_args)
