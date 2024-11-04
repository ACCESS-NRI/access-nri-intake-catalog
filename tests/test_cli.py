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
    metadata_template,
    metadata_validate,
)


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
            "config/access-om2.yaml",
            "config/cmip5.yaml",
        ],
        build_base_path=tempfile.TemporaryDirectory().name,  # Use pytest fixture here?
        catalog_file="cat.csv",
        version="v2024-01-01",
        no_update=True,
    ),
)
def test_build(mockargs, test_data):
    """Test full catalog build process from config files"""
    # Update the config_yaml paths
    for i, p in enumerate(mockargs.return_value.config_yaml):
        mockargs.return_value.config_yaml[i] = os.path.join(test_data, p)
    build()

    # Try to open the catalog
    build_path = (
        Path(mockargs.return_value.build_base_path)
        / mockargs.return_value.version
        / mockargs.return_value.catalog_file
    )
    cat = intake.open_df_catalog(build_path)
    assert len(cat) == 2


@mock.patch("access_nri_intake.cli.get_catalog_fp")
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(
        config_yaml=[
            "config/access-om2.yaml",
            "config/cmip5.yaml",
        ],
        build_base_path=tempfile.TemporaryDirectory().name,  # Use pytest fixture here?
        catalog_file="cat.csv",
        version="v2024-01-01",
        no_update=False,
    ),
)
def test_build_repeat_nochange(mockargs, get_catalog_fp, test_data):
    """
    Test if the intelligent versioning works correctly when there is
    no significant change to the underlying catalogue
    """
    # Update the config_yaml paths
    for i, p in enumerate(mockargs.return_value.config_yaml):
        mockargs.return_value.config_yaml[i] = os.path.join(test_data, p)

    # Write the catalog.yamls to where the catalogs go
    get_catalog_fp.return_value = os.path.join(
        mockargs.return_value.build_base_path, "catalog.yaml"
    )

    build()

    # Update the version number and have another crack at building
    mockargs.return_value.version = "v2024-01-02"
    build()

    # There is no change between catalogs, so we should be able to
    # see just a version number change in the yaml
    with Path(get_catalog_fp.return_value).open(mode="r") as fobj:
        cat_yaml = yaml.safe_load(fobj)

    assert (
        cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("min")
        == "v2024-01-01"
    ), f'Min version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("min")} does not match expected v2024-01-01'
    assert (
        cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("max")
        == "v2024-01-02"
    ), f'Max version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("max")} does not match expected v2024-01-02'
    assert (
        cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("default")
        == "v2024-01-02"
    ), f'Default version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("default")} does not match expected v2024-01-02'


@mock.patch("access_nri_intake.cli.get_catalog_fp")
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(
        config_yaml=[
            "config/access-om2.yaml",
            # "config/cmip5.yaml",  # Save this for addition
        ],
        build_base_path=tempfile.TemporaryDirectory().name,  # Use pytest fixture here?
        catalog_file="cat.csv",
        version="v2024-01-01",
        no_update=False,
    ),
)
def test_build_repeat_adddata(mockargs, get_catalog_fp, test_data):
    # Update the config_yaml paths
    for i, p in enumerate(mockargs.return_value.config_yaml):
        mockargs.return_value.config_yaml[i] = os.path.join(test_data, p)

    # Write the catalog.yamls to where the catalogs go
    get_catalog_fp.return_value = os.path.join(
        mockargs.return_value.build_base_path, "catalog.yaml"
    )

    # Build the first catalog
    build()

    # Now, add the second data source & rebuild
    mockargs.return_value.config_yaml.append(
        os.path.join(test_data, "config/cmip5.yaml")
    )
    mockargs.return_value.version = "v2024-01-02"
    build()

    # There is no change between catalogs, so we should be able to
    # see just a version number change in the yaml
    with Path(get_catalog_fp.return_value).open(mode="r") as fobj:
        cat_yaml = yaml.safe_load(fobj)

    assert (
        cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("min")
        == "v2024-01-01"
    ), f'Min version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("min")} does not match expected v2024-01-01'
    assert (
        cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("max")
        == "v2024-01-02"
    ), f'Max version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("max")} does not match expected v2024-01-02'
    assert (
        cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("default")
        == "v2024-01-02"
    ), f'Default version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("default")} does not match expected v2024-01-02'
    assert cat_yaml["sources"]["access_nri"]["metadata"]["storage"] == "gdata/al33"


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
            "access-om2/metadata.yaml",
            "access-om3/metadata.yaml",
        ],
    ),
)
def test_metadata_validate_multi(mockargs, test_data):
    """Test metadata_validate"""
    # Update the config_yaml paths
    for i, p in enumerate(mockargs.return_value.file):
        mockargs.return_value.file[i] = os.path.join(test_data, p)
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
