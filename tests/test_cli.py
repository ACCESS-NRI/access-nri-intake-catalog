# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import argparse
import os
import tempfile
from pathlib import Path
from unittest import mock

import intake
import pytest

from access_nri_intake.cli import MetadataCheckError, _check_args, build


def test_entrypoint():
    """
    Check that entry point works
    """
    exit_status = os.system("catalog-build --help")
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
def test_check_args(args, raises):
    """
    Check that non-unique names and uuids return an error
    """
    if raises:
        with pytest.raises(MetadataCheckError) as excinfo:
            _check_args(args)
        assert "exp0" in str(excinfo.value)
    else:
        _check_args(args)


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
