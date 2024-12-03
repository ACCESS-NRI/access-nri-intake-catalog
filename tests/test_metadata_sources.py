# Copyright 2024 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import glob
from pathlib import Path

from conftest import metadata_sources

from access_nri_intake.cli import metadata_validate


def pytest_generate_tests(metafunc):
    if "yamlfile" in metafunc.fixturenames:
        metafunc.parametrize(
            "yamlfile", glob.glob(str(Path(metadata_sources() / "*" / "metadata.yaml")))
        )


def test_metadata_sources_valid(yamlfile, capsys):
    try:
        metadata_validate(
            [
                yamlfile,
            ]
        )
    except Exception:
        assert False, f"Validation of {yamlfile} failed with uncaught exception"
    output = capsys.readouterr()
    assert "FAILED" not in output.out, f"Validation failed for {yamlfile}"
