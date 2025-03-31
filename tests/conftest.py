# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from pathlib import Path

import pytest

here = Path(__file__).parent


@pytest.fixture(scope="session")
def test_data():
    return Path(here / "data")


def metadata_sources():
    return Path(here.parent / "config" / "metadata_sources")


@pytest.fixture(scope="session")
def config_dir():
    return Path(here / "e2e/configs")


@pytest.fixture(scope="session")
def live_config_dir():
    return Path(here).parent / "config"


@pytest.fixture(scope="session")
def BASE_DIR(tmp_path_factory):
    yield tmp_path_factory.mktemp("catalog-dir")


@pytest.fixture(scope="session")
def v_num():
    return datetime.now().strftime("v%Y-%m-%d")


@pytest.fixture(scope="function")
def check_metadata_cwd():

    # Run test
    yield

    # Look for metadata.yaml in CWD
    if (Path.cwd() / "metadata.yaml").is_file():
        raise UserWarning("Stray metadata.yaml left in CWD!")


def pytest_addoption(parser):
    parser.addoption(
        "--e2e",
        action="store_true",
        default=False,
        help="Run end-to-end tests",
        dest="e2e",
    )


def pytest_collection_modifyitems(config, items):
    """
    This function is called by pytest to modify the items collected during test
    collection.

    Previously used to mark tests with missing coordinate variables as xfails,
    now used to mark the CordexTranslator test as xfail. Keeping the xfails here
    makes it much easier to see what tests are expected to fail - no grepping
    for 'xfail' required.

    """

    # Now add xfail to broken CordexTranslator in end to end tests.
    for item in items:
        if item.name == "test_alignment[CordexTranslator]":
            item.add_marker("xfail")
