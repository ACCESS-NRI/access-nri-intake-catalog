# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from pathlib import Path

from pytest import fixture

here = Path(__file__).parent


@fixture(scope="session")
def test_data():
    return Path(here / "data")


@fixture(scope="session")
def BASE_DIR(tmp_path_factory):
    yield tmp_path_factory.mktemp("catalog-dir")


@fixture(scope="session")
def v_num():
    return datetime.now().strftime("v%Y-%m-%d")
