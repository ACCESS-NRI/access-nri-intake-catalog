# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path

from pytest import fixture

here = os.path.abspath(os.path.dirname(__file__))


@fixture(scope="session")
def test_data():
    return Path(os.path.join(here, "data"))
