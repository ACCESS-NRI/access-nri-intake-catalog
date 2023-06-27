# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import os


def test_entrypoint():
    exit_status = os.system("catalog-build --help")
    assert exit_status == 0
