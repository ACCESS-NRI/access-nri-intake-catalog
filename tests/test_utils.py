# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import pytest

from access_nri_intake.utils import get_jsonschema


@pytest.mark.parametrize(
    "known_hash",
    ["2a09030653f495939c90a22e95dd1c4587c8695f7f07e17b9129a6491469f9fc", None],
)
def test_get_jsonschema(known_hash):
    url = "https://raw.githubusercontent.com/ACCESS-NRI/schema/4e3d10e563d7c1c9f66e9ab92a2926cdec3d6893/file_asset.json"
    required = [
        "path",
    ]
    schema, schema_required = get_jsonschema(
        url=url, known_hash=known_hash, required=required
    )
    assert "$schema" in schema
    assert schema_required["required"] == required

    required += ["foo"]
    with pytest.warns(UserWarning):
        _, _ = get_jsonschema(url=url, known_hash=known_hash, required=required)
