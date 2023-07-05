# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import datetime

import pytest
import yaml

from access_nri_intake.utils import (
    get_jsonschema,
    load_metadata_yaml,
    validate_against_schema,
)


@pytest.mark.parametrize(
    "known_hash",
    ["2a09030653f495939c90a22e95dd1c4587c8695f7f07e17b9129a6491469f9fc", None],
)
def test_get_jsonschema(known_hash):
    """
    Test that jsonschema are correctly downloaded and required fields are overwritten
    """
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


def test_load_metadata_yaml(tmp_path):
    """
    Test that dates are left as strings when reading metadata.yaml files
    """
    path = tmp_path / "metadata.yaml"
    date = "2001-01-01"
    contents = {"date": datetime.date.fromisoformat(date)}
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "date": {
            "type": "string",
        },
    }
    with open(path, mode="w") as fpath:
        yaml.dump(contents, fpath)
    load_metadata_yaml(path, schema)


@pytest.mark.parametrize("instance", [{"foo": [0, 1, 2]}, {"foo": (0, 1, 2)}])
def test_validate_against_schema(instance):
    """
    Test jsonschema validation, allowing for tuples as arrays
    """
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "foo": {
                "type": "array",
            },
        },
    }
    validate_against_schema(instance, schema)
