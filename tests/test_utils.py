# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import datetime
from pathlib import Path

import jsonschema
import pytest
import yaml

from access_nri_intake.catalog import EXP_JSONSCHEMA
from access_nri_intake.utils import (
    get_jsonschema,
    load_metadata_yaml,
    validate_against_schema,
)


@pytest.mark.parametrize(
    "schema_file",
    ["data/metadata_schema_experiment.json", "data/metadata_schema_file.json"],
)
def test_get_jsonschema(schema_file):
    """
    Test that  required fields are overwritten
    """
    required = [
        "realm",
    ]
    schema, schema_required = get_jsonschema(
        metadata_file=schema_file, required=required
    )
    assert "$schema" in schema
    assert schema_required["required"] == required

    required += ["foo"]
    with pytest.warns(UserWarning):
        _, _ = get_jsonschema(metadata_file=schema_file, required=required)


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


@pytest.mark.parametrize(
    "instance,no_errs,bad_kw",
    [
        ("bad_metadata/metadata-missing-name.yaml", 1, []),
        ("bad_metadata/metadata-missing-name-missing-uuid.yaml", 2, []),
        (
            "bad_metadata/metadata-bad-name-missing-uuid.yaml",
            2,
            [
                "name",
            ],
        ),
        (
            "bad_metadata/metadata-bad-name.yaml",
            1,
            [
                "name",
            ],
        ),
        (
            "bad_metadata/metadata-bad-name2.yaml",
            1,
            [
                "name",
            ],
        ),
        (
            "bad_metadata/metadata-bad-name2-missing-uuid.yaml",
            2,
            [
                "name",
            ],
        ),
        (
            "bad_metadata/metadata-bad-descript.yaml",
            1,
            [
                "description",
            ],
        ),
        (
            "bad_metadata/metadata-bad-descript2.yaml",
            1,
            [
                "description",
            ],
        ),
        (
            "bad_metadata/metadata-bad-longdescript.yaml",
            1,
            [
                "long_description",
            ],
        ),
        (
            "bad_metadata/metadata-bad-longdescript2.yaml",
            1,
            [
                "long_description",
            ],
        ),
    ],
)
def test_bad_metadata_validation_raises(test_data, instance, no_errs, bad_kw):
    fpath = str(test_data / Path(instance))

    try:
        _ = load_metadata_yaml(fpath, EXP_JSONSCHEMA)
    except jsonschema.ValidationError as e:
        assert (
            f"{no_errs:02d}" in e.message and f"{no_errs+1:02d}" not in e.message
        ), "Can't see the right number of errors!"
        for kw in bad_kw:
            assert f"| {kw}" in e.message, f"Can't see expected specific error for {kw}"
    except Exception as e:
        assert (
            False
        ), f"load_metadata_yaml didn't raise jsonschema.ValidationError: {str(e)}"
