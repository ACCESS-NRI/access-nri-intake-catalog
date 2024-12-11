# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import datetime
from pathlib import Path
from unittest import mock

import jsonschema
import pytest
import yaml

from access_nri_intake import CATALOG_LOCATION, USER_CATALOG_LOCATION
from access_nri_intake.catalog import EXP_JSONSCHEMA
from access_nri_intake.utils import (
    get_catalog_fp,
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
        (
            "bad_metadata/metadata-bad-descript-bad-name.yaml",
            2,
            [
                "description",
                "name",
            ],
        ),
        (
            "bad_metadata/metadata-bad-descript-bad-name2.yaml",
            2,
            [
                "description",
                "name",
            ],
        ),
        (
            "bad_metadata/metadata-bad-descript2-bad-name2.yaml",
            2,
            [
                "description",
                "name",
            ],
        ),
        (
            "bad_metadata/metadata-bad-all-bad.yaml",
            4,
            [
                "description",
                "name",
                "long_description",
                "experiment_uuid",
            ],
        ),
        (
            "bad_metadata/metadata-bad-created.yaml",
            1,
            [
                "created",
            ],
        ),
        (
            "bad_metadata/metadata-bad-url.yaml",
            1,
            [
                "url",
            ],
        ),
        (
            "bad_metadata/metadata-bad-contact.yaml",
            1,
            [
                "contact",
            ],
        ),
        (
            "bad_metadata/metadata-bad-email.yaml",
            1,
            [
                "email",
            ],
        ),
    ],
)
def test_bad_metadata_validation_raises(test_data, instance, no_errs, bad_kw):
    fpath = str(test_data / Path(instance))

    with pytest.raises(jsonschema.ValidationError) as excinfo:
        _ = load_metadata_yaml(fpath, EXP_JSONSCHEMA)

    err_msg = excinfo.value.message

    assert (
        f"{no_errs:02d}" in err_msg and f"{no_errs+1:02d}" not in err_msg
    ), "Can't see the right number of errors!"
    for kw in bad_kw:
        assert f"| {kw}" in err_msg, f"Can't see expected specific error for {kw}"


@mock.patch("pathlib.Path.is_file")
@pytest.mark.parametrize("isfile_return", [True, False])
@pytest.mark.parametrize(
    "basepath",
    [
        "/path/like/string",
        Path("/path/like/string"),
    ],
)
def test_get_catalog_fp_basepath(mock_is_file, isfile_return, basepath):
    mock_is_file.return_value = isfile_return
    assert str(get_catalog_fp(basepath=basepath)) == "/path/like/string/catalog.yaml"


@mock.patch("pathlib.Path.is_file", return_value=True)
def test_get_catalog_fp_local(mock_is_file):
    """
    Check that we get pointed back to the user catalog
    """
    assert str(get_catalog_fp()) == USER_CATALOG_LOCATION


@mock.patch("pathlib.Path.is_file", return_value=False)
def test_get_catalog_fp_xp65(mock_is_file):
    """
    Check that we get pointed back to the user catalog
    """
    assert str(get_catalog_fp()) == CATALOG_LOCATION
