# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Utility functions """

import json
from warnings import warn

import jsonschema
import pooch


def get_catalog_jsonschema(url, known_hash, required):
    """
    Download and return a jsonschema from a url and adjust the entries in the "required" key

    Parameters
    ----------
    url: str
        The URL to the jsonschema file. ACCESS-NRI schema can be found at
        https://github.com/ACCESS-NRI/schema.
    known_hash: str
        A known hash (checksum) of the file. See :py:func:`~pooch.retrieve`.
    required: list
        A list of the properties to include in the "required" key
    """

    schema_file = pooch.retrieve(url=url, known_hash=known_hash)

    with open(schema_file) as fpath:
        schema = json.load(fpath)

    req = []
    for col in required:
        if col not in schema["properties"]:
            warn(
                f"Required column {col} does not exist in schema. Entries in this column will not be validated"
            )
        else:
            req.append(col)

    schema["required"] = req

    return schema


def validate_against_schema(instance, schema):
    """
    Validate a dictionary againsta a jsonschema, allowing for tuples as arrays

    Parameters
    ----------
    instance: dict
        The instance to validate
    schema: dict
        The jsonschema
    """

    Validator = jsonschema.validators.validator_for(schema)
    type_checker = Validator.TYPE_CHECKER.redefine(
        "array", lambda checker, instance: isinstance(instance, (list, tuple))
    )
    TupleAllowingValidator = jsonschema.validators.extend(
        Validator, type_checker=type_checker
    )

    TupleAllowingValidator(schema).validate(instance)
