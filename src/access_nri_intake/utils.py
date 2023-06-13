# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" General utility functions  for access-rni-intake """

import json
from warnings import warn

import jsonschema
import pooch
import yaml


def get_jsonschema(url, known_hash, required):
    """
    Download a jsonschema from a url. Returns the unaltered jsonschema and a version with the "required" key
    matching the properties provided.

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

    schema_required = schema.copy()
    req = []
    for col in required:
        if col not in schema_required["properties"]:
            warn(
                f"Required column {col} does not exist in schema. Entries in this column will not be validated"
            )
        else:
            req.append(col)

    schema_required["required"] = req

    return schema, schema_required


def load_metadata_yaml(path):
    """
    Load a metadata.yaml file, leaving dates as strings

    Parameters
    ----------
    paths: str
        The path to the metadata.yaml
    """

    class NoDatesSafeLoader(yaml.SafeLoader):
        @classmethod
        def remove_implicit_resolver(cls, tag_to_remove):
            """
            Remove implicit resolvers for a particular tag

            See https://stackoverflow.com/questions/34667108/ignore-dates-and-times-while-parsing-yaml
            """
            if "yaml_implicit_resolvers" not in cls.__dict__:
                cls.yaml_implicit_resolvers = cls.yaml_implicit_resolvers.copy()

            for first_letter, mappings in cls.yaml_implicit_resolvers.items():
                cls.yaml_implicit_resolvers[first_letter] = [
                    (tag, regexp) for tag, regexp in mappings if tag != tag_to_remove
                ]

    NoDatesSafeLoader.remove_implicit_resolver("tag:yaml.org,2002:timestamp")

    with open(path) as fpath:
        metadata = yaml.load(fpath, Loader=NoDatesSafeLoader)

    return metadata


def validate_against_schema(instance, schema):
    """
    Validate a dictionary against a jsonschema, allowing for tuples as arrays

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
