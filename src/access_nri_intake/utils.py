# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" General utility functions  for access-rni-intake """

import json
from importlib import resources as rsr
from warnings import warn

import jsonschema
import yaml


def get_jsonschema(metadata_file, required):
    """
    Read in the required JSON schema, and annotate it with "required" fields.

    Parameters
    ----------
    required: list
        A list of the properties to include in the "required" key
    """

    schema_file = rsr.files("access_nri_intake").joinpath(metadata_file)
    with schema_file.open(mode="r") as fpath:
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


def load_metadata_yaml(path, jsonschema):
    """
    Load a metadata.yaml file, leaving dates as strings, and validate against a jsonschema,
    allowing for tuples as arrays

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

    validate_against_schema(metadata, jsonschema)

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


def _can_be_array(field):
    """
    Does the schema allow the provided field to be an array?
    """

    def _is_array(field):
        try:
            return field["type"] == "array"
        except KeyError:
            return False

    is_array = _is_array(field)
    if (not is_array) and ("oneOf" in field):
        for nfield in field["oneOf"]:
            is_array = is_array or _is_array(nfield)
    return is_array


def get_catalog_fp():
    return rsr.files("access_nri_intake").joinpath("data/catalog.yaml")
