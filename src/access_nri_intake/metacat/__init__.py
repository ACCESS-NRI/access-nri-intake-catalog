# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Tools for managing intake-dataframe-catalog metacatalogs """


from ..utils import get_catalog_jsonschema

CORE_COLUMNS = [
    "name",
    "model",
    "description",
    "realm",
    "frequency",
    "variable",
]
YAML_COLUMN = "yaml_column"
NAME_COLUMN = "name_column"
TRANSLATOR_GROUPBY_COLUMNS = ["model", "realm", "frequency"]

SCHEMA_URL = "https://raw.githubusercontent.com/ACCESS-NRI/schema/21db6b6dad8fd03cce7b32f39c1b012255946226/experiment_asset.json"
SCHEMA_HASH = "804a82659f44005727d6ad65870d5cb569bd413dc4241e4c5b5f1ab36c4fc92a"

JSONSCHEMA = get_catalog_jsonschema(
    url=SCHEMA_URL, known_hash=SCHEMA_HASH, required=CORE_COLUMNS
)
