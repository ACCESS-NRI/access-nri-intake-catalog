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
YAML_COLUMN = "yaml"
NAME_COLUMN = "name"
TRANSLATOR_GROUPBY_COLUMNS = ["model", "realm", "frequency"]

SCHEMA_URL = "https://raw.githubusercontent.com/ACCESS-NRI/schema/783ccf045e8e8e3dfbe35046c2b91a5b5e4e4874/experiment_asset.json"
SCHEMA_HASH = "73d4d864caeebde14d30fa3e698a13aa3013fec5a981a92039d1e7dfa3c306a2"

JSONSCHEMA = get_catalog_jsonschema(
    url=SCHEMA_URL, known_hash=SCHEMA_HASH, required=CORE_COLUMNS
)

COLUMNS_WITH_ITERABLES = [
    col for col in CORE_COLUMNS if JSONSCHEMA["properties"][col]["type"] == "array"
]
