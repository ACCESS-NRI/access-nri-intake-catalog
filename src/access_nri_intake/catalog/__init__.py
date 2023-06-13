# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Tools for managing intake-dataframe-catalogs like the ACCESS-NRI catalog """


from ..utils import get_jsonschema

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

SCHEMA_URL = "https://raw.githubusercontent.com/ACCESS-NRI/schema/4e3d10e563d7c1c9f66e9ab92a2926cdec3d6893/experiment_asset.json"
SCHEMA_HASH = "b18cf5bdd06a6f5bcdc71dfc80f7336c63eb49f6d6f75c2cd3371e59eee5488b"

EXP_JSONSCHEMA, CATALOG_JSONSCHEMA = get_jsonschema(
    url=SCHEMA_URL, known_hash=SCHEMA_HASH, required=CORE_COLUMNS
)

COLUMNS_WITH_ITERABLES = [
    col
    for col in CORE_COLUMNS
    if CATALOG_JSONSCHEMA["properties"][col]["type"] == "array"
]
