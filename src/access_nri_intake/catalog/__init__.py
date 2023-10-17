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

SCHEMA_URL = "https://raw.githubusercontent.com/ACCESS-NRI/schema/36abe2fe28eb2853a54f41c5eedfd964617d9d68/experiment_asset.json"
SCHEMA_HASH = "60d439a9ad5602464c7dad54072ac276d1fae3634f9524edcc82073a5a92616a"

EXP_JSONSCHEMA, CATALOG_JSONSCHEMA = get_jsonschema(
    url=SCHEMA_URL, known_hash=SCHEMA_HASH, required=CORE_COLUMNS
)

COLUMNS_WITH_ITERABLES = [
    col
    for col in CORE_COLUMNS
    if CATALOG_JSONSCHEMA["properties"][col]["type"] == "array"
]
