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

SCHEMA_URL = "https://raw.githubusercontent.com/ACCESS-NRI/schema/9170a894c22123c90a703104d6a53c32cf975645/experiment_asset.json"
SCHEMA_HASH = "c4f4a546110a6f761f934ddb50b3ff63031a70f08233c1b5109a5ce80b078a41"

EXP_JSONSCHEMA, CATALOG_JSONSCHEMA = get_jsonschema(
    url=SCHEMA_URL, known_hash=SCHEMA_HASH, required=CORE_COLUMNS
)

COLUMNS_WITH_ITERABLES = [
    col
    for col in CORE_COLUMNS
    if CATALOG_JSONSCHEMA["properties"][col]["type"] == "array"
]
