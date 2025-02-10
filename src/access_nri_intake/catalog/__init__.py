# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Tools for managing intake-dataframe-catalogs like the ACCESS-NRI catalog"""


from ..utils import _can_be_array, get_jsonschema

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


EXP_JSONSCHEMA, CATALOG_JSONSCHEMA = get_jsonschema(
    metadata_file="data/metadata_schema_experiment.json", required=CORE_COLUMNS
)

COLUMNS_WITH_ITERABLES = [
    col for col in CORE_COLUMNS if _can_be_array(CATALOG_JSONSCHEMA["properties"][col])
]
