# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Tools for managing intake-dataframe-catalogs like the ACCESS-NRI catalog """


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

SCHEMA_URL = "https://raw.githubusercontent.com/ACCESS-NRI/schema/e9055da95093ec2faa555c090fc5af17923d1566/au.org.access-nri/model/output/experiment-metadata/1-0-2.json"
SCHEMA_HASH = "ecb72c1adde3679896ceeca96aa6500d07ea2e05810155ec7a5dc301593c1dc7"

EXP_JSONSCHEMA, CATALOG_JSONSCHEMA = get_jsonschema(
    url=SCHEMA_URL, known_hash=SCHEMA_HASH, required=CORE_COLUMNS
)

COLUMNS_WITH_ITERABLES = [
    col for col in CORE_COLUMNS if _can_be_array(CATALOG_JSONSCHEMA["properties"][col])
]
