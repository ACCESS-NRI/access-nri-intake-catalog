# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Tools for generating Intake-ESM catalogs """


from ..utils import get_jsonschema

CORE_COLUMNS = [
    "path",
    "realm",
    "frequency",
    "variable",
    "start_date",
    "end_date",
]
PATH_COLUMN = "path"
VARIABLE_COLUMN = "variable"

SCHEMA_URL = "https://raw.githubusercontent.com/ACCESS-NRI/schema/25643eb437e95ee48b3fa6b620c7a0986c2c3bb0/file_asset.json"
SCHEMA_HASH = "d7b5fcab71861f6c4b319e64cfde75f36de2bdc797f13b5b4f7029b41ce51e5a"

_, ESM_JSONSCHEMA = get_jsonschema(
    url=SCHEMA_URL, known_hash=SCHEMA_HASH, required=CORE_COLUMNS
)
