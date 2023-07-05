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

SCHEMA_URL = "https://raw.githubusercontent.com/ACCESS-NRI/schema/d4da77a0e627775c11ba394c0a3f72a2c654971c/file_asset.json"
SCHEMA_HASH = "7f1f58e1ae419faf8e24f15e937ef5717fa872920a06758ee2983506fcaf70fc"

_, ESM_JSONSCHEMA = get_jsonschema(
    url=SCHEMA_URL, known_hash=SCHEMA_HASH, required=CORE_COLUMNS
)
