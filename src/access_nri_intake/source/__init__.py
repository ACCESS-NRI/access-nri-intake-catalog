# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Tools for generating Intake-ESM catalogs"""


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


_, ESM_JSONSCHEMA = get_jsonschema(
    metadata_file="data/metadata_schema_file.json", required=CORE_COLUMNS
)
