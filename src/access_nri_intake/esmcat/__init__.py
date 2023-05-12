# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Tools for generating intake-esm catalogs """


from ..utils import get_catalog_jsonschema

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

SCHEMA_URL = "https://raw.githubusercontent.com/ACCESS-NRI/schema/21db6b6dad8fd03cce7b32f39c1b012255946226/file_asset.json"
SCHEMA_HASH = "2a09030653f495939c90a22e95dd1c4587c8695f7f07e17b9129a6491469f9fc"

JSONSCHEMA = get_catalog_jsonschema(
    url=SCHEMA_URL, known_hash=SCHEMA_HASH, required=CORE_COLUMNS
)
