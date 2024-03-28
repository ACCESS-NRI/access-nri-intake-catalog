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

SCHEMA_URL = "https://raw.githubusercontent.com/ACCESS-NRI/schema/e9055da95093ec2faa555c090fc5af17923d1566/au.org.access-nri/model/output/file-metadata/1-0-1.json"
SCHEMA_HASH = "8f2f069fa06d81ff086b91daa6503f75615aa90385ab61ee2d1a7956dc96f9a6"

_, ESM_JSONSCHEMA = get_jsonschema(
    url=SCHEMA_URL, known_hash=SCHEMA_HASH, required=CORE_COLUMNS
)
