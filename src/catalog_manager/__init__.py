# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import re

from . import _version

__version__ = _version.get_versions()["version"]

_ALLOWABLE_FREQS = ["fx", "subhr", "\d+hr", "\d+day", "\d+mon", "\d+yr", "\d+dec"]

# TODO: There's probably (definitely) a better way to do this
ESM_CORE_METADATA = {
    "path_column": {
        "name": "path",
        "validate": (
            lambda x: isinstance(x, str),
            "must be string",
        ),
    },
    "realm_column": {
        "name": "realm",
        "validate": (
            lambda x: isinstance(x, str),
            "must be string",
        ),
    },
    "variable_column": {
        "name": "variable",
        "validate": (
            lambda x: isinstance(x, list) and all(isinstance(s, str) for s in x),
            "must be a list of strings",
        ),
    },
    "frequency_column": {
        "name": "frequency",
        "validate": (
            lambda x: any(
                re.match(pattern, x) for pattern in [freq for freq in _ALLOWABLE_FREQ]
            ),
            f"must be one of {', '.join(_ALLOWABLE_FREQS)}",
        ),
    },
    "start_date_column": {
        "name": "start_date",
        "validate": (
            lambda x: True
            if re.match(r"\d\d\d\d-\d\d-\d\d,\s\d\d:\d\d:\d\d", x)
            else False,
            "must have the format %Y-%m-%d, %H:%M:%S",
        ),
    },
    "end_date_column": {
        "name": "end_date",
        "validate": (
            lambda x: True
            if re.match(r"\d\d\d\d-\d\d-\d\d,\s\d\d:\d\d:\d\d", x)
            else False,
            "must have the format %Y-%m-%d, %H:%M:%S",
        ),
    },
}

DF_CORE_METADATA = {
    "model_column": {
        "name": "model",
        "validate": (
            lambda x: isinstance(x, str),
            "must be string",
        ),
    },
    "experiment_column": {
        "name": "experiment",
        "validate": (
            lambda x: isinstance(x, str),
            "must be string",
        ),
    },
    "realm_column": {
        "name": "realm",
        "validate": (
            lambda x: isinstance(x, str),
            "must be string",
        ),
    },
    "variable_column": {
        "name": "variable",
        "validate": (
            lambda x: isinstance(x, list) and all(isinstance(s, str) for s in x),
            "must be a list of strings",
        ),
    },
    "frequency_column": {
        "name": "frequency",
        "validate": (
            lambda x: any(
                re.match(pattern, x)
                for pattern in [r"\d+" + freq for freq in _ALLOWABLE_FREQ]
            ),
            f"must be one of {', '.join(_ALLOWABLE_FREQS)}",
        ),
    },
}
