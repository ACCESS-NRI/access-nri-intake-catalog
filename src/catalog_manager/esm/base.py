# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Base parser for generating an intake-esm catalog """

import re

from ecgtools.builder import Builder, INVALID_ASSET

# TO DO: There's probably (definitely) a better way to do this
CORE_INFO = {
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
                re.match(pattern, x)
                for pattern in [
                    r"\d+yr",
                    r"\d+mon",
                    r"\d+day",
                    r"\d+hr",
                    "fx",
                ]
            ),
            "must be one of 'Nyr', 'Nmon', 'Nday', 'Nhr', 'fx', where N is an integer",
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


class ParserValidationError(Exception):
    pass


class BaseParser(Builder):
    """
    Base class for creating intake-esm parsers. This is very similar to the ecgtools.Builder
    class, but it includes the parser as a staticmethod in the class, which makes validation
    of the parser output simpler
    """

    def _parse(self):
        super().parse(parsing_func=self.parser)

    def parse(self):
        self._parse()
        return self

    def validate_parser(self):
        """
        Run the parser on a single file and check the schema of the info being parsed
        """

        if not self.assets:
            raise ValueError(
                "asset list provided is None. Please run `.get_assets()` first"
            )

        for asset in self.assets:
            info = self.parser(asset)
            if INVALID_ASSET not in info:
                for key, val in CORE_INFO.items():
                    name = val["name"]
                    func = val["validate"][0]
                    msg = val["validate"][1]
                    if name not in info:
                        raise ParserValidationError(
                            f"Parser must parse '{name}' info that {msg}"
                        )
                    if not func(info[name]):
                        raise ParserValidationError(f"Parser output '{name}' {msg}")
                return self

        raise ParserValidationError(
            f"Parser returns no valid assets. Try parsing a single file with {self.parser}(file)"
        )

    def build(self):
        """
        Builds a catalog from a list of netCDF files or zarr stores.
        """

        self.get_assets().validate_parser().parse().clean_dataframe()

        return self

    @property
    def columns_with_iterables(self):
        """
        Return a set of the columns that have iterables
        """
        # Stolen from intake-esm.cat.ESMCatalogModel
        if self.df.empty:
            return set()
        has_iterables = (
            self.df.sample(20, replace=True)
            .applymap(type)
            .isin([list, tuple, set])
            .any()
            .to_dict()
        )
        return {column for column, check in has_iterables.items() if check}

    @staticmethod
    def parser(file):
        """Override this: parse catalog information from a given file"""
        pass
