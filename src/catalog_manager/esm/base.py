# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Base parser for generating an intake-esm catalog """

from ecgtools.builder import Builder, INVALID_ASSET

from .. import ESM_CORE_METADATA


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
                for key, val in ESM_CORE_METADATA.items():
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
