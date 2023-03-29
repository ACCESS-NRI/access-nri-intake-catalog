# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Tools for managing intake catalogues """

import os

import intake

# import jsonschema

from . import esm

# config_schema = {
#         'type': 'object',
#         'properties': {
#             'model': {'type': 'string'},
#             'catalogs': {
#                 'type': 'object',
#                 'properties': {
#                     'catalog_names': {
#                         'type': 'array',
#                         'items': {'type': 'string'},
#                     },
#                 },
#             },
#             'parser': {'type': 'string'},
#             'search': {
#                 'type': 'object',
#                 'properties': {
#                     'depth': {'type': 'integer'},
#                     'exclude_patterns': {
#                         'type': 'array',
#                         'items': {'type': 'string'},
#                     },
#                     'include_patterns': {
#                         'type': 'array',
#                         'items': {'type': 'string'},
#                     },
#                 },
#             },
#         },
#         'required': ['id','catalogs','parser','search'],
#     }


class CatalogExistsError(Exception):
    "Exception for trying to write catalog that already exists"
    pass


class CatalogManager:
    """
    Manage intake catalogs in an intake-dataframe-catalog
    """

    def __init__(self, cat, metadata=None):
        """
        Initialise a CatalogManager

        Parameters
        ----------
        cat: :py:class:`intake.DataSource`
            An intake catalog to append/update in the intake-dataframe-catalog
        metadata: dict
            Metadata associated with cat to include in the intake-dataframe-catalog.
            If adding to an existing dataframe-catalog, keys in this dictionary must
            correspond to columns in the dataframe-catalog.
        """

        self.cat = cat
        self.metadata = metadata or {}

    @classmethod
    def build_esm(
        cls,
        name,
        description,
        parser,
        root_dirs,
        data_format,
        parser_kwargs=None,
        groupby_attrs=None,
        aggregations=None,
        directory=None,
        overwrite=False,
    ):
        """
        Build an intake-esm catalog

        Parameters
        ----------
        name: str
            The name of the catalog
        description: str
            Description of the contents of the catalog
        parser: subclass of :py:class:`catalog_manager.esm.BaseParser`
            The parser to use to build the intake-esm catalog
        root_dirs: list of str
            Root directories to parse for files to add to the catalog
        data_format: str
            The data format. Valid values are 'netcdf', 'reference', 'zarr' and 'opendap'.
        parser_kwargs: dict
            Additional kwargs to pass to the parser
        groupby_attrs
            Intake-esm column names that define data sets that can be aggegrated.
        aggregations: listof dict
            List of aggregations to apply to query results
        directory: str
            The directory to save the catalog to. If None, use the current directory
        overwrite: bool, optional
            Whether to overwrite any existing catalog(s) with the same name
        """

        parser_kwargs = parser_kwargs or {}

        json_file = os.path.abspath(f"{os.path.join(directory, name)}.json")
        if os.path.isfile(json_file):
            if not overwrite:
                raise CatalogExistsError(
                    f"A catalog already exists for {name}. To overwrite, "
                    "pass `overwrite=True` to CatalogBuilder.build"
                )

        builder = parser(
            root_dirs,
            **parser_kwargs,
        ).build()

        builder.save(
            name=name,
            path_column_name=esm.CORE_INFO["path_column"]["name"],
            variable_column_name=esm.CORE_INFO["variable_column"]["name"],
            data_format=data_format,
            groupby_attrs=groupby_attrs,
            aggregations=aggregations,
            esmcat_version="0.0.1",
            description=description,
            directory=directory,
            catalog_type="file",
        )

        return cls(intake.open_esm_datastore(json_file))

    @classmethod
    def load_esm(cls, json_file):
        """
        Load an existing intake-esm catalog

        Parameters
        ----------
        json_file: str
            The path to the intake-esm catalog JSON file
        """
        return cls(intake.open_esm_datastore(json_file))
