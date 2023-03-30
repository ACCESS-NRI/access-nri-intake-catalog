# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Tools for managing intake catalogues """

import os

import intake

import pandas as pd

# import jsonschema

from . import ESM_CORE_METADATA, DF_CORE_METADATA

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

    def __init__(self, cat):
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
        self.metadata = {}

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
            path_column_name=ESM_CORE_METADATA["path_column"]["name"],
            variable_column_name=ESM_CORE_METADATA["variable_column"]["name"],
            data_format=data_format,
            groupby_attrs=groupby_attrs,
            aggregations=aggregations,
            esmcat_version="0.0.1",
            description=description,
            directory=directory,
            catalog_type="file",
        )

        columns_with_iterables = builder.columns_with_iterables

        return cls(
            intake.open_esm_datastore(
                json_file, columns_with_iterables=list(columns_with_iterables)
            )
        )

    @classmethod
    def load_esm(cls, json_file, **kwargs):
        """
        Load an existing intake-esm catalog

        Parameters
        ----------
        json_file: str
            The path to the intake-esm catalog JSON file
        kwargs: dict
            Additional kwargs to pass to :py:class:`~intake.open_esm_datastore`
        """
        return cls(intake.open_esm_datastore(json_file, **kwargs))

    def parse_metadata(self, translator, groupby):
        """
        Parse metadata table to include in the intake-dataframe-catalog from the intake-esm dataframe
        and merge into a set of rows with unique values of the columns specified in groupby.

        Parameters
        ----------
        translator: dict
            Dictionary with keys corresponding to core metadata columns in the
            intake-dataframe-catalog (see catalog_manager.DF_CORE_METADATA) and values corresponding
            to functions that translate information in the intake-esm dataframe to the
            intake-dataframe-catalog metadata. If a key is missing from this dictionary it is assumed
            that this key exists as a column in the intake-esm dataframe. If values are not not callable
            they are input directly as metadata for that key.
        groupby: list of str
            Core metadata columns to group by before merging metadata across remaining core columns.
        """

        def _parse_row(series, translator):
            metadata = {}
            for col in core_metadata:
                if col in translator:
                    if isinstance(translator[col], str):
                        metadata[col] = translator[col]
                    else:
                        metadata[col] = translator[col](series)
                else:
                    metadata[col] = series[col]
            return pd.DataFrame(metadata, index=[0])

        def _sum_unique(values):
            return values.drop_duplicates().sum()

        core_metadata = [val["name"] for val in DF_CORE_METADATA.values()]
        nongrouped_metadata = list(set(core_metadata) - set(groupby))

        df_metadata = pd.DataFrame(columns=core_metadata)

        for ind, row in self.cat.df.iterrows():
            row = _parse_row(row, translator)
            df_metadata = pd.concat([df_metadata, row], ignore_index=True)

        self.metadata = (
            df_metadata.groupby(groupby)
            .agg({col: _sum_unique for col in nongrouped_metadata})
            .reset_index()
        )
