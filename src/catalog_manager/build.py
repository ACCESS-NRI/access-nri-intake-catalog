# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Tools for managing intake catalogues """

import os

import intake

from . import CoreESMMetadata, CoreDFMetadata


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
            path_column_name=CoreESMMetadata.path_column_name,
            variable_column_name=CoreESMMetadata.variable_column_name,
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

    def parse_esm_metadata(self, translator, groupby):
        """
        Parse metadata table to include in the intake-dataframe-catalog from an intake-esm dataframe
        and merge into a set of rows with unique values of the columns specified in groupby.

        Parameters
        ----------
        translator: :py:class:`~catalog_manager.translators.ColumnTranslator`
            An instance of the :py:class:`~catalog_manager.translators.ColumnTranslator` class for
            translating intake-esm column metadata into intake-dataframe-catalog column metadata
        groupby: list of str
            Core metadata columns to group by before merging metadata across remaining core columns.
        """

        def _sum_unique(values):
            return values.drop_duplicates().sum()

        ungrouped_columns = list(set(CoreDFMetadata.columns) - set(groupby))

        metadata = translator.translate(self.cat.df)

        self.metadata = (
            metadata.groupby(groupby)
            .agg({col: _sum_unique for col in ungrouped_columns})
            .reset_index()
        )
