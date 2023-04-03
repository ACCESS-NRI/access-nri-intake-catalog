# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Tools for managing intake catalogues """

import os

import intake
from intake_dataframe_catalog.core import DFCatalogModel

from .metadata import CoreDFMetadata
from .translators import SimpleMetadataTranslator


class CatalogExistsError(Exception):
    "Exception for trying to write catalog that already exists"
    pass


class CatalogManager:
    """
    Add/update intake catalogs in an intake-dataframe-catalog
    """

    def __init__(self, cat, metadata):
        """
        Initialise a DFCatUpdater

        Parameters
        ----------
        cat: :py:class:`intake.DataSource`
            An intake catalog to append/update in the intake-dataframe-catalog
        metadata: :py:class:`~pandas.DataFrame`
            :py:class:`~pandas.DataFrame` with columns corresponding to Metadata associated
            with cat to include in the intake-dataframe-catalog.
        """

        # Overwrite the catalog name with the name_column entry in metadata
        name = metadata[CoreDFMetadata.name_column].unique()
        if len(name) != 1:
            raise ValueError(
                f"Metadata column '{CoreDFMetadata.name_column}' must be the same for all rows "
                "since this corresponds to the catalog name"
            )
        name = name[0]
        cat.name = name

        self.cat = cat
        self.metadata = metadata
        self.dfcat = None

    @classmethod
    def build_esm(
        cls,
        name,
        description,
        builder,
        translator,
        paths,
        builder_kwargs=None,
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
        builder: subclass of :py:class:`catalog_manager.esmcat.BaseBuilder`
            The builder to use to build the intake-esm catalog
        translator: :py:class:`~catalog_manager.translators.MetadataTranslator`
            An instance of the :py:class:`~catalog_manager.translators.MetadataTranslator` class for
            translating intake-esm column metadata into intake-dataframe-catalog column metadata
        paths: list of str
            List of paths to crawl for assets/files to add to the catalog.
        builder_kwargs: dict
            Additional kwargs to pass to the builder
        directory: str
            The directory to save the catalog to. If None, use the current directory
        overwrite: bool, optional
            Whether to overwrite any existing catalog(s) with the same name
        """

        builder_kwargs = builder_kwargs or {}

        json_file = os.path.abspath(f"{os.path.join(directory, name)}.json")
        if os.path.isfile(json_file):
            if not overwrite:
                raise CatalogExistsError(
                    f"A catalog already exists for {name}. To overwrite, "
                    "pass `overwrite=True` to CatalogBuilder.build"
                )

        builder = builder(paths, **builder_kwargs).build()
        builder.save(name=name, description=description, directory=directory)

        cat = intake.open_esm_datastore(
            json_file, columns_with_iterables=list(builder.columns_with_iterables)
        )
        cat.name = name
        cat.description = description

        metadata = translate_esm_metadata(cat, translator)

        return cls(cat, metadata)

    @classmethod
    def load_esm(cls, json_file, translator, **kwargs):
        """
        Load an existing intake-esm catalog

        Parameters
        ----------
        json_file: str
            The path to the intake-esm catalog JSON file
        translator: :py:class:`~catalog_manager.translators.MetadataTranslator`
            An instance of the :py:class:`~catalog_manager.translators.MetadataTranslator` class for
            translating intake-esm column metadata into intake-dataframe-catalog column metadata
        kwargs: dict
            Additional kwargs to pass to :py:class:`~intake.open_esm_datastore`
        """

        cat = intake.open_esm_datastore(json_file, **kwargs)
        metadata = translate_esm_metadata(cat, translator)

        return cls(cat, metadata)

    def add(self, name, directory=None, **kwargs):
        """
        Add the catalog to an intake-dataframe-catalog

        Parameters
        ----------
        name: str
            The name of the intake-dataframe-catalog
        directory: str
            The directory to save the DF catalog to. If None, use the current directory.
        kwargs: dict, optional
            Additional keyword arguments passed to :py:func:`~pandas.DataFrame.to_csv`.
        """

        fname = os.path.join(directory, f"{name}.csv")
        csv_kwargs = {"index": False}
        csv_kwargs.update(kwargs or {})
        compression = csv_kwargs.get("compression")
        extensions = {
            "gzip": ".gz",
            "bz2": ".bz2",
            "zip": ".zip",
            "xz": ".xz",
            None: "",
        }
        fname = f"{fname}{extensions[compression]}"

        if os.path.exists(fname):
            dfcat = DFCatalogModel.load(
                fname,
                yaml_column=CoreDFMetadata.yaml_column,
                name_column=CoreDFMetadata.name_column,
            )
        else:
            dfcat = DFCatalogModel(
                yaml_column=CoreDFMetadata.yaml_column,
                name_column=CoreDFMetadata.name_column,
                metadata_columns=list(
                    set(CoreDFMetadata.columns) - set([CoreDFMetadata.name_column])
                ),
            )

        overwrite = True
        for _, row in self.metadata.iterrows():
            dfcat.add(self.cat, row.to_dict(), overwrite=overwrite)
            overwrite = False

        dfcat.save(name, directory, **kwargs)


def translate_esm_metadata(
    cat, translator=SimpleMetadataTranslator, groupby=CoreDFMetadata.groupby_columns
):
    """
    Parse metadata table to include in the intake-dataframe-catalog from an intake-esm dataframe
    and merge into a set of rows with unique values of the columns specified in groupby.

    Parameters
    ----------
    translator: :py:class:`~catalog_manager.translators.MetadataTranslator`
        An instance of the :py:class:`~catalog_manager.translators.MetadataTranslator` class for
        translating intake-esm column metadata into intake-dataframe-catalog column metadata. Defaults
        to catalog_manager.translators.SimpleMetadataTranslator which assumes all core
        intake-dataframe-catalog columns are present in the intake-esm catalog.
    groupby: list of str, optional
        Core metadata columns to group by before merging metadata across remaining core columns.
        Defaults to catalog_manager.CoreDFMetadata.groupby_columns
    """

    def _list_unique(series):
        # TODO: This could be made more robust
        iterable_entries = isinstance(series.iloc[0], (list, tuple, set))
        uniques = sorted(
            set(
                series.drop_duplicates()
                .apply(lambda x: x if iterable_entries else [x])
                .sum()
            )
        )
        return uniques[0] if (len(uniques) == 1) & (not iterable_entries) else uniques

    ungrouped_columns = list(set(CoreDFMetadata.columns) - set(groupby))

    metadata = translator.translate(cat)
    metadata = (
        metadata.groupby(groupby)
        .agg({col: _list_unique for col in ungrouped_columns})
        .reset_index()
    )

    return metadata[
        list(CoreDFMetadata.columns)
    ]  # Order according to entries in CoreDFMetadata.columns
