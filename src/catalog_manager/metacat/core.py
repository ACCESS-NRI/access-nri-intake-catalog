# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Tools for managing intake catalogues """

import os

import jsonschema

import intake
from intake_dataframe_catalog.core import DFCatalogModel

from . import schema
from .translators import DefaultTranslator


_metacat_columns = list(schema["jsonschema"]["properties"].keys())


class CatalogExistsError(Exception):
    "Exception for trying to write catalog that already exists"
    pass


class CatalogManager:
    """
    Add/update intake catalogs in an intake-dataframe-catalog
    """

    def __init__(self, cat, df_metadata):
        """
        Initialise a CatalogManager instance to add/update intake catalogs in a
        intake-dataframe-catalog

        Parameters
        ----------
        cat: :py:class:`intake.DataSource`
            An intake catalog to append/update in the intake-dataframe-catalog
        df_metadata: :py:class:`~pandas.DataFrame`
            :py:class:`~pandas.DataFrame` containing metadata associated with cat to include as
            columns in the intake-dataframe-catalog. If adding to an existing
            intake-dataframe-catalog, the columns in df_metadata must be the same as those in the
            existing intake-dataframe-catalog.
        """

        # Overwrite the catalog name with the name_column entry in metadata
        name = df_metadata[schema["name_column"]].unique()
        if len(name) != 1:
            raise ValueError(
                f"Metadata column '{schema['name_column']}' must be the same for all rows "
                "since this corresponds to the catalog name"
            )
        name = name[0]
        cat.name = name

        # Validate df_metadata against schema
        for idx, row in df_metadata.iterrows():
            jsonschema.validate(row.to_dict(), schema["jsonschema"])

        self.cat = cat
        self.df_metadata = df_metadata
        self.dfcat = None

    @classmethod
    def build_esm(
        cls,
        name,
        description,
        builder,
        path,
        translator=DefaultTranslator,
        metadata=None,
        directory=None,
        overwrite=False,
        **kwargs,
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
        path: str or list of str
            Path or list of paths to crawl for assets/files to add to the catalog.
        translator: :py:class:`~catalog_manager.translators.MetadataTranslator`
            An instance of the :py:class:`~catalog_manager.translators.MetadataTranslator` class for
            translating info in the intake-esm catalog into intake-dataframe-catalog column metadata.
            Defaults to catalog_manager.translators.DefaultTranslator.
        metadata: dict, optional
            Additional info to store in the intake cat.metadata attribute. This info will be available
            to MetadataTranslators and to users of the catalog
        directory: str
            The directory to save the catalog to. If None, use the current directory
        overwrite: bool, optional
            Whether to overwrite any existing catalog(s) with the same name
        kwargs: dict
            Additional kwargs to pass to the builder
        """

        metadata = metadata or {}

        json_file = os.path.abspath(f"{os.path.join(directory, name)}.json")
        if os.path.isfile(json_file):
            if not overwrite:
                raise CatalogExistsError(
                    f"A catalog already exists for {name}. To overwrite, "
                    "pass `overwrite=True` to CatalogBuilder.build"
                )

        builder = builder(path, **kwargs).build()
        builder.save(name=name, description=description, directory=directory)

        cat, df = _open_and_translate(
            json_file,
            name,
            description,
            metadata,
            translator,
            columns_with_iterables=list(builder.columns_with_iterables),
        )

        return cls(cat, df)

    @classmethod
    def load_esm(
        cls,
        name,
        description,
        path,
        translator,
        metadata=None,
        **kwargs,
    ):
        """
        Load an existing intake-esm catalog

        Parameters
        ----------
        name: str
            The name of the catalog
        description: str
            Description of the contents of the catalog
        path: str
            The path to the intake-esm catalog JSON file
        translator: :py:class:`~catalog_manager.translators.MetadataTranslator`
            An instance of the :py:class:`~catalog_manager.translators.MetadataTranslator` class for
            translating intake-esm column metadata into intake-dataframe-catalog column metadata
        metadata: dict, optional
            Additional info to store in the intake cat.metadata attribute. This info will be available
            to MetadataTranslators and to users of the catalog
        kwargs: dict, optional
            Additional kwargs to pass to :py:class:`~intake.open_esm_datastore`
        """

        if isinstance(path, list):
            if len(path) != 1:
                raise ValueError(
                    "Only a single JSON file can be passed to CatalogManager.load_esm. Received {len(path)}"
                )
            path = path[0]

        metadata = metadata or {}

        cat, df = _open_and_translate(
            path, name, description, metadata, translator, **kwargs
        )

        return cls(cat, df)

    def add(self, name, **kwargs):
        """
        Add the catalog to an intake-dataframe-catalog

        Parameters
        ----------
        name: str
            The path to the intake-dataframe-catalog
        kwargs: dict, optional
            Additional keyword arguments passed to :py:func:`~pandas.DataFrame.to_csv`.
        """

        if os.path.exists(name):
            dfcat = DFCatalogModel.load(
                name,
                yaml_column=schema["yaml_column"],
                name_column=schema["name_column"],
            )
        else:
            metadata_columns = _metacat_columns
            metadata_columns.remove(schema["name_column"])
            dfcat = DFCatalogModel(
                yaml_column=schema["yaml_column"],
                name_column=schema["name_column"],
                metadata_columns=metadata_columns,
            )

        overwrite = True
        for _, row in self.df_metadata.iterrows():
            dfcat.add(self.cat, row.to_dict(), overwrite=overwrite)
            overwrite = False

        dfcat.save(name, **kwargs)


def translate_esm_metadata(cat, translator, groupby=["model", "realm", "frequency"]):
    """
    Parse metadata table to include in the intake-dataframe-catalog from an intake-esm catalog
    and merge into a set of rows with unique values of the columns specified in groupby.

    Parameters
    ----------
    cat: :py:class:`intake-esm.esm_datastore`
        An intake-esm catalog
    translator: :py:class:`~catalog_manager.translators.MetadataTranslator`
        An instance of the :py:class:`~catalog_manager.translators.MetadataTranslator` class for
        translating intake-esm column metadata into intake-dataframe-catalog column metadata. Defaults
        to catalog_manager.translators.DefaultTranslator.
    groupby: list of str, optional
        Core metadata columns to group by before merging metadata across remaining core columns.
        Defaults to ["model", "realm", "frequency"]
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

    ungrouped_columns = list(set(_metacat_columns) - set(groupby))

    metadata = translator.translate(cat)
    metadata = (
        metadata.groupby(groupby)
        .agg({col: _list_unique for col in ungrouped_columns})
        .reset_index()
    )

    return metadata[list(_metacat_columns)]  # Ensure ordered correctly


def _open_and_translate(json_file, name, description, metadata, translator, **kwargs):
    """
    Open an esm-datastore, assign name, description and metadata attrs and
    translate using the provided translator
    """
    cat = intake.open_esm_datastore(json_file, **kwargs)
    cat.name = name
    cat.description = description
    cat.metadata = metadata

    metadata = translate_esm_metadata(cat, translator)

    return cat, metadata
