# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Manager for adding/updating intake sources in an intake-dataframe-catalog like the ACCESS-NRI catalog """

import os

import intake
from intake_dataframe_catalog.core import DfFileCatalog

from ..utils import validate_against_schema
from . import (
    CATALOG_JSONSCHEMA,
    COLUMNS_WITH_ITERABLES,
    CORE_COLUMNS,
    NAME_COLUMN,
    TRANSLATOR_GROUPBY_COLUMNS,
    YAML_COLUMN,
)
from .translators import DefaultTranslator


class CatalogManagerError(Exception):
    "Generic Exception for the CatalogManager class"
    pass


class CatalogManager:
    """
    Add/update intake sources in an intake-dataframe-catalog like the ACCESS-NRI catalog
    """

    def __init__(self, path):
        """
        Initialise a CatalogManager instance to add/update intake sources in a
        intake-dataframe-catalog like the ACCESS-NRI catalog

        Parameters
        ----------
        path: str
            The path to the intake-dataframe-catalog
        """

        self.path = path

        mode = "a" if os.path.exists(path) else "w"

        self.dfcat = DfFileCatalog(
            path=self.path,
            yaml_column=YAML_COLUMN,
            name_column=NAME_COLUMN,
            mode=mode,
            columns_with_iterables=COLUMNS_WITH_ITERABLES,
        )

        self.source = None
        self.source_metadata = None

    def build_esm(
        self,
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
        builder: subclass of :py:class:`access_nri_catalog.esmcat.BaseBuilder`
            The builder to use to build the intake-esm catalog
        path: str or list of str
            Path or list of paths to crawl for assets/files to add to the catalog.
        translator: :py:class:`~access_nri_catalog.metacat.translators.DefaultTranslator`
            An instance of the :py:class:`~access_nri_catalog.metacat.translators.DefaultTranslator` class
            for translating info in the intake-esm catalog into intake-dataframe-catalog column metadata.
            Defaults to access_nri_catalog.metacat.translators.DefaultTranslator.
        metadata: dict, optional
            Additional info to store in the intake cat.metadata attribute. This info will be available
            to the translator and to users of the catalog
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
                raise CatalogManagerError(
                    f"A catalog already exists for {name}. To overwrite, "
                    "pass `overwrite=True` to CatalogBuilder.build"
                )

        builder = builder(path, **kwargs).build()
        builder.save(name=name, description=description, directory=directory)

        self.source, self.source_metadata = _open_and_translate(
            json_file,
            name,
            description,
            metadata,
            translator,
            columns_with_iterables=list(builder.columns_with_iterables),
        )

        return self

    def load(
        self,
        name,
        description,
        path,
        translator,
        metadata=None,
        **kwargs,
    ):
        """
        Load an existing intake catalog and add it to the catalog

        Parameters
        ----------
        name: str
            The name of the catalog
        description: str
            Description of the contents of the catalog
        path: str
            The path to the intake-esm catalog JSON file
        translator: :py:class:`~access_nri_catalog.metacat.translators.DefaultTranslator`
            An instance of the :py:class:`~access_nri_catalog.metacat.translators.DefaultTranslator` class for
            translating intake-esm column metadata into intake-dataframe-catalog column metadata
        metadata: dict, optional
            Additional info to store in the intake cat.metadata attribute. This info will be available to
            the translator and to users of the catalog
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

        self.source, self.source_metadata = _open_and_translate(
            path, name, description, metadata, translator, **kwargs
        )

        return self

    def add(self, **kwargs):
        """
        Add a source to the catalog

        Parameters
        ----------
        kwargs: dict, optional
            Additional keyword arguments passed to :py:func:`~pandas.DataFrame.to_csv`.
        """

        if self.source is None:
            raise CatalogManagerError(
                "To add a source to the catalog you must first load or build the source"
            )

        # Overwrite the catalog name with the name_column entry in metadata
        name = self.source_metadata[NAME_COLUMN].unique()
        if len(name) != 1:
            raise ValueError(
                f"Metadata column '{NAME_COLUMN}' must be the same for all rows "
                "since this corresponds to the catalog name"
            )
        name = name[0]
        self.source.name = name

        # Validate source_metadata against schema
        for idx, row in self.source_metadata.iterrows():
            validate_against_schema(row.to_dict(), CATALOG_JSONSCHEMA)

        overwrite = True
        for _, row in self.source_metadata.iterrows():
            self.dfcat.add(self.source, row.to_dict(), overwrite=overwrite)
            overwrite = False

        self.dfcat.save(**kwargs)


def _open_and_translate(json_file, name, description, metadata, translator, **kwargs):
    """
    Open an esm-datastore, assign name, description and metadata attrs and
    translate using the provided translator
    """
    cat = intake.open_esm_datastore(json_file, **kwargs)
    cat.name = name
    cat.description = description
    cat.metadata = metadata

    metadata = translator(cat, CORE_COLUMNS).translate(TRANSLATOR_GROUPBY_COLUMNS)

    return cat, metadata
