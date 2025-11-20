# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Manager for adding/updating intake sources in an intake-dataframe-catalog like the ACCESS-NRI catalog"""

from pathlib import Path

import intake
from intake_dataframe_catalog.core import DfFileCatalog, DfFileCatalogError
from pandas.errors import EmptyDataError

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

    def __init__(self, path: Path | str):
        """
        Initialise a CatalogManager instance to add/update intake sources in a
        intake-dataframe-catalog like the ACCESS-NRI catalog

        Parameters
        ----------
        path: str
            The path to the intake-dataframe-catalog
        """
        path = Path(path)

        self.path = str(path)

        self.mode = "a" if path.exists() else "w"

        try:
            self.dfcat = DfFileCatalog(
                path=self.path,
                yaml_column=YAML_COLUMN,
                name_column=NAME_COLUMN,
                mode=self.mode,
                columns_with_iterables=COLUMNS_WITH_ITERABLES,
            )
        except (EmptyDataError, DfFileCatalogError) as e:
            raise Exception(str(e) + f": {self.path}") from e

        self.source = None
        self.source_metadata = None

    def build_esm(
        self,
        name: str,
        description: str,
        builder,
        path: str | list[str],
        translator=DefaultTranslator,
        metadata: dict | None = None,
        directory: str | None = None,
        overwrite: bool = False,
        **kwargs,
    ):
        """
        Build an Intake-ESM datastore and add it to the catalog

        Parameters
        ----------
        name: str
            The name of the Intake-ESM datastore
        description: str
            Description of the contents of the Intake-ESM datastore
        builder: subclass of :py:class:`access_nri_intake.source.builders.BaseBuilder`
            The builder to use to build the Intake-ESM datastore
        path: str or list of str
            Path or list of paths to crawl for assets/files to add to the Intake-ESM datastore.
        translator: :py:class:`~access_nri_intake.catalog.translators.DefaultTranslator`, optional
            An instance of the :py:class:`~access_nri_intake.catalog.translators.DefaultTranslator` class
            for translating info in the Intake-ESM datastore into intake-dataframe-catalog column metadata.
            Defaults to access_nri_intake.catalog.translators.DefaultTranslator
        metadata: dict, optional
            Additional info to store in the intake cat.metadata attribute. This info will be available
            to the translator and to users of the Intake-ESM datastore
        directory: str
            The directory to save the Intake-ESM datastore to. If None, use the current directory
        overwrite: bool, optional
            Whether to overwrite if an Intake-ESM datastore with the same name already exists
        kwargs: dict
            Additional kwargs to pass to the builder
        """

        metadata = metadata or {}
        directory = directory or ""

        json_file = (Path(directory) / f"{name}.json").absolute()
        if json_file.is_file():
            if not overwrite:
                raise CatalogManagerError(
                    f"An Intake-ESM datastore already exists for {name}. To overwrite, "
                    "pass `overwrite=True` to CatalogBuilder.build_esm"
                )

        builder = builder(path, **kwargs).build()
        builder.save(name=name, description=description, directory=directory)

        self.source, self.source_metadata = _open_and_translate(
            str(json_file),
            "esm_datastore",
            name,
            description,
            metadata,
            translator,
            columns_with_iterables=list(builder.columns_with_iterables),
        )

        self._add()

    def load(
        self,
        name: str,
        description: str,
        path: str,
        driver: str = "esm_datastore",
        translator=DefaultTranslator,
        metadata: dict | None = None,
        **kwargs,
    ):
        """
        Load an existing data source using Intake and add it to the catalog

        Parameters
        ----------
        name: str
            The name of the data source
        description: str
            Description of the contents of the data source
        path: str
            The path to the Intake data source
        driver: str
            The name of the Intake driver to use to open the data source
        translator: :py:class:`~access_nri_catalog.metacat.translators.DefaultTranslator`, optional
            An instance of the :py:class:`~access_nri_catalog.metacat.translators.DefaultTranslator` class for
            translating data source metadata into intake-dataframe-catalog column metadata. Defaults to
            access_nri_intake.catalog.translators.DefaultTranslator
        metadata: dict, optional
            Additional info to store in the intake metadata attribute for this data source. This info will be
            available to the translator and to users of the catalog
        kwargs: dict, optional
            Additional kwargs to pass to :py:class:`~intake.open_<driver>`
        """

        if isinstance(path, list):
            if len(path) != 1:
                raise CatalogManagerError(
                    f"Only a single data source can be passed to CatalogManager.load. Received {len(path)}"
                )
            path = path[0]

        metadata = metadata or {}

        self.source, self.source_metadata = _open_and_translate(
            path, driver, name, description, metadata, translator, **kwargs
        )

        self._add()

    def _add(self):
        """
        Add a source to the catalog
        """

        if self.source is None:
            raise CatalogManagerError(
                "To add a source to the catalog you must first load or build the source"
            )

        # Overwrite the catalog name with the name_column entry in metadata
        name = self.source_metadata[NAME_COLUMN].unique()
        if len(name) != 1:
            raise CatalogManagerError(
                f"Metadata column '{NAME_COLUMN}' must be the same for all rows in source_metadata "
                "since this corresponds to the source name"
            )
        name = name[0]
        self.source.name = name

        # Validate source_metadata against schema
        for idx, row in self.source_metadata.iterrows():
            validate_against_schema(row.to_dict(), CATALOG_JSONSCHEMA)

        overwrite = True
        for _, row in self.source_metadata.iterrows():
            try:
                self.dfcat.add(self.source, row.to_dict(), overwrite=overwrite)
            except DfFileCatalogError as exc:
                # If we have 'iterable metadata' in the error message, it likely relates to
                # issues discussed at https://github.com/ACCESS-NRI/access-nri-intake-catalog/issues/223,
                # so if the error message contains 'iterable metadata', we wrap the error with some
                # additional information about catalog issues and then raise
                if "iterable metadata" in str(exc):
                    raise CatalogManagerError(
                        f"Error adding source '{name}' to the catalog due to iterable metadata issues. "
                        " See https://github.com/ACCESS-NRI/access-nri-intake-catalog/issues/223: likely"
                        " due to issues with 'model' column in the catalog"
                    ) from exc
                raise CatalogManagerError(exc)
            overwrite = False

    def save(self, **kwargs):
        """
        Save the catalog

        Parameters
        ----------
        kwargs: dict, optional
            Additional keyword arguments passed to :py:func:`~pandas.DataFrame.to_csv`.
        """
        self.dfcat.save(**kwargs)


def _open_and_translate(
    file, driver, name, description, metadata, translator, **kwargs
):
    """
    Open an Intake data source, assign name, description and metadata attrs and
    translate using the provided translator
    """
    open_ = getattr(intake, f"open_{driver}")
    source = open_(file, **kwargs)
    source.name = name
    source.description = description
    source.metadata = metadata

    metadata = translator(source, CORE_COLUMNS).translate(TRANSLATOR_GROUPBY_COLUMNS)

    return source, metadata
