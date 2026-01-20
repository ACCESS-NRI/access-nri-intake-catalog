# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Manager for adding/updating intake sources in an intake-dataframe-catalog like the ACCESS-NRI catalog"""

from pathlib import Path
from typing import Any

import intake
from intake_dataframe_catalog.core import DfFileCatalog, DfFileCatalogError
from intake_esm import esm_datastore
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

    def __init__(self, path: Path | str, use_parquet: bool = False):
        """
        Initialise a CatalogManager instance to add/update intake sources in a
        intake-dataframe-catalog like the ACCESS-NRI catalog

        Parameters
        ----------
        path: str
            The path to the intake-dataframe-catalog
        use_parquet: bool
            Whether to use parquet files instead of csv files. This will also save version info into
            the `parameters::version_pq` namespace, allowing us to separately track parquet & csv
            catalog versions, maintaining backwards compatibility with existing catalogs.
        """
        path = Path(path)

        self.path = str(path)
        self.use_parquet = use_parquet

        self.mode = "a" if path.exists() else "w"

        columns_with_iterables = (
            COLUMNS_WITH_ITERABLES if not Path.suffix == ".parquet" else None
        )

        try:
            self.dfcat = DfFileCatalog(
                path=self.path,
                yaml_column=YAML_COLUMN,
                name_column=NAME_COLUMN,
                mode=self.mode,
                columns_with_iterables=columns_with_iterables,
            )
        except (EmptyDataError, DfFileCatalogError) as e:
            raise Exception(str(e) + f": {self.path}") from e

        self.source: esm_datastore | None = None
        self.source_metadata: dict[str, Any] | None = None

    def build_esm(  # noqa: PLR0913 # Allow this func to have many arguments
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
        builder.save(
            name=name,
            description=description,
            directory=directory,
            use_parquet=self.use_parquet,
        )

        open_translate_kwargs = dict(
            file=str(json_file),
            driver="esm_datastore",
            name=name,
            description=description,
            metadata=metadata,
            translator=translator,
            columns_with_iterables=list(builder.columns_with_iterables),
        )
        if self.use_parquet:
            # Don't need to specify columns_with_iterables for parquet - serialised into format
            open_translate_kwargs.pop("columns_with_iterables")

        self.source, self.source_metadata = _open_and_translate(**open_translate_kwargs)

        self._add()

    def load(  # noqa: PLR0913 # Allow this func to have many arguments
        self,
        name: str,
        description: str,
        path: str,
        directory: str | None = None,
        driver: str = "esm_datastore",
        translator=DefaultTranslator,
        metadata: dict | None = None,
        **kwargs,
    ):
        """
        Load an existing data source using Intake and add it to the catalog. If
        it's an NCI datastore, reserialize it into the build directory as parquet,
        if we've specified a parquet build.

        Parameters
        ----------
        name: str
            The name of the data source
        description: str
            Description of the contents of the data source
        path: str
            The path to the Intake data source
        directory: str, optional
            The directory to save reserialized Intake data source to. Defaults to
            `Path(self.path).parent`
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

        directory = directory or str(Path(self.path).parent)
        metadata = metadata or {}

        source, source_metadata = _open_and_translate(
            path, driver, name, description, metadata, translator, **kwargs
        )

        self.source, self.source_metadata = source, source_metadata

        if self.use_parquet:
            self.source.serialize(
                name=source.name,
                directory=directory,
                catalog_type="file",
                file_format="parquet",
            )

            reserialized_path = str(Path(directory) / f"{source.name}.json")

            self.source, self.source_metadata = _open_and_translate(
                reserialized_path,
                driver,
                name,
                description,
                metadata,
                translator,
                **kwargs,
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


def _open_and_translate(  # noqa: PLR0913 # Allow this func to have many arguments
    file, driver, name, description, metadata, translator, **kwargs
) -> tuple[esm_datastore, dict]:
    """
    Open an Intake data source, assign name, description and metadata attrs and
    translate using the provided translator
    """
    if driver != "esm_datastore":
        raise CatalogManagerError(f"Driver '{driver}' not supported in CatalogManager")
    open_ = getattr(intake, f"open_{driver}")
    source = open_(file, **kwargs)
    source.name = name
    source.description = description
    source.metadata = metadata

    metadata = translator(source, CORE_COLUMNS).translate(TRANSLATOR_GROUPBY_COLUMNS)

    return source, metadata
