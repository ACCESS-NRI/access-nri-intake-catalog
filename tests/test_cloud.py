# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path

import polars as pl
import pytest

from access_nri_intake.cloud import CatalogMirror


def test_entrypoint():
    """
    Check that entry point works
    """
    exit_status = os.system("mirror-to-cloud --help")
    assert exit_status == 0


class TestCatalogMirror:
    """
    Tests for the CatalogMirror class.
    """

    @pytest.fixture
    def catalog_lf(self, test_data) -> pl.LazyFrame:
        return pl.scan_csv(test_data / "esm_datastore" / "access-ct11.csv")

    @pytest.fixture
    def tmp_dataframe_catfile(self, tmp_path, test_data) -> Path:
        """

        Use a temporary, *real* dataframe catalog instead of a mock one, because there's too much wacky
        intake behaviour we can't easily mock that we need to test.

        stolen from tests/test_manager.py/test_CatalogManager_all
        """

        from access_nri_intake.catalog import EXP_JSONSCHEMA
        from access_nri_intake.catalog.manager import CatalogManager
        from access_nri_intake.catalog.translators import Cmip5Translator
        from access_nri_intake.source.builders import (AccessOm2Builder,
                                                       AccessOm3Builder)
        from access_nri_intake.utils import load_metadata_yaml

        path = str(tmp_path / "cat.parquet")
        cat = CatalogManager(path)

        # Load source
        load_args = dict(
            name="cmip5-al33",
            description="cmip5-al33",
            path=str(test_data / "esm_datastore/cmip5-al33.json"),
            translator=Cmip5Translator,
        )
        cat.load(
            **load_args,  # type: ignore
        )

        # Build a couple of sources
        models = {"access-om2": AccessOm2Builder, "access-om3": AccessOm3Builder}
        for model, builder in models.items():
            metadata = load_metadata_yaml(
                str(test_data / model / "metadata.yaml"), EXP_JSONSCHEMA
            )
            cat.build_esm(
                name=model,
                description=model,
                builder=builder,
                path=str(test_data / model),
                metadata=metadata,
                directory=str(tmp_path),
            )

        # Check that entry with same name overwrites correctly
        cat.load(
            **load_args,  # type: ignore
        )
        cat.save()

        return Path(path)

    def test_init(self):
        assert True

    def test___call__(self):
        assert True

    def test_mirror_intake_catalog(self):
        assert True

    def test_restructure_metacat(self, tmp_dataframe_catfile):
        df_init = pl.read_parquet(tmp_dataframe_catfile)

        cat_mirror = CatalogMirror()

        # Update the path to the dataframe catalog file to point to our temporary one,
        # which has a real catalog in it, so we can test the restructure function properly.
        cat_mirror.metacat_path = tmp_dataframe_catfile
        cat_mirror.restructure_metacat()

        df_restructured = pl.read_parquet(tmp_dataframe_catfile)

        for name in df_init.get_column("name").unique():
            subset_df_init = df_init.filter(pl.col("name") == name)
            subset_df_restruc = df_restructured.filter(pl.col("name") == name)
            for colname in ["model", "realm", "frequency", "variable"]:
                # restructuring just collapses init_df into non-singleton lists, so the unique
                # values in each column should be the same
                assert set(
                    subset_df_init.get_column(colname).explode().unique()
                ) == set(subset_df_restruc.get_column(colname).explode().unique())

    def test_update_esm_datastores(self):
        assert True

    def test_create_sidecar_files(self):
        assert True

    def test__create_datastore_metadata(self):
        assert True

    def test_partition_parquet_files(self):
        assert True

    def test__get_project_id(self, catalog_lf: pl.LazyFrame):
        mirror = CatalogMirror()
        project_id = mirror._get_project_id(catalog_lf)
        assert project_id == "ct11"

    def test_write_to_object_store(self):
        assert True
