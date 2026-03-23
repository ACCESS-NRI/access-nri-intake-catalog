# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import os
from pathlib import Path
from unittest import mock

import polars as pl
import pytest

from access_nri_intake.cloud import CatalogMirror, mirror_catalog


def test_entrypoint():
    """
    Check that entry point works
    """
    exit_status = os.system("mirror-to-cloud --help")
    assert exit_status == 0


class MockCatalogMirror(CatalogMirror):
    def __init__(
        self,
        mirror_fail=False,
        pq_fail=False,
        json_fail=False,
    ):
        super().__init__()
        self.mirror_fail = mirror_fail

        self.failed_pq_files = ["fake_file.parquet"] if pq_fail else []
        self.failed_json_files = ["fake_file.json"] if json_fail else []

    def mirror_intake_catalog(self, *args, **kwargs) -> None:
        if not self.mirror_fail:
            self.mirror_intake_catalog_called = True
        else:
            raise Exception("Nonspecific failure...")

    def restructure_metacat(self) -> None:
        self.restructure_metacat_called = True

    def update_esm_datastores(self) -> None:
        self.update_esm_datastores_called = True

    def create_sidecar_files(self) -> None:
        self.create_sidecar_files_called = True

    def _create_datastore_metadata(self) -> None:
        self._create_datastore_metadata_called = True

    def partition_parquet_files(self) -> None:
        self.partition_parquet_files_called = True

    def write_to_object_storage(self) -> None:
        return None


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
        from access_nri_intake.source.builders import AccessOm2Builder, AccessOm3Builder
        from access_nri_intake.utils import load_metadata_yaml

        path = str(tmp_path / "cat.parquet")
        cat = CatalogManager(path, use_parquet=True)

        # Load source
        load_args = dict(
            name="cmip5_al33",
            description="cmip5_al33",
            path=str(test_data / "esm_datastore/cmip5_al33.json"),
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

    def test_update_esm_datastores(self, tmp_dataframe_catfile):
        tmpdir_loc = tmp_dataframe_catfile.parent.iterdir()
        json_files = [f for f in tmpdir_loc if f.suffix == ".json"]

        cat_mirror = CatalogMirror()
        cat_mirror.local_json_files = json_files
        cat_mirror.update_esm_datastores()

        # Now check that the json files have been updated with the new paths to the parquet files
        json_files_updated = [
            f for f in tmp_dataframe_catfile.parent.iterdir() if f.suffix == ".json"
        ]

        for f in json_files_updated:
            with open(f) as fobj:
                json_dict = json.load(fobj)
            assert json_dict["catalog_file"].startswith(
                "https://object-store.rc.nectar.org.au/v1/AUTH_685340a8089a4923a71222ce93d5d323/access-nri-intake-catalog/source/"
            )

    def test_update_esm_datastores_failure(self, tmp_dataframe_catfile):
        """
        Test that if we can't update json files, we just print an error and move on, not crash. Esily done by passing a parquet file in instead.
        """
        tmpdir_loc = tmp_dataframe_catfile.parent.iterdir()
        json_files = [f for f in tmpdir_loc if f.suffix == ".parquet"]

        cat_mirror = CatalogMirror()
        cat_mirror.local_json_files = json_files
        cat_mirror.update_esm_datastores()

        assert set(json_files) == set(cat_mirror.failed_json_files)

    def test_create_sidecar_files(self, tmp_dataframe_catfile):
        tmpdir_loc = tmp_dataframe_catfile.parent.iterdir()
        pq_files = [f for f in tmpdir_loc if f.suffix == ".parquet" and f.stem != "cat"]

        cat_mirror = CatalogMirror()
        cat_mirror.local_pq_files = pq_files

        cat_mirror.create_sidecar_files()

        file_pairs = [(f, f.with_name(f"{f.stem}_uniqs.parquet")) for f in pq_files]

        for pq_file, sidecar_file in file_pairs:
            df = pl.read_parquet(pq_file)
            sidecar_df = pl.read_parquet(sidecar_file)
            sidecar_metadata = pl.read_parquet_metadata(sidecar_file)

            assert int(sidecar_metadata["num_records"]) == len(df)

            str_cols = [
                col
                for col, dtype in sidecar_df.schema.items()
                if dtype == pl.Utf8 and col != "path"
            ]
            list_cols = [
                col
                for col, dtype in sidecar_df.schema.items()
                if dtype == pl.List(pl.Utf8)
            ]

            for col in str_cols:
                assert set(sidecar_df.get_column(col).explode().unique()) == set(
                    df.get_column(col).unique()
                )

            for col in list_cols:
                assert set(sidecar_df.get_column(col).explode().unique()) == set(
                    df.get_column(col).explode().unique()
                )

    def test__create_datastore_metadata(self, tmp_dataframe_catfile):
        tmpdir_loc = tmp_dataframe_catfile.parent.iterdir()
        pq_files = [f for f in tmpdir_loc if f.suffix == ".parquet" and f.stem != "cat"]

        cat_mirror = CatalogMirror()
        cat_mirror.local_pq_files = pq_files
        cat_mirror._create_datastore_metadata()

        tmpdir_loc_updated = tmp_dataframe_catfile.parent.iterdir()
        sidecar_files = set(
            (
                f
                for f in tmpdir_loc_updated
                if f.suffix == ".json" and f.stem.endswith("_metadata")
            )
        )
        sidecar_fnames = set(f.stem for f in sidecar_files)

        assert sidecar_fnames == set(
            ["access-om2_metadata", "access-om3_metadata", "cmip5_al33_metadata"]
        )

        # project_id is liable to change depending on where the tests are rn, so
        # we'll ignore it for now. I'm just leaving them in for potential future
        # debugging
        sidecars = {
            "access-om2_metadata": {"project_id": "catalog", "num_records": 12},
            "access-om3_metadata": {"project_id": "catalog", "num_records": 14},
            "cmip5_al33_metadata": {"project_id": "al33", "num_records": 5},
        }

        for f in sidecar_files:
            fname = f.stem
            with open(f) as fobj:
                n_records = json.load(fobj).get("num_records")
            assert n_records == sidecars[fname]["num_records"]

    def test_partition_parquet_files(self, tmp_dataframe_catfile, caplog):
        tmpdir_loc = tmp_dataframe_catfile.parent.iterdir()
        pq_files = [
            f
            for f in tmpdir_loc
            if f.suffix == ".parquet" and f.stem in ["cmip5_al33", "access-om2"]
        ]
        cat_mirror = CatalogMirror()
        cat_mirror.local_pq_files = pq_files

        with caplog.at_level(logging.INFO):
            cat_mirror.partition_parquet_files()
        assert Path(tmp_dataframe_catfile.parent / "cmip5_al33.parquet").is_dir()
        assert not Path(tmp_dataframe_catfile.parent / "access-om2.parquet").is_dir()

        assert (
            "No partitioning information for datastore access-om2, skipping partitioning."
            in caplog.text
        )
        assert "Changing row group size to 10,000" in caplog.text
        assert "Partitioning datastore" in caplog.text

    def test_partition_parquet_failure(self, tmp_dataframe_catfile):
        """Test that if we can't partition a file, we just print an error and move on, not crash. Easily done by passing a json file in instead."""
        tmpdir_loc = tmp_dataframe_catfile.parent.iterdir()
        pq_files = [
            f
            for f in tmpdir_loc
            if f.suffix == ".json" and f.stem in ["cmip5_al33", "access-om2"]
        ]
        cat_mirror = CatalogMirror()
        cat_mirror.local_pq_files = pq_files

        cat_mirror.partition_parquet_files()

        assert set(cat_mirror.failed_pq_files) == set(pq_files)

    def test__get_project_id(self, catalog_lf: pl.LazyFrame):
        mirror = CatalogMirror()
        project_id = mirror._get_project_id(catalog_lf)
        assert project_id == "ct11"

    @pytest.mark.parametrize(
        "mirror_fail, printout_mirror",
        [
            (True, "Error mirroring intake catalog"),
            (False, "Successfully mirrored intake catalog"),
        ],
    )
    @pytest.mark.parametrize(
        "pq_fail, printout_pq",
        [(True, "Failed parquet files:"), (False, "No failed parquet files")],
    )
    @pytest.mark.parametrize(
        "json_fail, printout_json",
        [(True, "Failed JSON files:"), (False, "No failed JSON files")],
    )
    def test___call__(
        self,
        mirror_fail,
        printout_mirror,
        pq_fail,
        printout_pq,
        json_fail,
        printout_json,
        caplog,
    ):
        mirror = MockCatalogMirror(mirror_fail, pq_fail, json_fail)

        with caplog.at_level(logging.INFO):
            if not mirror_fail:
                mirror(catalog_version="test", hidden=False)
                assert printout_mirror in caplog.text
            else:
                with pytest.raises(SystemExit, match="1") as excinfo:
                    mirror(catalog_version="test", hidden=False)
                    assert printout_mirror in caplog.text

        if mirror_fail:
            return None

        assert printout_pq in caplog.text
        assert printout_json in caplog.text

    @mock.patch("access_nri_intake.cloud.swiftclient.Connection")
    @mock.patch("access_nri_intake.cloud.openstack.connect")
    def test_write_to_object_store(self, mock_openstack, mock_swift, tmp_path):
        # Create real files so rglob picks them up
        (tmp_path / "source").mkdir()
        (tmp_path / "metacatalog.parquet").touch()
        (tmp_path / "source" / "ds_a.parquet").touch()
        (tmp_path / "source" / "ds_a.json").touch()

        mock_openstack.return_value.session.get_endpoint.return_value = (
            "https://fake-endpoint"
        )
        mock_openstack.return_value.session.get_token.return_value = "fake-token"
        mock_conn = mock_swift.return_value

        cat_mirror = CatalogMirror()
        cat_mirror.local_mirror_path = tmp_path
        cat_mirror.write_to_object_storage()

        mock_openstack.assert_called_once_with(cloud="nectar")
        mock_swift.assert_called_once_with(
            preauthurl="https://fake-endpoint",
            preauthtoken="fake-token",
        )
        mock_conn.post_container.assert_called_once_with(
            container="access-nri-intake-catalog",
            headers={
                "X-Container-Read": ".r:*",
                "X-Container-Meta-Access-Control-Allow-Origin": "*",
                "X-Container-Meta-Access-Control-Allow-Methods": "GET, HEAD",
                "X-Container-Meta-Access-Control-Allow-Headers": "Range",
                "X-Container-Meta-Access-Control-Expose-Headers": "Accept-Ranges, Content-Length, Content-Range",
            },
        )
        # One put_object call per real file under tmp_path
        expected_rel_paths = {
            Path("metacatalog.parquet"),
            Path("source") / "ds_a.parquet",
            Path("source") / "ds_a.json",
        }
        actual_rel_paths = {
            Path(call.kwargs["obj"]) for call in mock_conn.put_object.call_args_list
        }
        assert mock_conn.put_object.call_count == 3
        assert actual_rel_paths == expected_rel_paths
        for call in mock_conn.put_object.call_args_list:
            assert call.kwargs["container"] == "access-nri-intake-catalog"

    @mock.patch("access_nri_intake.cloud.Connection")
    def test_mirror_intake_catalog(self, mock_connection, tmp_path):
        mock_conn = mock_connection.return_value
        mock_sftp = mock_conn.sftp.return_value
        mock_sftp.listdir.return_value = [
            "ds_a.parquet",
            "ds_b.parquet",
            "ds_a.json",
            "ds_b.json",
        ]

        from datetime import date

        cat_mirror = CatalogMirror()
        cat_mirror.local_mirror_path = tmp_path
        cat_mirror.mirror_intake_catalog(catalog_version=date(2025, 1, 1), hidden=False)

        mock_connection.assert_called_once_with("gadi")
        assert mock_conn.get.call_count == 5  # 1 metacat + 2 pq + 2 json
        assert {f.stem for f in cat_mirror.local_pq_files} == {"ds_a", "ds_b"}
        assert {f.stem for f in cat_mirror.local_json_files} == {"ds_a", "ds_b"}
        assert all(f.parent == tmp_path / "source" for f in cat_mirror.local_pq_files)

    @mock.patch("access_nri_intake.cloud.Connection")
    def test_mirror_intake_catalog_hidden(self, mock_connection, tmp_path):
        mock_conn = mock_connection.return_value
        mock_sftp = mock_conn.sftp.return_value
        mock_sftp.listdir.return_value = ["ds_a.parquet", "ds_a.json"]

        from datetime import date

        cat_mirror = CatalogMirror()
        cat_mirror.local_mirror_path = tmp_path
        cat_mirror.mirror_intake_catalog(catalog_version=date(2025, 1, 1), hidden=True)

        # The metacatalog get call should use the hidden (dot-prefixed) version path
        metacat_call_arg = str(mock_conn.get.call_args_list[0].args[0])
        assert "/.v2025-01-01/" in metacat_call_arg

    @mock.patch("access_nri_intake.cloud.Connection")
    def test_mirror_intake_catalog_mismatch(self, mock_connection, tmp_path):
        mock_conn = mock_connection.return_value
        mock_sftp = mock_conn.sftp.return_value
        # 1 parquet but 2 json — mismatch
        mock_sftp.listdir.return_value = ["ds_a.parquet", "ds_a.json", "ds_b.json"]

        from datetime import date

        cat_mirror = CatalogMirror()
        cat_mirror.local_mirror_path = tmp_path
        with pytest.raises(ValueError, match="Mismatch"):
            cat_mirror.mirror_intake_catalog(
                catalog_version=date(2025, 1, 1), hidden=False
            )


@mock.patch("access_nri_intake.cloud.CatalogMirror", return_value=MockCatalogMirror())
@pytest.mark.parametrize(
    "argv, expected_mirror_args",
    [
        (
            ["--catalog-version", "v2025-01-01"],
            {"catalog_version": "2025-01-01", "hidden": False},
        ),
        (
            [
                "--catalog-version",
                ".v2000-01-01",
            ],
            {"catalog_version": "2000-01-01", "hidden": True},
        ),
    ],
)
def test_mirror_catalog(mock_CatalogMirror, argv, expected_mirror_args):
    mirror_catalog(argv)

    mock_CatalogMirror.assert_called_once_with()
    mock_CatalogMirror.hidden = expected_mirror_args["hidden"]
    mock_CatalogMirror.catalog_version = expected_mirror_args["catalog_version"]


@pytest.mark.parametrize(
    "argv",
    [
        ["--catalog-version", "v2025-13-01"],  # Not a real date, looks like one though
        [
            "--version",
            ".v2000-01-01",
        ],  # Wrong arg
        [
            "--catalog-version",
            "..v2000-01-01",
        ],  # Extra dot
        [
            "--catalog-version",
            ".2000-01-01",
        ],  # No v
    ],
)
def test_mirror_catalog_failure(argv):
    with pytest.raises(SystemExit, match="2"):
        mirror_catalog(argv)
