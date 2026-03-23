# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import argparse
import json
import logging
import tempfile
from collections.abc import Sequence
from datetime import date
from pathlib import Path

import openstack
import polars as pl
import pyarrow.parquet as pq
import swiftclient
from fabric import Connection

logger = logging.getLogger(__name__)
log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=log_fmt)

# PARTITION_TABLE, CONTAINER_HEADERS, and ROW_GROUP_SIZE could all be put in a config file or something.
# TODO

"""
Partition table for various datasets. Used to determine how to partition the
parquet files for efficient querying - we want to have fast page loads.
"""
PARTITION_TABLE = {
    "cmip6_oi10": ["realm", "table_id"],
    "cmip6_fs38": ["realm", "table_id"],
    "cmip5_rr3": ["realm", "table"],
    "cmip5_al33": ["realm", "table"],
    "narclim2_zz63": ["experiment_id", "frequency"],
    "barpa_py18": ["source_id", "domain_id", "freq"],
    "cordex_ig45": ["experiment_id", "frequency"],
    "era5_rt52": ["product", "levtype"],
    "rcm_ccam_hq89": ["experiment_id", "version"],
    "01deg_jra55v140_iaf_cycle4": ["frequency"],
    "cj877": ["realm", "frequency"],
    "bz687": ["realm", "frequency"],
    "01deg_jra55v140_iaf": ["realm", "frequency"],
    "01deg_jra55v140_iaf_cycle2": ["realm", "frequency"],
    "01deg_jra55v140_iaf_cycle3": ["realm", "frequency"],
    "PI_GWL_B2060": ["realm"],
    "PI_GWL_B2055": ["realm"],
    "PI_GWL_B2045": ["realm"],
    "PI_GWL_B2035": ["realm"],
    "PI_GWL_B2050": ["realm"],
    "PI_GWL_B2040": ["realm"],
    "cmip-forcing-qv56": ["realm"],
}

"""
Headers to set on the container when we upload it to object storage.  These are
required to make sure the files are readable by anyone, and that we can do range
requests on them (for efficient querying).
"""
CONTAINER_HEADERS = {
    "X-Container-Read": ".r:*",
    "X-Container-Meta-Access-Control-Allow-Origin": "*",
    "X-Container-Meta-Access-Control-Allow-Methods": "GET, HEAD",
    "X-Container-Meta-Access-Control-Allow-Headers": "Range",
    "X-Container-Meta-Access-Control-Expose-Headers": "Accept-Ranges, Content-Length, Content-Range",
}

BUCKET_BASE_URL = "https://object-store.rc.nectar.org.au/v1/AUTH_685340a8089a4923a71222ce93d5d323/access-nri-intake-catalog"

"""
See https://stackoverflow.com/questions/76782018/what-is-actually-meant-when-referring-to-parquet-row-group-size
We are tuning down row group size here because we use this files to render an interactive UI, so we're less interested in
total throughput and more interested in getting the first few rows as quickly as possible.
"""
ROW_GROUP_SIZE = 10_000


class CatalogMirror:
    """Mirror the intake catalog to the datalake.

    Implementation Notes:

    Could be improved with:
    - Fault Tolerance (Currently, one file breaking will break the whole thing).
    - Batching/Async (Fetch/Post multiple files at once)
    - Steaming (Is it totally necessary to download everything, do the work, and then post it? Smaller memory footprint might be helpful.)
    """

    def __init__(self) -> None:
        self.bucket_name = "access-nri-intake-catalog"
        self.local_json_files: list[Path] = []
        self.local_pq_files: list[Path] = []

        self.failed_json_files: list[Path] = []
        self.failed_pq_files: list[Path] = []
        self.local_mirror_path = Path(tempfile.TemporaryDirectory().name)
        self.metacat_path = self.local_mirror_path / "metacatalog.parquet"
        self.basedir = Path("/g/data/xp65/public/apps/access-nri-intake-catalog/")

    def __call__(self, catalog_version: date, hidden: bool) -> None:
        """Main execution method."""

        # Cache catalog_version & hidden as instance attributes (mostly for testing)
        self.catalog_version = catalog_version
        self.hidden = hidden

        try:
            self.mirror_intake_catalog(catalog_version=catalog_version, hidden=hidden)
            logger.info("Successfully mirrored intake catalog.")
        except Exception as e:
            raise SystemExit(1, "Error mirroring intake catalog") from e

        logger.info("Restructuring metacatalog parquet file...")
        self.restructure_metacat()
        logger.info("Metacatalog restructured successfully.")

        logger.info("Updating ESM datastore parquet file path fields...")
        self.update_esm_datastores()
        logger.info("ESM datastore parquet files updated successfully.")

        logger.info("Creating sidecar files for ESM datastores...")
        self.create_sidecar_files()
        logger.info("ESM datastore sidecar files created successfully.")

        logger.info("Writing metadata json files...")
        self._create_datastore_metadata()
        logger.info("Metadata json successfully written.")

        logger.info("Partitioning ESM datastore parquet files...")
        self.partition_parquet_files()
        logger.info("ESM datastore parquet files partitioned successfully.")

        if self.failed_pq_files:
            logger.info("Failed parquet files:")
            for f in self.failed_pq_files:
                logger.info(" - %s", f)
        else:
            logger.info("No failed parquet files.")

        if self.failed_json_files:
            logger.info("Failed JSON files:")
            for f in self.failed_json_files:
                logger.info(" - %s", f)
        else:
            logger.info("No failed JSON files.")

        logger.info("Writing mirrored catalog to object storage...")
        self.write_to_object_storage()

    def mirror_intake_catalog(
        self,
        catalog_version: date | None = None,
        hidden: bool = False,
    ) -> None:
        """
        Mirrors the intake catalog to the datalake. Works by scp'ing the specified
        folder off of Gadi, and then doing a bit of processing to get it into the format
        we want for this server.

        Parameters
        ----------
        version : date
            The version date of the intake catalog to mirror. Defaults to today's date.
        hidden : bool
            Whether to mirror a hidden version of the catalog (prefixed with a dot). Defaults to False

        Returns
        None

        Notes
        -----
        This function requires SSH access to Gadi and the Fabric library. As of right now,
        it will just copy a file structure to a local temp folder - further processing
        will be needed to integrate it into the datalake structure.
        """

        catalog_version = catalog_version or date.today()
        conn = Connection("gadi")

        dotstr = "." if hidden else ""
        version_dir = f"{dotstr}v{catalog_version.isoformat()}"

        remote_path = self.basedir / version_dir
        source_dir = self.basedir / version_dir / "source"

        logger.info(
            "Mirroring intake catalog from %s to %s",
            remote_path,
            self.local_mirror_path,
        )

        metacat_file = self.basedir / version_dir / "metacatalog.parquet"

        logger.info(
            "Downloading metacatalog file: %s to %s",
            metacat_file,
            self.local_mirror_path,
        )

        conn.get(
            metacat_file,
            local=f"{str(self.local_mirror_path)}/",
            preserve_mode=False,
        )

        logger.info("Metacatalog file transferred successfully!")

        sftp = conn.sftp()

        logger.info("sftp initiated: listing %s contents", source_dir)
        sourcedir_contents: list[str] = sftp.listdir(str(source_dir))

        pq_files = [f for f in sourcedir_contents if Path(f).suffix == ".parquet"]
        json_files = [f for f in sourcedir_contents if Path(f).suffix == ".json"]

        if len(pq_files) != len(json_files):
            raise ValueError(
                "Mismatch between number of parquet and json files in source directory."
            )

        logger.info("Found %d esm-datastores in source directory.", len(pq_files))

        Path(self.local_mirror_path / "source").mkdir(parents=True, exist_ok=True)

        for idx, pq_file in enumerate(pq_files):
            remote_file_path = source_dir / pq_file
            local_file_path = self.local_mirror_path / "source" / Path(pq_file).name
            self.local_pq_files.append(local_file_path.absolute())

            logger.info(
                "Transferring file %d/%d: %s",
                idx + 1,
                2 * len(pq_files),
                remote_file_path.name,
            )
            conn.get(
                str(remote_file_path),
                local=str(local_file_path),
                preserve_mode=False,
            )

        logger.info("All parquet files transferred successfully!")

        for idx, json_file in enumerate(json_files):
            remote_file_path = source_dir / json_file
            local_file_path = self.local_mirror_path / "source" / Path(json_file).name

            logger.info(
                "Transferring file %d/%d: %s",
                idx + len(json_files) + 1,
                2 * len(json_files),
                remote_file_path.name,
            )
            conn.get(
                str(remote_file_path),
                local=str(local_file_path),
                preserve_mode=True,
            )
            self.local_json_files.append(local_file_path)

    def restructure_metacat(self) -> None:
        """
        We need to go into the parquet files we've just mirrrored and make a few
        changes.

        This collapses duplicate names, aggregating lists columns together. This
        effectively removes the `123 entries across 3000 rows` structure in the
        dataframe catalog. It could be removed in future if users find it unhelpful.
        """

        lf = pl.scan_parquet(self.metacat_path)
        (
            lf.group_by("name")
            .agg(
                [
                    pl.col("model").flatten().unique(),
                    pl.col("description").first(),
                    pl.col("realm").flatten().unique(),
                    pl.col("frequency").flatten().unique(),
                    pl.col("variable").flatten().unique(),
                    pl.col("yaml").first(),
                ]
            )
            .collect()
            .write_parquet(self.metacat_path, compression="zstd")
        )

    def update_esm_datastores(self) -> None:
        """
        We need to go into each of the esm-datastore parquet files and make a few
        changes. Most important, we need to change the `catalog_file` field to
        point to the one next door to it.
        """

        for file in self.local_json_files:
            try:
                pl.read_json(file).with_columns(
                    pl.concat_str(
                        [
                            pl.lit(f"{BUCKET_BASE_URL}/source/"),
                            pl.col("catalog_file").str.split("/").list.last(),
                        ]
                    ).alias("catalog_file")
                ).write_ndjson(file)
            except Exception as e:
                logger.error("Error updating JSON file %s: %s", file, e)
                self.failed_json_files.append(file)

    def create_sidecar_files(self) -> None:
        """
        Create sidecar files for each of the esm-datastore parquet files. These contain a single row, which is a list of all the available values in their corresponding main parquet files.

        We also write the number of records into the parquet metadata.
        """

        self.sidecar_files: list[Path] = []

        for fhandle in self.local_pq_files:
            sidecar_fname = fhandle.parent / f"{fhandle.stem}_uniqs.parquet"
            schema = pl.read_parquet_schema(fhandle)

            lf = pl.scan_parquet(fhandle)

            n_rows: int = lf.select(pl.len()).collect()[0, 0]
            lf.select(
                [
                    *[  # Uniques in non-list string columns
                        pl.col(colname).unique().implode()
                        for colname, dtype in schema.items()
                        if colname != "path" and dtype == pl.Utf8
                    ],
                    *[  # Uniques in list columns
                        pl.col(colname).explode().unique().implode()
                        for colname, dtype in schema.items()
                        if colname != "path" and dtype == pl.List
                    ],
                ]
            ).sink_parquet(sidecar_fname)

            # Then open the sidecarfile with arrow and add a num_records metadata field
            schema = pq.read_schema(sidecar_fname)
            table = pq.read_table(sidecar_fname)

            metadata = schema.metadata or {}
            metadata[b"num_records"] = str(n_rows).encode("utf8")

            table = table.replace_schema_metadata(metadata)

            pq.write_table(table, sidecar_fname)

            self.sidecar_files.append(sidecar_fname)

    def _create_datastore_metadata(self) -> None:
        """
        Create a separate sidecar metadata json file for each of the esm-datastore
        parquet files, containing information about the project_id and number of
        records in the parquet file.  This is to avoid having to read the parquet
        files to get this information when we want to display it on the frontend.

        Note: num_records is also written into the parquet metadata in the _create_sidecar_files
        method, but this is a bit of a hack and not easily accessible without
        reading the parquet file, so we write it out separately here for easy access.

        The frontend currently reads from the parquet metadata, so keep both hanging
        around until we update that.
        """

        for fhandle in self.local_pq_files:
            lf = pl.scan_parquet(fhandle)
            project_id = self._get_project_id(lf)
            num_records = lf.collect().height

            metadata_json = {
                "project_id": project_id,
                "num_records": num_records,
            }

            fname = f"{fhandle.stem}_metadata.json"
            with open(fhandle.parent / fname, "w") as f:
                json.dump(metadata_json, f)

    def partition_parquet_files(self) -> None:
        """
        Take each of the esm-datastore parquet files and partition them according
        to the PARTITION_TABLE above, before sorting non-partitioned columns using
        their cardinality.

        This should optimise internal file structure for expected access patterns to
        make it as easy as possible for the interactive catalog to just grab the row groups it needs.

        Notes
        -----

        - Row groups sizes are tuned down to 10,000 to optimise for fast page loads in the interactive catalog,
        rather than total throughput.
        - We collect the whole dataframe in memory and then unlink the original file before we write it out,
        because if we partition, we need to change eg. `FILE.parquet` from a file to a folder, which
        the operating system won't let us do without unlinking first. This might be able to be optimised if we run into memory issues.
        - We sort the data by the top 3 least cardinal columns that aren't partition columns, to try and optimise for
        common access patterns in the interactive catalog. TLDR; if we have a column with eg. 10 values, and one with 100 values,
        we're better off sorting by the one with 10 values first, because that will make it more likely that the row groups we
        need to load for a given query will be contiguous. This means it's more likely we can skip row groups, partitions, etc,
        which minimises I/O, fetching, and should speed up page loads.
        """
        for fhandle in self.local_pq_files:
            try:
                datastore_name = fhandle.stem
                partition_cols = PARTITION_TABLE.get(datastore_name, [])

                if not partition_cols:
                    logger.warning(
                        "No partitioning information for datastore %s, skipping partitioning.",
                        datastore_name,
                    )
                    logger.info("Changing row group size to 10,000")
                    pl.scan_parquet(fhandle).collect().write_parquet(
                        fhandle,
                        row_group_size=ROW_GROUP_SIZE,
                    )
                    continue
                else:
                    logger.info("Partitioning datastore %s", datastore_name)

                schema = pl.read_parquet_schema(fhandle)
                lf = pl.scan_parquet(fhandle)

                sort_cols = [
                    col
                    for col, dtype in schema.items()
                    if col not in partition_cols and dtype == pl.Utf8
                ]
                cardinalities = (
                    lf.select(
                        pl.col(colname).unique().implode().list.len()
                        for colname in sort_cols
                    )
                    .collect()
                    .to_dicts()[0]
                )

                sort_on = sorted(cardinalities.keys(), key=lambda k: cardinalities[k])[
                    :3
                ]

                df = lf.sort(
                    sort_on
                ).collect()  # Collect before unlinking, otherwise we can't write our partitioned files out (need in memory)

                Path(fhandle).unlink()
                Path(fhandle).mkdir(parents=True, exist_ok=True)

                df.write_parquet(
                    fhandle,
                    partition_by=partition_cols,
                    row_group_size=ROW_GROUP_SIZE,
                )
            except Exception as e:
                logger.error("Error partitioning parquet file %s: %s", fhandle, e)
                self.failed_pq_files.append(fhandle)

    def _get_project_id(self, lf: pl.LazyFrame) -> str:
        """
        Our df will always have a path column, containing something like `/g/data/xp65/...`. We are
        gonna grab that and return eg. `{'project_id': 'xp65'}`
        """

        return lf.head(1).collect().get_column("path").str.split("/").list.get(3)[0]

    def write_to_object_storage(self):
        """
        How do we get our hands on these credentials? Ask @jo-basevi is the
        current best guess... That's how I did it originally!
        """

        cloud = openstack.connect(cloud="openstack")

        # Get storage URL and token from the session
        # auth = cloud.session.get_auth_headers()
        storage_url = cloud.session.get_endpoint(service_type="object-store")
        token = cloud.session.get_token()

        conn = swiftclient.Connection(
            preauthurl=storage_url,
            preauthtoken=token,
        )

        conn.post_container(
            container="access-nri-intake-catalog",
            headers=CONTAINER_HEADERS,
        )

        objects = [f for f in Path(self.local_mirror_path).rglob("*") if f.is_file()]
        n_objs = len(objects)
        for idx, object in enumerate(objects):
            rel_path = object.relative_to(self.local_mirror_path)
            with open(object, "rb") as f:
                conn.put_object(
                    container="access-nri-intake-catalog",
                    obj=str(rel_path),
                    contents=f,
                )
            logger.info("%d/%d: Uploaded %s", idx, n_objs, rel_path)

        logger.info("Successfully uploaded %d files to object storage", len(objects))


def mirror_catalog(argv: Sequence[str] | None = None) -> None:
    """CLI entry point for mirroring the intake catalog."""
    parser = argparse.ArgumentParser(
        description="Mirror the intake catalog to the datalake.",
        epilog="Example usage: $ mirror-to-cloud --catalog-version 2024-06-01",
    )
    parser.add_argument(
        "--catalog-version",
        type=lambda d: date.fromisoformat(d),  # noqa: PLW0108
        default=date.today(),
        help="The version date of the intake catalog to mirror (YYYY-MM-DD). Defaults to today's date.",
    )
    parser.add_argument(
        "--hidden",
        action="store_true",
        help="Whether to mirror a hidden version of the catalog (prefixed with a dot). Defaults to False.",
    )

    args = parser.parse_args(argv)

    return CatalogMirror()(catalog_version=args.catalog_version, hidden=args.hidden)
