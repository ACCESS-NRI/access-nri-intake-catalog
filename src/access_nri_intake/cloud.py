# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import argparse
import sys
import tempfile
from collections.abc import Sequence
from datetime import date
from pathlib import Path

import openstack
import polars as pl
import swiftclient
from fabric import Connection

from access_nri_intake.experiment.colours import (
    f_err,
    f_info,
    f_path,
    f_reset,
    f_success,
    f_warn,
)

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


class CatalogMirror:
    """Mirror the intake catalog to the datalake.

    TODO:
    - [ ] Can we do this asynchronously without upsetting NCI
    - [ ] Logging
    - [ ] Fault tolerant transfer/ resume/ yadda yadda
    - [ ] Sidecar files
    - [ ] Do we really need to copy everything to local first?
    """

    def __init__(self, use_permanent: bool = False) -> None:
        self.bucket_name = "access-nri-intake-catalog"
        self.local_json_files: list[Path] = []
        self.local_pq_files: list[Path] = []

        self.failed_json_files: list[Path] = []
        self.failed_pq_files: list[Path] = []
        if not use_permanent:
            self.local_mirror_path = Path(tempfile.TemporaryDirectory().name)
        else:
            self.local_mirror_path = Path("~/scratch/intake-mirror/").expanduser()
        self.metacat_path = self.local_mirror_path / "metacatalog.parquet"
        self.basedir = Path("/g/data/xp65/public/apps/access-nri-intake-catalog/")

        # if we are on Gadi, we can use a local mirror path
        self.use_local_mirror = self.basedir.exists()

    def run(self, catalog_version: date, hidden: bool):
        """Main execution method."""

        try:
            self.mirror_intake_catalog(catalog_version=catalog_version, hidden=hidden)
            print(f"{f_success}Successfully mirrored intake catalog.{f_reset}")
        except Exception as e:
            print(
                f"{f_warn}Error mirroring intake catalog: {e}{f_reset}", file=sys.stderr
            )
            print(f"\n{f_warn}Usage examples:{f_reset}", file=sys.stderr)
            print("  python cloud.py")
            print("  python cloud.py --catalog-version 2025-11-01")
            print("  python cloud.py --hidden")
            print("  python cloud.py --catalog-version 2025-11-01 --hidden")
            sys.exit(1)

        print(f"{f_info}Restructuring metacatalog parquet file...{f_reset}")
        self.restructure_metacat()
        print(f"{f_success}Metacatalog restructured successfully.{f_reset}")

        print(f"{f_info}Updating ESM datastore parquet file path fields...{f_reset}")
        self.update_esm_datastores()
        print(f"{f_success}ESM datastore parquet files updated successfully.{f_reset}")

        print(f"{f_info}Creating sidecar files for ESM datastores...{f_reset}")
        self.create_sidecar_files()
        print(f"{f_success}ESM datastore sidecar files created successfully.{f_reset}")

        print(f"{f_info}Partitioning ESM datastore parquet files...{f_reset}")
        self.partition_parquet_files()
        print(
            f"{f_success}ESM datastore parquet files partitioned successfully.{f_reset}"
        )

        if self.failed_pq_files:
            print(f"{f_info}Failed parquet files:{f_reset}")
            for f in self.failed_pq_files:
                print(f" - {f}")
        else:
            print(f"{f_success}No failed parquet files.{f_reset}")

        if self.failed_json_files:
            print(f"{f_info}Failed JSON files:{f_reset}")
            for f in self.failed_json_files:
                print(f" - {f}")
        else:
            print(f"{f_success}No failed JSON files.{f_reset}")

        print(f"{f_info}Writing mirrored catalog to object storage...{f_reset}")
        self.write_to_object_storage()

    def mirror_intake_catalog(
        self, catalog_version: date = date.today(), hidden: bool = False
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
        conn = Connection("gadi")

        dotstr = "." if hidden else ""
        version_dir = f"{dotstr}v{catalog_version.isoformat()}"

        remote_path = self.basedir / version_dir
        source_dir = self.basedir / version_dir / "source"

        print(
            f"{f_info}Mirroring intake catalog from {f_path}{remote_path}{f_reset}{f_info} to {f_path}{self.local_mirror_path}{f_reset}"
        )

        metacat_file = self.basedir / version_dir / "metacatalog.parquet"

        print(
            f"{f_info}Downloading metacatalog file: {f_path}{metacat_file}{f_reset}{f_info} to {f_path}{self.local_mirror_path}{f_reset}"
        )

        conn.get(
            metacat_file,
            local=f"{str(self.local_mirror_path)}/",
            preserve_mode=False,
        )

        print(f"{f_success}Metacatalog file transferred sucessfully!{f_reset}")

        sftp = conn.sftp()

        print(
            f"{f_info}sftp initiated: listing {f_path}{source_dir}{f_reset}{f_info} contents{f_reset}"
        )
        sourcedir_contents = sftp.listdir(str(source_dir))

        pq_files = [f for f in sourcedir_contents if Path(f).suffix == ".parquet"]
        json_files = [f for f in sourcedir_contents if Path(f).suffix == ".json"]

        if len(pq_files) != len(json_files):
            raise ValueError(
                "Mismatch between number of parquet and json files in source directory."
            )

        print(
            f"{f_info}Found {len(pq_files)} esm-datastores in source directory.{f_reset}"
        )

        Path(self.local_mirror_path / "source").mkdir(parents=True, exist_ok=True)

        for idx, pq_file in enumerate(pq_files):
            remote_file_path = source_dir / pq_file
            local_file_path = self.local_mirror_path / "source" / Path(pq_file).name
            self.local_pq_files.append(local_file_path.absolute())

            print(
                f"{f_info}Transferring file {idx + 1}/{2 * len(pq_files)}: {f_path}{remote_file_path.name}{f_reset}"
            )
            conn.get(
                str(remote_file_path),
                local=str(local_file_path),
                preserve_mode=False,
            )

        print(f"{f_success}All parquet files transferred successfully!{f_reset}")

        for idx, json_file in enumerate(json_files):
            remote_file_path = source_dir / json_file
            local_file_path = self.local_mirror_path / "source" / Path(json_file).name

            print(
                f"{f_info}Transferring file {idx + len(json_files) + 1}/{2 * len(json_files)}: {f_path}{remote_file_path.name}{f_reset}"
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
            .write_parquet(self.metacat_path, compression="snappy")
        )

    def update_esm_datastores(self) -> None:
        """
        We need to go into each of the esm-datastore parquet files and make a few
        changes. Most important, we need to change the `catalog_file` field to
        point to the one next door to it. We'll also make sure all parquet files
        are snappy-compressed.
        """

        for file in self.local_json_files:
            try:
                pl.read_json(file).with_columns(
                    pl.concat_str(
                        [
                            pl.lit(
                                "https://object-store.rc.nectar.org.au/v1/AUTH_685340a8089a4923a71222ce93d5d323/access-nri-intake-catalog/source/"
                            ),
                            pl.col("catalog_file").str.split("/").list.last(),
                        ]
                    ).alias("catalog_file")
                ).write_ndjson(file)
            except Exception as e:
                print(
                    f"{f_err}Error updating JSON file {f_path}{file}{f_reset}: {e}{f_reset}",
                    file=sys.stderr,
                )
                self.failed_json_files.append(str(file))

    def create_sidecar_files(self) -> None:
        """
        Create sidecar files for each of the esm-datastore parquet files. These

        """

        self.sidecar_files: list[Path] = []

        for fhandle in self.local_pq_files:
            sidecar_fname = fhandle.parent / f"{fhandle.stem}_uniqs.parquet"
            schema = pl.read_parquet_schema(fhandle)

            lf = pl.scan_parquet(fhandle)
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

            self.sidecar_files.append(sidecar_fname)

    def partition_parquet_files(self) -> None:
        """
        Take each of the esm-datastore parquet files and partition them according
        to the PARTITION_TABLE above, before sorting non-partitioned columns using
        their cardinality
        """
        for fhandle in self.local_pq_files:
            try:
                datastore_name = fhandle.stem
                partition_cols = PARTITION_TABLE.get(datastore_name, False)

                if not partition_cols:
                    print(
                        f"{f_warn}No partitioning information for datastore {f_path}{datastore_name}{f_reset}, skipping partitioning."
                    )
                    continue
                else:
                    print(
                        f"{f_info}Partitioning datastore {f_path}{datastore_name}{f_reset}"
                    )

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

                sort_on = sorted(cardinalities.keys(), key=lambda k: -cardinalities[k])[
                    :3
                ]

                df = lf.sort(sort_on).collect()  # Collect before unlinking

                Path(fhandle).unlink()
                Path(fhandle).mkdir(parents=True, exist_ok=True)

                df.write_parquet(fhandle, partition_by=partition_cols)
            except Exception as e:
                print(
                    f"{f_err}Error partitioning parquet file {f_path}{fhandle}{f_reset}: {e}{f_reset}",
                    file=sys.stderr,
                )
                self.failed_pq_files.append(str(fhandle))

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
            headers={
                "X-Container-Read": ".r:*",
                "X-Container-Meta-Access-Control-Allow-Origin": "*",
                "X-Container-Meta-Access-Control-Allow-Methods": "GET, HEAD",
                "X-Container-Meta-Access-Control-Allow-Headers": "Range",
                "X-Container-Meta-Access-Control-Expose-Headers": "Accept-Ranges, Content-Length, Content-Range",
            },
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
            # Add progress feedback
            print(f"{f_info}{idx}/{n_objs}: Uploaded {f_path}{rel_path}{f_reset}")

        print(
            f"{f_success}Successfully uploaded {len(objects)} files to object storage{f_reset}"
        )


def mirror_catalog(argv: Sequence[str] | None = None) -> None:
    """CLI entry point for mirroring the intake catalog."""
    parser = argparse.ArgumentParser(
        description="Mirror the intake catalog to the datalake."
    )
    parser.add_argument(
        "--catalog-version",
        type=lambda d: date.fromisoformat(d),
        default=date.today(),
        help="The version date of the intake catalog to mirror (YYYY-MM-DD). Defaults to today's date.",
    )
    parser.add_argument(
        "--hidden",
        action="store_true",
        help="Whether to mirror a hidden version of the catalog (prefixed with a dot). Defaults to False.",
    )

    args = parser.parse_args(argv)

    mirror = CatalogMirror()
    mirror.run(catalog_version=args.catalog_version, hidden=args.hidden)
