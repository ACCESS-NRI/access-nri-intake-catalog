# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import argparse
import sys
from collections.abc import Sequence
from datetime import date
from pathlib import Path

import openstack
import polars as pl
import swiftclient
from fabric import Connection

from access_nri_intake.experiment.colours import (
    f_info,
    f_path,
    f_reset,
    f_success,
    f_warn,
)


class CatalogMirror:
    """Mirror the intake catalog to the datalake.

    TODO:
    - [ ] Can we do this asynchronously without upsetting NCI
    - [ ] Logging
    - [ ] Fault tolerant transfer/ resume/ yadda yadda
    """

    def __init__(self):
        self.bucket_name = "access-nri-intake-catalog"
        self.json_files = []
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
                preserve_mode=False,
            )
            self.json_files.append(local_file_path)

    def restructure_metacat(self):
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

    def update_esm_datastores(self):
        """
        We need to go into each of the esm-datastore parquet files and make a few
        changes. Most important, we need to change the `catalog_file` field to
        point to the one next door to it. We'll also make sure all parquet files
        are snappy-compressed.
        """

        for file in self.json_files:
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
