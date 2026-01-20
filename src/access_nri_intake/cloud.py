from datetime import date
from pathlib import Path

import polars as pl
import swiftclient
from django.core.management.base import BaseCommand, CommandError
from fabric import Connection


class Command(BaseCommand):
    help = "Mirror the intake catalog to the datalake."

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = "access-nri-intake-catalog"
        self.json_files = []
        self.local_mirror_path = Path("~/scratch/intake-mirror/").expanduser()
        self.metacat_path = self.local_mirror_path / "metacatalog.parquet"

    def add_arguments(self, parser):
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

    def handle(self, *args, **kwargs):
        catalog_version = kwargs.get("catalog_version", date.today())
        hidden = kwargs.get("hidden", False)

        try:
            self.mirror_intake_catalog(catalog_version=catalog_version, hidden=hidden)
            self.stdout.write(
                self.style.SUCCESS("Successfully mirrored intake catalog.")
            )
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error mirroring intake catalog: {e}"))
            self.stdout.write(self.style.WARNING("\nUsage examples:"))
            self.stdout.write("  python manage.py mirror_catalog")
            self.stdout.write("  python manage.py mirror_catalog --version 2025-11-01")
            self.stdout.write("  python manage.py mirror_catalog --hidden")
            self.stdout.write(
                "  python manage.py mirror_catalog --version 2025-11-01 --hidden"
            )
            raise CommandError(f"Command failed: {e}")

        self.stdout.write(
            self.style.MIGRATE_HEADING("Restructuring metacatalog parquet file...")
        )
        self.restructure_metacat()
        self.stdout.write(self.style.SUCCESS("Metacatalog restructured successfully."))
        self.stdout.write(
            self.style.MIGRATE_HEADING(
                "Updating ESM datastore parquet file path fields..."
            )
        )
        self.update_esm_datastores()

        self.stdout.write(
            self.style.SUCCESS("ESM datastore parquet files updated successfully.")
        )
        self.stdout.write(
            self.style.MIGRATE_HEADING("Writing mirrored catalog to object storage...")
        )
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

        basedir = Path("/g/data/xp65/public/apps/access-nri-intake-catalog/")

        dotstr = "." if hidden else ""
        version_dir = f"{dotstr}v{catalog_version.isoformat()}"

        remote_path = basedir / version_dir
        source_dir = basedir / version_dir / "source"

        self.stdout.write(
            self.style.HTTP_INFO(
                f"Mirroring intake catalog from {remote_path} to {self.local_mirror_path}"
            )
        )

        metacat_file = basedir / version_dir / "metacatalog.parquet"

        self.stdout.write(
            self.style.HTTP_INFO(
                f"Downloading metacatalog file: {metacat_file} to {self.local_mirror_path}"
            )
        )

        conn.get(
            metacat_file,
            local=f"{str(self.local_mirror_path)}/",
            preserve_mode=False,
        )

        self.stdout.write(
            self.style.SUCCESS("Metacatalog file transferred sucessfully!")
        )

        sftp = conn.sftp()

        print(f"sftp initiated: listing {source_dir} contents")
        sourcedir_contents = sftp.listdir(str(source_dir))

        pq_files = [f for f in sourcedir_contents if Path(f).suffix == ".parquet"]
        json_files = [f for f in sourcedir_contents if Path(f).suffix == ".json"]

        if len(pq_files) != len(json_files):
            raise ValueError(
                "Mismatch between number of parquet and json files in source directory."
            )

        print(f"Found {len(pq_files)} esm-datastores in source directory.")

        Path(self.local_mirror_path / "source").mkdir(parents=True, exist_ok=True)

        for idx, pq_file in enumerate(pq_files):
            remote_file_path = source_dir / pq_file
            local_file_path = self.local_mirror_path / "source" / Path(pq_file).name

            self.stdout.write(
                self.style.HTTP_INFO(
                    f"Transferring file {idx + 1}/{2 * len(pq_files)}: {remote_file_path.name}"
                )
            )
            conn.get(
                str(remote_file_path),
                local=str(local_file_path),
                preserve_mode=False,
            )

        self.stdout.write(
            self.style.SUCCESS("All parquet files transferred successfully!")
        )

        for idx, json_file in enumerate(json_files):
            remote_file_path = source_dir / json_file
            local_file_path = self.local_mirror_path / "source" / Path(json_file).name

            self.stdout.write(
                self.style.HTTP_INFO(
                    f"Transferring file {idx + len(json_files) + 1}/{2 * len(json_files)}: {remote_file_path.name}"
                )
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
        point to the one next door to it.
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
        The credentials below can be obtained by doing the following:

        1. Start a bash shell
        2. run `source `~/access-nri-store-openrc.sh`
        3. run `swift auth
        4. Copy the OS_STORAGE_URL and OS_AUTH_TOKEN values from the output

        Absolute mess of a process & will need to be fixed into something functional!

        N.B I have tried following the docs here
        https://tutorials.rc.nectar.org.au/object-storage/03-object-storage-cli
        and gotten nowhere.
        """
        import openstack

        cloud = openstack.connect(cloud="openstack")

        # Get storage URL and token from the session
        auth = cloud.session.get_auth_headers()
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
            self.stdout.write(
                self.style.HTTP_INFO(f"{idx}/{n_objs}: Uploaded {rel_path}")
            )

        self.stdout.write(
            self.style.SUCCESS(
                f"Successfully uploaded {len(objects)} files to object storage"
            )
        )
