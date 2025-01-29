import json
import re
import warnings
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
from colorama import Fore, Style
from yamanifest.manifest import Manifest

from ..source.builders import Builder


class DataStoreWarning(RuntimeWarning):
    pass


class DataStoreError(RuntimeError):
    pass


class MultipleDataStoreError(DataStoreError):
    pass


@dataclass
class DatastoreInfo:
    """
    Datastores have a json file and a csv.gz file. This class is a simple way to
    handle both of these files. It also contans a validity flag, which defaults to
    True, and is flipped to False if any of the checks in __post_init__ fail.


    It might be necessary to add a hash handle to this? I want to create a hash
    of the experiment dir, and then check that if we hash the dir we get the same
    hash.
    """

    json_handle: Path | str
    csv_handle: Path | str
    valid: bool = field(default=True)
    invalid_ds_cause: str = field(default="unknown issue")

    def __post_init__(self):
        """
        Run through a bunch of potential issues with the datastore and valid flag
        and cause accordingly.

        This fails at the first issue it finds. We should find a more comprehensive/elegant/faster
        way to deal with it, but that's a problem for another day.
        """
        if not any(
            [self.json_handle, self.csv_handle, self.valid, self.invalid_ds_cause]
        ):
            # If we have an empty/false instance then just return None
            return None

        self.json_handle = Path(self.json_handle)
        self.csv_handle = Path(self.csv_handle)

        if self.json_handle.stem != self.csv_handle.name.replace(
            "".join([suffix for suffix in self.csv_handle.suffixes]), ""
        ):  # This gnarly statement removes the whole suffix to compare stems
            # I think this might duplicate the check in find_esm_datastore
            self.valid = False
            self.invalid_ds_cause = "mismatch between json and csv.gz file names"
            return None

        with open(self.json_handle) as f:
            try:
                ds_json = json.load(f)
            except json.JSONDecodeError:
                self.valid = False
                self.invalid_ds_cause = "datastore JSON corrupted"
                return None

        colnames = pd.read_csv(self.csv_handle, nrows=0).columns

        # We need to check that the 'catalog_file' field of the json file matches the
        # csv file, and that we hav all the right attributes in the csv file.

        """
        The internal reference (on Gadi) typically starts with file:///path/filename.csv.gz
        What this means is that we might need to be careful if we are moving things about.
        What intake_esm does is:
        look at ds_json["catalog_file"] and check that this exists, using a fsspec
        get_mapper. If it doesn't exist, then it prepends the dirname of fsspec.get_mapper().root
        to the path, which winds up creating a horrendously bundled path, something
        like '/home/189/ct1163/experiments_274/file:///home/189/ct1163/test_datastore_built_in_homedir.csv.gz

        - The reason we need to be careful is that potentially the .name attribute of the Path object
        might still match, even though the handles are invalid

        We can match fo this pattern with the reget r'.+/file:///.+$
        """
        if (match := re.search(r"^file:///.+$", ds_json["catalog_file"])) and re.sub(
            r"^file://", "", match.group()
        ) != str(self.csv_handle.absolute()):
            # If our internal reference starts with /file:///, then we need to
            # ensure that the rest of this *perfectly* matches the csv file or the
            # datastore will break when we try to open it.
            self.valid = False
            self.invalid_ds_cause = "path in JSON does not match csv.gz"
            return None

        # If the previous check passes, then we need to check that the name in
        # the catalog_file matches the name of the csv file. Someone might have
        # manually fiddled with it, so we'll convert it to a path object and check
        # the name attribute.
        if Path(ds_json["catalog_file"]).name != self.csv_handle.name:
            self.valid = False
            self.invalid_ds_cause = (
                "catalog_filename in JSON does not match csv.gz filename"
            )
            return None

        if set(colnames) != set(
            [item["column_name"] for item in ds_json["attributes"]]
        ).union({"path"}):
            self.valid = False
            self.invalid_ds_cause = "columns specified in JSON do not match csv.gz file"
            return None

        # If all of these pass, then we can try to open the datastore

    def __bool__(self):
        return not all(
            [
                self.json_handle == "",
                self.csv_handle == "",
                not self.valid,
                self.invalid_ds_cause == "",
            ]
        )


def verify_ds_current(
    ds_info: DatastoreInfo,
    experiment_files: set[Path],
) -> bool:
    """
    Check that the datastore is current - do we have assets in our directory that
    are not in the datastore? Do we have assets in the datastore that are not in
    our directory? Are the assets in the datastore the same as the assets in our
    directory? If any of these are true, then we need to rebuild the datastore.



    Parameters
    ----------
    ds_info : DatastoreInfo
        The datastore information object.
    experiment_files : set[Path]
        The set of files found in the experiment directory. These are typically going
        to be generated by the find_experiment_files function.

    Returns
    -------
    bool
        Whether the datastore is valid and up to date.

    """

    hashfile = (
        Path(ds_info.json_handle).parent / f".{Path(ds_info.json_handle).stem}.hash"
    )

    if not hashfile.exists():
        warnings.warn(
            f"{Fore.YELLOW}No hash file found for datastore. Regenerating datastore...{Style.RESET_ALL}",
            category=DataStoreWarning,
            stacklevel=2,
        )
        return False

    mf = Manifest(str(hashfile)).load()
    manifest_files = {v.get("fullpath") for v in mf.data.values()}

    # Convert experiment files to strings for compatibility with yamanifest
    experiment_files_str = {str(file) for file in experiment_files}

    if experiment_files_str != manifest_files:
        warn_str = (
            "extra files in"
            if len(experiment_files_str) < len(manifest_files)
            else "missing files from"
        )
        warnings.warn(
            f"{Fore.YELLOW}Experiment directory and datastore do not match ({warn_str} datastore). Regenerating datastore...{Style.RESET_ALL}",
            category=DataStoreWarning,
            stacklevel=2,
        )
        return False

    expdir_manifest = Manifest("_")
    expdir_manifest.add(experiment_files_str, hashfn="binhash")

    if not expdir_manifest.equals(mf):
        warnings.warn(
            f"{Fore.YELLOW}Experiment directory and datastore do not match (differing hashes). Regenerating datastore...{Style.RESET_ALL}",
            category=DataStoreWarning,
            stacklevel=2,
        )
        return False

    print(f"{Fore.GREEN}Datastore integrity verified!{Style.RESET_ALL}")
    return True


def hash_catalog(
    catalog_dir: Path, datastore_name: str, builder_instance: Builder
) -> None:
    """
    Use yamanifest to hash the files contained in the builder, and then stick that in a
    .$datastore_name.hash file in the catalog_dir. This will be used to check if the datastore
    is current.
    """
    cat_files = builder_instance.df.path.tolist()
    cat_fullfiles = [str(Path(file).resolve()) for file in cat_files]

    mf = Manifest(str(catalog_dir / f".{datastore_name}.hash"))

    mf.add(cat_fullfiles, hashfn="binhash")

    mf.dump()
    return None


def find_experiment_files(builder: Builder, experiment_dir: Path) -> set[Path]:
    """
    Find all the files in the experiment directory and return them as a set.

    Parameters
    ----------
    builder : Builder
        The builder object that will be used to build the datastore.
    experiment_dir : Path
        The directory containing the experiment.

    Returns
    -------
    set[str]
        A set of the full paths of the files in the experiment directory.
    """

    print(f"{Fore.BLUE}Parsing experiment dir...{Style.RESET_ALL}")

    builder_instance: Builder = builder(path=str(experiment_dir))

    return {Path(file).resolve() for file in builder_instance.get_assets().assets}
