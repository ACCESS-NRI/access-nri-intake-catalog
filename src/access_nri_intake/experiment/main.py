import itertools
import warnings
from pathlib import Path

import intake
from colorama import Fore, Style
from intake_esm import esm_datastore
from yamanifest.manifest import Manifest

from ..source.builders import Builder
from .utils import DatastoreInfo, DataStoreWarning, MultipleDataStoreError

warnings.simplefilter("always")


def use_datastore(
    builder: Builder,
    experiment_dir: Path,
    catalog_dir: Path | None = None,
    open_ds: bool = True,
    datastore_name: str = "experiment_datastore",
    description: str | None = None,
) -> esm_datastore | None:
    """
    Handles building datastores for experiments for experiments contained in a
    directory, as well as verifying that the datastores are valid and rebuilding
    them if needs be. If `open_ds` is False, then we won't try to open the datastore
    after building it - it'll just slow things down if we're using the console.

    Procedure:
    1. Look for a catalog.json and a matching catalog.csv.gz file in the directory
    provided. TBC: How robust can we make this? Does it need to be in the root of
    the experiment dir? These things all need to be worked out.
    2. If we find a catalog there, then we need to check that it is valid. First,
    we need to just try to open the catalog. If that doesn't work, we need to rebuild
    it, for sure. But we also need to check that the catalog has the right number of
    entries, and that the entries are all valid. This is a bit more tricky, but we
    use just enumerate the entries and check they're all in the directory, and then
    enumerate the files in the directory and check they're all in the catalog. If
    we find any discrepancies, we need to rebuild the catalog. This is kind of involved,
    so we are going to try to use some hashing tricks to speed things up.

    Parameters
    ----------
    builder : Builder
        The builder object that will be used to build the datastore.
    experiment_dir : Path
        The directory containing the experiment.
    catalog_dir : Path, optional
        The directory containing/to write the catalog to, if it differs from the
        experiment directory. If None, the catalog will be written to the experiment
        directory.
    open_ds : bool
        Whether to open the datastore after building it.

    Returns
    -------
    esm_datastore | None
        The datastore object, if it was requested to be opened. Otherwise, None.

    Raises
    ------
    TBC

    """
    description = (
        description or f"esm_datastore for the model output in '{str(experiment_dir)}'"
    )
    catalog_dir = catalog_dir or experiment_dir

    ds_info = find_esm_datastore(catalog_dir)

    if ds_info.valid:
        # Nothing is obviously wrong with the datastore, so
        print(
            f"{Fore.BLUE}Datastore found in {Style.BRIGHT}{experiment_dir}{Style.NORMAL}, verifying datastore integrity...{Style.RESET_ALL}"
        )
        ds_info.valid = verify_ds_current(ds_info, builder, experiment_dir, catalog_dir)
    elif ds_info:
        # The datastore was found but was invalid. Rebuild it.
        warnings.warn(
            f"{Fore.YELLOW}esm_datastore broken due to {ds_info.invalid_ds_cause}. Regenerating datastore...{Style.RESET_ALL}",
            category=DataStoreWarning,
            stacklevel=2,
        )
    else:
        # No datastore found. Build one.
        print(
            f"{Fore.GREEN}Generating esm-datastore for {experiment_dir}{Style.RESET_ALL}"
        )

    scaffold_cmd = "scaffold_catalog_entry" if open_ds else "scaffold-catalog-entry"
    ds_full_path = str((catalog_dir / f"{datastore_name}.json").absolute())

    if not ds_info.valid:
        builder_instance: Builder = builder(path=str(experiment_dir))
        print(f"{Fore.BLUE}Building esm-datastore...{Style.RESET_ALL}")
        builder_instance.build()
        print(f"{Fore.GREEN}Sucessfully built esm-datastore!{Style.RESET_ALL}")
        print(
            f"{Fore.BLUE}Saving esm-datastore to {Fore.CYAN}{Style.BRIGHT}{str(catalog_dir.absolute())}{Style.RESET_ALL}"
        )
        builder_instance.save(
            name=datastore_name,
            description=description
            or f"esm_datastore for the model output in '{str(experiment_dir)}'",
            directory=str(catalog_dir),
        )

        print(
            f"{Fore.BLUE}Hashing catalog to prevent unnecessary rebuilds.\nThis may take some time...{Style.RESET_ALL}"
        )
        hash_catalog(catalog_dir, datastore_name, builder_instance)
        print(f"{Fore.GREEN}Catalog sucessfully hashed!{Style.RESET_ALL}")

        print(
            f"{Fore.GREEN}Datastore sucessfully written to {Fore.CYAN}{Style.BRIGHT}{ds_full_path}{Style.NORMAL}{Fore.GREEN}!"
            f"\n{Fore.BLUE}Please note that this has not added the datastore to the access-nri-intake catalog."
            f"\nTo add to catalog, please run '{Fore.WHITE}{Style.BRIGHT}{scaffold_cmd}{Fore.BLUE}{Style.NORMAL}' for help on how to do so."
        )
    else:
        print(
            f"{Fore.GREEN}Datastore found in {Fore.CYAN}{Style.BRIGHT}{ds_full_path}{Style.NORMAL}{Fore.GREEN}!"
            f"\n{Fore.BLUE}Please note that this has not added the datastore to the access-nri-intake catalog."
            f"\nTo add to catalog, please run '{Fore.WHITE}{Style.BRIGHT}{scaffold_cmd}{Fore.BLUE}{Style.NORMAL}' for help on how to do so."
        )

    if open_ds:
        return intake.open_esm_datastore(
            str(catalog_dir / f"{datastore_name}.json"),
            columns_with_iterables=["variable"],
        )
    else:
        print(
            f"{Fore.BLUE}To open the datastore, run `{Fore.WHITE}{Style.BRIGHT}intake.open_esm_datastore('{ds_full_path}',"
            f" columns_with_iterables=['variable']){Fore.BLUE}{Style.NORMAL}` in a Python session."
        )

    print(f"{Style.RESET_ALL}")
    return None


def find_esm_datastore(experiment_dir: Path) -> DatastoreInfo:
    """
    Try to find an ESM datastore in the experiment directory. If not, return a dummy
    DatastoreInfo object.

    To find an ESM datastore, we use the heuristic that an esm_datastore comprises
    a json file and a csv.gz file with the same name. To find these, we are first
    going to search experiment_dir and all its subdirectories for a json file, and
    then look for a file in the same directory where '.csv' is a member of the file
    objects suffixes property.
    """

    json_files = experiment_dir.rglob("*.json")
    csv_files = itertools.chain(
        experiment_dir.rglob("*.csv"), experiment_dir.rglob("*.csv.gz")
    )

    matched_pairs: list[tuple[Path, Path]] = []
    for json_file in json_files:
        for csv_file in csv_files:
            if (
                json_file.stem
                == csv_file.name.replace(
                    "".join([suffix for suffix in csv_file.suffixes]), ""
                )  # THis gnarly statement removes the whole suffix to compaer stems
                and json_file.parent == csv_file.parent
            ):
                matched_pairs.append((json_file, csv_file))

    if len(matched_pairs) == 0:
        return DatastoreInfo("", "", False, "")
    elif len(matched_pairs) > 1:
        raise MultipleDataStoreError(
            f"Multiple datastores found in {experiment_dir}. Please remove duplicates."
        )

    return DatastoreInfo(*matched_pairs[0])


def verify_ds_current(
    ds_info: DatastoreInfo, builder: Builder, experiment_dir: Path, catalog_dir: Path
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

    Returns
    -------
    bool
        Whether the datastore is valid.

    """
    builder_instance: Builder = builder(path=str(experiment_dir))
    print(f"{Fore.BLUE}Parsing experiment dir...{Style.RESET_ALL}")

    # Might be able to just run get_assets() here?
    builder_instance.get_assets().parse()

    experiment_files = set(builder_instance.df.path.unique())

    hashfile = catalog_dir / f".{Path(ds_info.json_handle).stem}.hash"

    if not hashfile.exists():
        warnings.warn(
            f"{Fore.YELLOW}No hash file found for datastore. Regenerating datastore...{Style.RESET_ALL}",
            category=DataStoreWarning,
            stacklevel=2,
        )
        return False

    mf = Manifest(str(hashfile)).load()
    manifest_files = {v.get("fullpath") for v in mf.data.values()}

    if experiment_files != manifest_files:
        warnings.warn(
            f"{Fore.YELLOW}Experiment directory and datastore do not match. Regenerating datastore...{Style.RESET_ALL}",
            category=DataStoreWarning,
            stacklevel=2,
        )
        return False

    warnings.warn(
        "*** I haven't checked the hashes! Otherwise looks good bro ***",
        category=DataStoreWarning,
    )
    return True


def hash_catalog(
    catalog_dir: Path, datastore_name: str, builder_instance: Builder
) -> None:
    """
    Use yamanifest to hash the files contained in the builder, and then stick that in a
    .$datastore_name.hash file in the catalog_dir. This will be used to check if the datastore
    is current.
    """

    mf = Manifest(str(catalog_dir / f".{datastore_name}.hash"))

    mf.add(builder_instance.df.path.tolist(), hashfn="binhash")

    mf.dump()
    return None
