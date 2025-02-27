import itertools
import warnings
from pathlib import Path

import intake
from intake_esm import esm_datastore

from ..source.builders import Builder
from .colours import f_info, f_path, f_reset, f_success, f_suggestion, f_warn
from .utils import (
    DatastoreInfo,
    DataStoreWarning,
    MultipleDataStoreError,
    find_experiment_files,
    hash_catalog,
    verify_ds_current,
)

warnings.simplefilter(
    "always"
)  # This will emit warnings from the command line - this is intentional


def use_datastore(
    experiment_dir: Path | str,
    builder: Builder | None = None,
    catalog_dir: Path | str | None = None,
    builder_kwargs: dict | None = None,
    open_ds: bool = True,
    datastore_name: str = "experiment_datastore",
    description: str | None = None,
) -> esm_datastore | None:
    """
    Specify a builder and an experiment directory in order to build and/or open
    an esm-datastore in place for that experiment. Valid and up to date datastores
    will not be overwritten.

    Further configuration can be done by passing additional keyword arguments

    Parameters
    ----------
    builder : Builder
        The builder object that will be used to build the datastore.
    experiment_dir : Path | str
        The directory containing the experiment. If a string is passed, it will be
        converted to a Path object.
    catalog_dir : Path | str, optional
        The directory containing/to write the catalog to, if it differs from the
        experiment directory. If None, the catalog will be written to the experiment
        directory. If a string is passed, it will be converted to a Path object.
    open_ds : bool
        Whether to open the datastore after building it. Typically set to false
        when called from a console script.
    builder_kwargs : dict, optional
        Any additional keyword arguments to pass to the builder if needed - for
        example, AccessEsm15Builder additionally takes an `ensemble` argument
    datastore_name : str, optional
        The name of the datastore to be written. Defaults to 'experiment_datastore'.
        Datastores are written as `catalog_dir / datastore_name.json`.
    description : str, optional
        A description of the datastore. If None, a default description will be used.

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
    builder_kwargs = builder_kwargs or {}

    catalog_dir, experiment_dir = (
        Path(catalog_dir).expanduser(),
        Path(experiment_dir).expanduser(),
    )

    catalog_dir_fmap = {
        ".": "current directory",
        "./": "current directory",
    }

    formatted_catdir_name = catalog_dir_fmap.get(str(catalog_dir), str(catalog_dir))

    ds_info = find_esm_datastore(catalog_dir, datastore_name)

    if ds_info.valid:  # Nothing is obviously wrong with the datastore
        print(
            f"{f_info}Datastore found in {f_path}{formatted_catdir_name}{f_info}, verifying datastore integrity...{f_reset}"
        )
        found_experiment_files = find_experiment_files(
            builder, experiment_dir, builder_kwargs
        )
        ds_info.valid = verify_ds_current(ds_info, found_experiment_files)
    elif ds_info:  # The datastore was found but was invalid. Rebuild it.
        warnings.warn(
            f"{f_warn}esm-datastore broken due to {ds_info.invalid_ds_cause}. Regenerating datastore...{f_reset}",
            category=DataStoreWarning,
            stacklevel=2,
        )
    else:  # No datastore found. Build one.
        print(f"{f_success}Generating esm-datastore for {experiment_dir}{f_reset}")

    scaffold_cmd = "scaffold_catalog_entry" if open_ds else "scaffold-catalog-entry"
    ds_full_path = str((catalog_dir / f"{datastore_name}.json").absolute())

    if not ds_info.valid and builder is None:
        raise ValueError(
            "A builder must be provided if no valid datastore is found in the experiment directory."
        )
    elif not ds_info.valid and builder is not None:
        builder_instance: Builder = builder(path=str(experiment_dir), **builder_kwargs)
        print(f"{f_info}Building esm-datastore...{f_reset}")
        builder_instance.get_assets().build()
        print(f"{f_success}Sucessfully built esm-datastore!{f_reset}")
        print(
            f"{f_info}Saving esm-datastore to {f_path}{str(catalog_dir.absolute())}{f_reset}"
        )
        builder_instance.save(
            name=datastore_name,
            description=description
            or f"esm_datastore for the model output in '{str(experiment_dir)}'",
            directory=str(catalog_dir),
        )

        print(
            f"{f_info}Hashing catalog to prevent unnecessary rebuilds.\nThis may take some time...{f_reset}"
        )
        hash_catalog(catalog_dir, datastore_name, builder_instance)
        print(f"{f_success}Catalog sucessfully hashed!{f_reset}")

        print(
            f"{f_success}Datastore sucessfully written to {f_path}{ds_full_path}{f_success}!"
            f"\n{f_info}Please note that this has not added the datastore to the access-nri-intake catalog."
            f"\nTo add to catalog, please run '{f_suggestion}{scaffold_cmd}{f_info}' for help on how to do so."
        )
    else:
        print(
            f"{f_success}Datastore found in {f_path}{ds_full_path}{f_success}!"
            f"\n{f_info}Please note that this has not added the datastore to the access-nri-intake catalog."
            f"\nTo add to catalog, please run '{f_suggestion}{scaffold_cmd}{f_info}' for help on how to do so."
        )

    if open_ds:
        return intake.open_esm_datastore(
            str(catalog_dir / f"{datastore_name}.json"),
            columns_with_iterables=["variable"],
        )
    else:
        print(
            f"{f_info}To open the datastore, run `{f_suggestion}intake.open_esm_datastore('{ds_full_path}',"
            f" columns_with_iterables=['variable']){f_info}` in a Python session."
        )

    print(f"{f_reset}")
    return None


def find_esm_datastore(experiment_dir: Path, datastore_name: str) -> DatastoreInfo:
    """
    Try to find an ESM datastore in the experiment directory, with the same name
    as the one we intend to build. If not, return a dummy DatastoreInfo object.

    To find an ESM datastore, we use the heuristic that an esm_datastore comprises
    a json file and a csv.gz (or.csv) file with the same name. To find these, we are
    first going to search experiment_dir and all its subdirectories for a json file,
    and then look for a file in the same directory where '.csv' is a member of the
    file objects suffixes property.

    Parameters
    ----------
    experiment_dir : Path
        The directory containing the experiment.
    datastore_name : str
        The name of the datastore to be found.

    Returns
    -------
    DatastoreInfo
        A DatastoreInfo object containing the json and csv files if found, or
        a null DatastoreInfo object if not found.
    """

    # If we don't realise iterators into memory, they will be consumed by inner
    # loops and we won't be able to iterate over them again - thus missing datastores.
    json_files = list(experiment_dir.rglob("*.json"))
    csv_files = list(
        itertools.chain(experiment_dir.rglob("*.csv"), experiment_dir.rglob("*.csv.gz"))
    )

    matched_pairs: list[tuple[Path, Path]] = []
    for json_file in json_files:
        for csv_file in csv_files:
            if (
                json_file.stem
                == csv_file.name.replace(
                    "".join([suffix for suffix in csv_file.suffixes]), ""
                )  # This gnarly statement removes the whole suffix to compare stems
                and json_file.parent == csv_file.parent
            ):
                matched_pairs.append((json_file, csv_file))

    # Remove any datastores that are not named the same as the one we are looking for
    matched_pairs = [pair for pair in matched_pairs if pair[0].stem == datastore_name]

    if len(matched_pairs) == 0:
        return DatastoreInfo("", "", False, "")
    elif len(matched_pairs) > 1:
        raise MultipleDataStoreError(
            f"Multiple datastores found in {experiment_dir}. Please remove duplicates."
        )

    return DatastoreInfo(*matched_pairs[0])


def scaffold_catalog_entry(interactive: bool) -> None:
    """
    Provides the user information about how to add a datastore to the access-nri-intake
    catalog. If interactive is set to True, the user will be prompted to confirm that they
    are ready to continue with each step - else, all steps will be printed at once.

    Parameters
    ----------
    interactive : bool
        Whether to provide interactive help or not.
    """

    modestr = "interactive" if interactive else "non-interactive"
    raise NotImplementedError(
        f"This function is not yet implemented for {modestr} mode."
    )
