import warnings
from pathlib import Path

import intake
from intake_esm import esm_datastore

from ..source.builders import Builder
from .utils import DatastoreInfo, DataStoreWarning

warnings.simplefilter("always")


def use_datastore(
    builder: Builder, experiment_dir: Path, open_ds: bool = True
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

    # What do we know about how to find the datastore? Potentially expensive?

    ds_info = find_esm_datastore(experiment_dir)

    if ds_info.valid:
        # Nothing is obviously wrong with the datastore, so
        print(f"Datastore found in {experiment_dir}, verifying datastore integrity...")
    elif ds_info:
        # The datastore was found but was invalid. Rebuild it.
        warnings.warn(
            f"esm_datastore broken due to {ds_info.invalid_ds_cause}. Regenerating datastore...",
            category=DataStoreWarning,
            stacklevel=2,
        )
    else:
        # No datastore found. Build one.
        print(f"Generating esm-datastore for {experiment_dir}")

    builder_instance: Builder = builder(path=str(experiment_dir))
    builder_instance.build()
    builder_instance.save

    scaffold_cmd = "scaffold_catalog_entry" if open_ds else "scaffold-catalog-entry"
    print(
        f"Datastore sucessfully written to {str(experiment_dir / 'catalog.json')}!"
        f" Please note that this has not added the datastore to the access-nri-intake catalog."
        f" To add to catalog, please run '{scaffold_cmd}' for help on how to do so."
    )

    if open_ds:
        return intake.open_esm_datastore(
            str(experiment_dir / "catalog.json"),
            columns_with_iterables=["variable"],
        )
    else:
        print(
            f"To open the datastore, run `intake.open_esm_datastore({str(experiment_dir / 'catalog.json')},"
            " columns_with_iterables=['variable'])` in a Python session."
        )
    return None


def find_esm_datastore(experiment_dir: Path) -> DatastoreInfo:
    """
    Try to find an ESM datastore in the experiment directory. If not, return a dummy
    DatastoreInfo object.
    """
    cant_be_found = False
    if cant_be_found:
        return DatastoreInfo("", "", False, "")

    return DatastoreInfo("dummy_json_handle", "dummy_csv_handle")
