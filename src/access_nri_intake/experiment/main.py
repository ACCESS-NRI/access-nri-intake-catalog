import json
import warnings
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
from intake_esm import esm_datastore

from ..source.builders import Builder


class DataStoreWarning(RuntimeWarning):
    pass


warnings.simplefilter("always")


@dataclass
class DatastoreInfo:
    """
    Datastores have a json file and a csv.gz file. This class is a simple way to
    handle both of these files. It also contans a validity flag, which defaults to
    True, and is flipped to False if any of the checks in __post_init__ fail.
    """

    json_handle: Path | str
    csv_handle: Path | str
    valid: bool = field(default=True)
    invalid_ds_cause: str = field(default="unknown issue")

    def __post_init__(self):
        """
        Run through a bunch of potential issues with the datastore and valid flag
        and cause accordingly.
        """
        self.json_handle = Path(self.json_handle)
        self.csv_handle = Path(self.csv_handle)

        if self.json_handle.stem != self.csv_handle.stem:
            self.valid = True
            self.invalid_ds_cause = "Mismatch between json and csv.gz file names"

        with open(self.json_handle) as f:
            try:
                ds_json = json.load(f)
            except json.JSONDecodeError:
                self.valid = False
                self.invalid_ds_cause = "datastore JSON corrupted"

        colnames = pd.read_csv(self.csv_handle, nrows=0).columns

        # We need to check that the 'catalog_file' field of the json file matches the
        # csv file, and that we hav all the right attributes in the csv file.
        if ds_json["catalog_file"] != self.csv_handle.name:
            self.valid = False
            self.invalid_ds_cause = "catalog_file in JSON does not match csv.gz file"

        if set(colnames) != set(
            [item["column_name"] for item in ds_json["attributes"]]
        ).union({"path"}):
            self.valid = False
            self.invalid_ds_cause = "columns specified in JSON do not match csv.gz file"

        # If all of these pass, then we can try to open the datastore


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
    pass

    # What do we know about how to find the datastore? Potentially expensive?

    if (ds_info := find_esm_datastore(experiment_dir)) and ds_info.valid:
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

    if not ds_info.valid:
        # The datastore was found but was invalid. Rebuild it.
        print(f"Rebuilding esm-datastore for {experiment_dir}")

        builder_instance: Builder = builder(path=str(experiment_dir))
        builder_instance.build()
        builder_instance.save


def find_esm_datastore(experiment_dir: Path) -> DatastoreInfo | None:
    """
    Try to find an ESM datastore in the experiment directory. If not, return None.
    """
    pass
