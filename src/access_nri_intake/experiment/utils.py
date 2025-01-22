import json
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd


class DataStoreWarning(RuntimeWarning):
    pass


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
        if not any(
            [self.json_handle, self.csv_handle, self.valid, self.invalid_ds_cause]
        ):
            # If we have an empty/false instance then just return None
            return None

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

    def __bool__(self):
        return not all(
            [
                self.json_handle == "",
                self.csv_handle == "",
                not self.valid,
                self.invalid_ds_cause == "",
            ]
        )
