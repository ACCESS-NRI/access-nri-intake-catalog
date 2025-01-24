import json
import re
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd


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
