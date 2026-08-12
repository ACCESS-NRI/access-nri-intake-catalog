import ast
import json
import re
from dataclasses import dataclass, field
from enum import Enum
from inspect import signature
from pathlib import Path
from typing import Any

import pandas as pd
from intake_esm.core import esm_datastore
from pandas.testing import assert_frame_equal

from ..source.builders import Builder
from .colours import f_info, f_reset, f_success, f_warn


class DataStoreWarning(RuntimeWarning):
    pass


class DataStoreError(RuntimeError):
    pass


class MultipleDataStoreError(DataStoreError):
    pass


class DataStoreInvalidCause(str, Enum):
    """
    Enum to store the causes of invalid datastores.
    """

    NO_ISSUE = ""
    UNKNOWN_ISSUE = "unknown issue"
    MISMATCH_NAME = "mismatch between json and csv.gz file names"
    JSON_CORRUPTED = "datastore JSON corrupted"
    PATH_MISMATCH = "path in JSON does not match csv.gz"
    CATALOG_MISMATCH = "catalog_filename in JSON does not match csv.gz filename"
    COLUMN_MISMATCH = "columns specified in JSON do not match csv.gz file"


# CT Note: I'm not sure if we need this at all right now. I think we're just going
# to want to rebuild the datastoer and check the frames are equal.
@dataclass
class DatastoreInfo:
    """
    Dataclass to group json & csv file handles for a datastore, along with it's
    validity and any straightforwardly identifiable issues with the datastore.

    """

    # Datastores have a json file and a csv.gz file. This class is a simple way to
    # handle both of these files. It also contans a validity flag, which defaults to
    # True, and is flipped to False if any of the checks in __post_init__ fail.

    json_handle: Path | str
    csv_handle: Path | str
    valid: bool = field(default=True)
    invalid_ds_cause: str = field(default=DataStoreInvalidCause.NO_ISSUE.value)

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
            self.invalid_ds_cause = DataStoreInvalidCause.MISMATCH_NAME.value
            return None

        with open(self.json_handle) as f:
            try:
                ds_json = json.load(f)
            except json.JSONDecodeError:
                self.valid = False
                self.invalid_ds_cause = DataStoreInvalidCause.JSON_CORRUPTED.value
                return None

        if self.internal_path_broken(ds_json):
            self.valid = False
            self.invalid_ds_cause = DataStoreInvalidCause.PATH_MISMATCH.value
            return None

        # If the previous check passes, then we need to check that the name in
        # the catalog_file matches the name of the csv file. Someone might have
        # manually fiddled with it, so we'll convert it to a path object and check
        # the name attribute.
        if not Path(ds_json["catalog_file"]).name == self.csv_handle.name:
            self.valid = False
            self.invalid_ds_cause = DataStoreInvalidCause.CATALOG_MISMATCH.value
            return None

        colnames = pd.read_csv(self.csv_handle, nrows=0).columns

        if not set(colnames) == set(
            [item["column_name"] for item in ds_json["attributes"]]
        ).union({"path"}):
            self.valid = False
            self.invalid_ds_cause = DataStoreInvalidCause.COLUMN_MISMATCH.value
            return None

        # If all of these pass, then we can try to open the datastore
        self.invalid_ds_cause = ""

    def __bool__(self):
        """
        Define the truthiness of the DatastoreInfo object. If any of the fields are
        populated or the valid flag is True, then the object is considered True.

        This allows us to define a bottom value for the DatastoreInfo object.
        """
        return self.valid or any(
            (self.json_handle != "", self.csv_handle != "", self.invalid_ds_cause != "")
        )

    def internal_path_broken(self, ds_json: dict) -> bool:
        """
        If our internal reference starts with file:///, then we need to
        ensure that the rest of this *perfectly* matches the csv file or the
        datastore will break when we try to open it.

        The internal reference (on Gadi) typically starts with file:///path/filename.csv.gz
        What this means is that we might need to be careful if a datastore is moved.
        What intake_esm does is:
        - look at ds_json["catalog_file"] and check that this exists, using a fsspec
        get_mapper.
        - If it doesn't exist, then it prepends the dirname of fsspec.get_mapper().root
        to the path, which winds up creating a horrendously bundled path, something
        like '/home/189/ct1163/experiments_274/file:///home/189/ct1163/test_datastore_built_in_homedir.csv.gz

        - We need to be careful, because here the .name attribute of the Path object
        might still match, even though the handles are invalid

        Parameters
        ----------
        ds_json : dict
            The json object of the datastore.

        Returns
        -------
        bool
            Whether the internal path is broken.
        """
        csv_handle = Path(self.csv_handle)
        return bool(
            (match := re.search(r"^file://(?P<abs_path>/.+)$", ds_json["catalog_file"]))
            and match.group("abs_path") != str(csv_handle.absolute())
        )


def verify_ds_current(
    builder_dataframe: pd.DataFrame,
    existing_datastore: esm_datastore | None = None,
) -> bool:
    """
    Take the dataframe generated by the builder and compare it to the dataframe
    in the existing datastore, assuming it exists.

    If it doesn't exist, or the DataFrames are not equal, return False.
    If they are equal, return True.

    Parameters
    ----------
    builder_dataframe : pd.DataFrame
        The dataframe generated by the builder.

    existing_datastore : esm_datastore | None, optional
        The existing datastore to compare against, by default None.

    Returns
    -------
    bool
    """

    if existing_datastore is None:
        print(f"{f_warn}No existing datastore found, rebuilding...{f_reset}")
        return False

    try:
        assert_frame_equal(builder_dataframe, existing_datastore.df)
        print(f"{f_success}Datastore integrity verified!{f_reset}")
        return True
    except AssertionError:
        return False


def parse_kwarg(kwarg: str) -> tuple[str, Any]:
    """
    Builder kwargs can be passed as `--builder-kwargs arg1=val1 arg2=val2` etc.
    The argparse.parse_args() function will return these as a list of strings -
    eg ['arg1=val1', 'arg2=val2'].  This function parses one of these strings into
    a tuple, which is later converted to a dictionary.  It will require some
    additional type coercion to pass on non string kwargs.

    The builders we use only take either a path, list of paths, or an `ensemble`
    kwarg. Ensemble is a boolean.
    """
    kw, arg = kwarg.split("=")
    if kw == "ensemble":
        try:
            arg = ast.literal_eval(arg.capitalize())
            if not isinstance(arg, bool):
                raise ValueError
        except (ValueError, SyntaxError):
            raise TypeError(f"Ensemble kwarg must be a boolean, not {arg}.")

    # Do we use some sort of pattern matching in here to allow for passing through
    # other kwargs to the builder? This will have changed with #346

    return kw, arg


def validate_args(builder: Builder, builder_kwargs: dict[str, Any]) -> None:
    """
    Take a builder and validate the kwargs provided against the builder's signature.

    This is provided to smooth debugging when wrong kwargs are passed from the command
    line.

    Parameters
    ----------
    builder : Builder
        The builder object that will be used to build the datastore.

    builder_kwargs : dict[str, Any]
        The keyword arguments to pass to the builder.

    Returns
    -------
    None

    Raises
    ------
    TypeError
        If the builder_kwargs do not match the builder's signature.
    """

    builder_sig = signature(builder.__init__).parameters

    builder_params = {k: v for k, v in builder_sig.items() if k != "self"}

    for key, val in builder_kwargs.items():
        if key not in builder_params:
            raise TypeError(
                f"Builder does not accept kwarg {key}."
                f" Accepted kwargs are: {builder_params.keys()}"
            )
        param = builder_params[key]
        expected_type = param.annotation if param.annotation is not param.empty else Any
        if expected_type is not Any and not isinstance(val, expected_type):  # type: ignore
            # mypy does not like the isinstance check here. I've looked at the mypy
            # repo & there are a bunch of open issues regarding this sort of behaviour
            raise TypeError(
                f"Builder kwarg {key} must be of type {expected_type}, not {type(val)}."
            )
