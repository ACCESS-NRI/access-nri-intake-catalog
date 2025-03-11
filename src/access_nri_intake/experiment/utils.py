import ast
import json
import re
import warnings
from dataclasses import dataclass, field
from enum import Enum
from inspect import signature
from pathlib import Path
from typing import Any

import pandas as pd
from yamanifest.manifest import Manifest

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


@dataclass
class DatastoreInfo:
    """
    Dataclass to group json & csv file handles for a datastore, along with it's
    validity and any straightforwardly identifiable issues with the datastore.

    """

    # Datastores have a json file and a csv.gz file. This class is a simple way to
    # handle both of these files. It also contans a validity flag, which defaults to
    # True, and is flipped to False if any of the checks in __post_init__ fail.

    # It might be necessary and/or helpful to add a hash handle to this? I want
    # to create a hash of the experiment dir, and then check that if we hash the
    # dir we get the same

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

        colnames = pd.read_csv(self.csv_handle, nrows=0).columns

        # We need to check that the 'catalog_file' field of the json file matches the
        # csv file, and that we have all the right attributes in the csv file.

        if self.match_broken_internal_path(ds_json):
            self.valid = False
            self.invalid_ds_cause = DataStoreInvalidCause.PATH_MISMATCH.value
            return None

        # If the previous check passes, then we need to check that the name in
        # the catalog_file matches the name of the csv file. Someone might have
        # manually fiddled with it, so we'll convert it to a path object and check
        # the name attribute.
        if Path(ds_json["catalog_file"]).name != self.csv_handle.name:
            self.valid = False
            self.invalid_ds_cause = DataStoreInvalidCause.CATALOG_MISMATCH.value
            return None

        if set(colnames) != set(
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
        return not all(
            [
                self.json_handle == "",
                self.csv_handle == "",
                not self.valid,
                self.invalid_ds_cause == "",
            ]
        )

    def match_broken_internal_path(self, ds_json: dict) -> bool:
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
        return (
            (match := re.search(r"^file:///.+$", ds_json["catalog_file"]))
            and re.sub(r"^file://", "", match.group()) != str(csv_handle.absolute())
        ) or False


def verify_ds_current(
    ds_info: DatastoreInfo,
    experiment_files: set[Path],
) -> bool:
    """
    Verify if the datastore is current, testing for missing/extra files, and files
    that appear to have changed since the datastore was built.

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
            f"{f_warn}No hash file found for datastore. Datastore regeneration required...{f_reset}",
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
            f"{f_warn}Experiment directory and datastore do not match ({warn_str} datastore). Datastore regeneration required...{f_reset}",
            category=DataStoreWarning,
            stacklevel=2,
        )
        return False

    expdir_manifest = Manifest("_")
    expdir_manifest.add(experiment_files_str, hashfn="binhash")

    if not expdir_manifest.equals(mf):
        warnings.warn(
            f"{f_warn}Experiment directory and datastore do not match (differing hashes). Datastore regeneration required...{f_reset}",
            category=DataStoreWarning,
            stacklevel=2,
        )
        return False

    print(f"{f_success}Datastore integrity verified!{f_reset}")
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


def find_experiment_files(
    builder: Builder, experiment_dir: Path, builder_kwargs: dict | None = None
) -> set[Path]:
    """
    Find all the relevant files in the experiment directory and return them as a set, using
    the builder.get_assets() method.

    Parameters
    ----------
    builder : Builder
        The builder object that will be used to build the datastore.
    experiment_dir : Path
        The directory containing the experiment.
    builder_kwargs : dict, optional
        Any additional keyword arguments to pass to the builder

    Returns
    -------
    set[str]
        A set of the full paths of the files in the experiment directory.
    """
    builder_kwargs = builder_kwargs or {}

    print(f"{f_info}Parsing experiment dir...{f_reset}")

    builder_instance: Builder = builder(path=str(experiment_dir), **builder_kwargs)

    return {Path(file).resolve() for file in builder_instance.get_assets().assets}


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

    return None
