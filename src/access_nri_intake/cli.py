# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Command line interfaces for access-nri-intake"""

import argparse
import datetime
import logging
import re
import shutil
import traceback
import warnings
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Literal

import jsonschema
import polars as pl
import yaml
from intake import open_esm_datastore

from .catalog import EXP_JSONSCHEMA, translators
from .catalog.manager import CatalogManager
from .data import CATALOG_NAME_FORMAT
from .experiment import use_datastore
from .experiment.colours import f_info, f_path, f_reset
from .experiment.main import scaffold_catalog_entry as _scaffold_catalog_entry
from .experiment.utils import parse_kwarg, validate_args
from .source import builders
from .utils import _can_be_array, get_catalog_fp, load_metadata_yaml

STORAGE_FLAG_PATTERN = "gdata/[a-z]{1,2}[0-9]{1,2}"

T_catname = Literal["access_nri", "access_nri_pq"]


class MetadataCheckError(Exception):
    pass


class DirectoryExistsError(Exception):
    """
    Raised when a directory already exists and the user has not specified
    the --force flag to overwrite it.
    """

    pass


class VersionHandler:
    CSV_AND_PQ_SOURCES = {"access_nri", "access_nri_pq"}

    def __init__(
        self,
        yaml_dict: dict,
        catalog_base_path: str | Path,
        build_base_path: Path,
        version: str,
        use_parquet: bool = False,
    ):
        self.yaml_dict = yaml_dict
        self.catalog_base_path = catalog_base_path
        self.build_base_path = build_base_path
        self.version = version
        self.use_parquet = use_parquet

        self._existing_cat: bool | None = None

    @property
    def cat_name(self) -> T_catname:
        return "access_nri" if not self.use_parquet else "access_nri_pq"

    @property
    def alt_name(self) -> T_catname:
        return "access_nri" if self.cat_name == "access_nri_pq" else "access_nri_pq"

    @property
    def cat_loc(self) -> Path:
        return Path(get_catalog_fp(basepath=self.catalog_base_path))

    @property
    def existing_cat(self) -> bool:
        """
        Memo-ised property to check for existing catalog file. If we don't memoize,
        then we run into mutation issues - because __call__ will write a new catalog
        partway through its execution, invaliding the checks.
        """
        if self._existing_cat is None:
            self._existing_cat = Path(self.cat_loc).exists()
        return self._existing_cat

    @property
    def yaml_old(self) -> dict | None:
        if self.existing_cat:
            with Path(self.cat_loc).open(mode="r") as fobj:
                return yaml.safe_load(fobj)
        return None

    def __call__(self) -> dict[str, Any]:
        """
        Dispatch to the version computation method.
        """
        return self._compute_previous_versions()

    def _compute_previous_versions(  # noqa: PLR0912
        self,
    ) -> dict[str, Any]:
        """Calculate previous version information for a new catalog build.

        Returns
        -------
        dict
            An updated YAML dict describing the new catalog, including current/min/max version.

        Notes
        -----
        The logic for determining the min/max catalog version is as follows:
        - If there are no existing catalogs, then min=max=current.
        - If there are existing catalogs, the following happens:
        - If the `args` or `driver` parts of the catalog YAML are changing in the
            new version, the versions are incompatible. The existing catalog.yaml
            will be moved aside to a new filename, labelled with its min and max
            version numbers. (An exception to this rule is legacy catalogs without
            a min or max version will have their storage flags retained.)
        - If existing catalogs are otherwise compatible with the new catalog, their
            min and max versions will be incorporated in with the new catalog and the
            existing catalog.yaml will be overwritten.
        - If we have switched from csv to parquet or vice versa, the existing catalog
            will be retained as-is, and the new catalog will be added alongside it.
        - Whether we update `access_nri` or `access_nri_pq` is determined by the
        `use_parquet` flag. The other catalog will be retained as-is.
        TODO: Storage flag combination probably needs updating, but implement in a
        separate PR to manage complexity.

        """

        if self.existing_cat:
            vmin_old, vmax_old = self._compute_prev_existing()

        if not self.existing_cat:
            self.set_versions_no_existing_cat()
            return self.yaml_dict

        if vmin_old is None and vmax_old is None:
            self.set_versions_no_prev_vmin_vmax()

        return self.yaml_dict

    def set_versions_no_existing_cat(self) -> None:
        """
        No existing catalog, so set min = max = current version,
        unless there are folders with the right names in the write
        directory
        """

        def _multiple_existing_versions() -> bool:
            """Only here for readability in conditionals below."""
            return len(existing_vers) > 1

        existing_vers = [
            v.name.lstrip(".")
            for v in self.build_base_path.iterdir()
            if re.match(CATALOG_NAME_FORMAT, v.name)
        ]

        if _multiple_existing_versions():
            self.yaml_dict = self._set_catalog_yaml_version_bounds(
                self.yaml_dict,
                min(*existing_vers, self.version),
                max(*existing_vers, self.version),
            )
        else:
            self.yaml_dict = self._set_catalog_yaml_version_bounds(
                self.yaml_dict, self.version, self.version
            )

    def set_versions_no_prev_vmin_vmax(self) -> dict[str, Any] | None:
        """
        Still not really sure what exactly this should be called.
        """

        def _multiple_existing_versions() -> bool:
            """Only here for readability in conditional below."""
            return len(existing_vers) > 1

        if set(self.yaml_dict["sources"].keys()) == self.CSV_AND_PQ_SOURCES:
            # First parquet catalog - don't update versions
            self.yaml_dict = self._set_catalog_yaml_version_bounds(
                self.yaml_dict, self.version, self.version
            )
            return self.yaml_dict

        existing_vers = [
            v.name.lstrip(".")
            for v in self.build_base_path.iterdir()
            if re.match(CATALOG_NAME_FORMAT, v.name)
        ]

        if _multiple_existing_versions():
            self.yaml_dict = self._set_catalog_yaml_version_bounds(
                self.yaml_dict,
                min(*existing_vers, self.version),
                max(*existing_vers, self.version),
            )

        return None

    def _pass_through_alt_catalog(self, yaml_old: dict) -> tuple[None, None]:
        """
        If we are computing previous versions, but we don't have a previous version
        for our current source, then we need to pass through the alternate catalog,
        by mutating yaml_old, and then return our previous versions - which are
        going to be `(None, None)`.

        We also call this to pass through the alternate catalog when we have an
        existing catalog for our source to update, but don't need to do anything
        with the version numbers there.
        """
        self.yaml_dict["sources"][self.alt_name] = yaml_old["sources"][self.alt_name]

        vmin_old, vmax_old = None, None
        return vmin_old, vmax_old

    def _compute_prev_existing(self) -> tuple[str | None, str | None]:
        """
        Handle the branch where we have an existing catalog.
        """
        yaml_old = self.yaml_old

        if yaml_old is None:  # Here to keep mypy happy - we can't test this easily
            raise RuntimeError(  # pragma: no cover
                "Invalid state encountered: `yaml_old` is None when `existing_cat` is True."
            )

        # Check to see what has changed. We care if the following keys
        # have changed (ignoring the sources.access_nri at the head
        # of each dict path):
        # - args (all parts - mode should never change)
        # - driver
        if not yaml_old["sources"].get(self.cat_name):
            return self._pass_through_alt_catalog(yaml_old)

        if set(yaml_old["sources"].keys()) == self.CSV_AND_PQ_SOURCES:
            self._pass_through_alt_catalog(yaml_old)

        fragment_new = self.yaml_dict["sources"][self.cat_name]
        fragment_old = yaml_old["sources"][self.cat_name]

        args_new, args_old = (fragment_new["args"], fragment_old["args"])
        driver_new, driver_old = (fragment_new["driver"], fragment_old["driver"])

        vmin_old, vmax_old = (
            fragment_old["parameters"]["version"].get("min", None),
            fragment_old["parameters"]["version"].get("max", None),
        )  # Nones are redundant here but help with readability

        storage_new, storage_old = (
            fragment_new["metadata"]["storage"],
            fragment_old["metadata"]["storage"],
        )

        _changed_args = args_new != args_old
        _changed_driver = driver_new != driver_old
        _changed_storage = storage_new != storage_old

        _previous_vlims = vmin_old is not None and vmax_old is not None

        if (_changed_args or _changed_driver) and _previous_vlims:
            # Move the old catalog out of the way
            # New catalog.yaml will have restricted version bounds
            vers_str = vmin_old if vmin_old == vmax_old else f"{vmin_old}-{vmax_old}"

            Path(self.cat_loc).rename(
                Path(self.cat_loc).parent / f"catalog-{vers_str}.yaml"
            )
            self.yaml_dict = self._set_catalog_yaml_version_bounds(
                self.yaml_dict, self.version, self.version
            )

        elif _changed_storage:
            self.yaml_dict["sources"][self.cat_name]["metadata"]["storage"] = (
                _combine_storage_flags(storage_new, storage_old)
            )

        if fragment_new["parameters"]["version"].get("min", None) is not None:
            return vmin_old, vmax_old

        # Set the minimum and maximum catalog versions, if they're not set already
        # in the 'new catalog' if statement above
        self.yaml_dict = self._set_catalog_yaml_version_bounds(
            self.yaml_dict,
            min(self.version, vmin_old or self.version),
            max(self.version, vmax_old or self.version),
        )
        return vmin_old, vmax_old

    def _set_catalog_yaml_version_bounds(self, d: dict, bl: str, bu: str) -> dict:
        """
        Set the version boundaries for the access_nri_intake_catalog.
        """

        d["sources"][self.cat_name]["parameters"]["version"]["min"] = bl
        d["sources"][self.cat_name]["parameters"]["version"]["max"] = bu

        return d


def _parse_build_inputs(
    config_yamls: list[str | Path], build_path: str | Path, data_base_path: str | Path
) -> list[tuple[str, dict]]:
    """
    Parse build inputs into a list of tuples of CatalogManager methods and args to
    pass to the methods
    """

    args = []
    for config_yaml in config_yamls:
        with open(config_yaml) as f:
            config = yaml.safe_load(f)

        builder = config.get("builder")
        translator = config.get("translator")
        sources = config.get("sources")

        config_args = {}
        if builder:
            method = "build_esm"
            config_args["builder"] = getattr(builders, builder)
            config_args["directory"] = str(build_path)
            config_args["overwrite"] = True
        else:
            method = "load"
            config_args["directory"] = str(build_path)

        for kwargs in sources:
            source_args = config_args

            source_args["path"] = [
                str(Path(data_base_path) / _) for _ in kwargs.pop("path")
            ]

            try:
                metadata_yaml = kwargs.pop("metadata_yaml")
            except KeyError:
                raise KeyError(
                    f"Could not find metadata_yaml kwarg for {config_yaml} - keys are {kwargs}"
                )

            try:
                metadata = load_metadata_yaml(
                    Path(data_base_path) / metadata_yaml, EXP_JSONSCHEMA
                )
            except jsonschema.exceptions.ValidationError:
                warnings.warn(
                    rf"Failed to validate metadata.yaml @ {Path(metadata_yaml).parent}. See traceback for details: {traceback.format_exc()}"
                )
                continue  # Skip the experiment w/ bad metadata

            source_args["name"] = metadata["name"]
            source_args["description"] = metadata["description"]
            source_args["metadata"] = metadata

            if translator:
                source_args["translator"] = getattr(translators, translator)

            args.append((method, source_args | kwargs))

    return args


def _check_build_args(args_list: list[dict]) -> None:
    """
    Run some checks on the parsed build argmuents to be passed to the CatalogManager

    Raises:
        MetadataCheckError: If there are experiments with the same name or experiment_uuid
    """

    names = []
    uuids = []
    for args in args_list:
        names.append(args["name"])
        uuids.append(args["metadata"]["experiment_uuid"])

    if len(names) != len(set(names)):
        seen = set()
        dupes = [name for name in names if name in seen or seen.add(name)]  # type: ignore
        raise MetadataCheckError(f"There are experiments with the same name: {dupes}")
    if len(uuids) != len(set(uuids)):
        seen = set()
        dupes = [uuid for uuid in uuids if uuid in seen or seen.add(uuid)]  # type: ignore
        dupes = [name for name, uuid in zip(names, uuids) if uuid in dupes]
        raise MetadataCheckError(
            f"There are experiments with the same experiment_uuid: {dupes}"
        )


def _add_source_to_catalog(
    cm: CatalogManager,
    method: str,
    src_args: dict,
    metacatalog_path: str | Path,
    logger: logging.Logger | None,
):
    """
    Add an experiment to the catalog.
    """
    if logger is not None:
        logger.info(f"Adding '{src_args['name']}' to metacatalog '{metacatalog_path}'")
    try:
        getattr(cm, method)(**src_args)
    except Exception as e:  # actually valid for once - it may raise naked Exceptions
        warnings.warn(
            f"Unable to add {src_args['name']} to catalog - continuing. Error: {str(e)}\n{traceback.format_exc()}"
        )


def _parse_build_directory(
    build_base_path: str | Path, version: str, catalog_file: str
) -> tuple[Path, Path, Path]:
    """
    Build the location for the new catalog. We put everything in temporary directory
    for now, which will basically be a map `$DIR/$FNAME` => `$DIR/.$FNAME` where
    `$DIR` is the base directory and `$FNAME` is the catalog file name that the user
    specified. We can't stick things in /scratch if we want our operations to be atomic.

    At the end of the build process, `$DIR/.$FNAME` will be moved to
    `$DIR/$FNAME` and all relevant filepaths contained within it altered to point
    to the right location.

    Parameters
    ----------
    base_build_path : str | Path
        Base path for catalog directories.
    version : str
        New catalog version
    catalog_file : str
        Catalog file name
    """
    build_base_path = Path(build_base_path).absolute()
    build_path = Path(build_base_path) / f".{version}" / "source"
    metacatalog_path = Path(build_base_path) / f".{version}" / catalog_file

    return build_base_path, build_path, metacatalog_path


def _get_project_code(path: str | Path):
    match = re.match(r"/g/data/([^/]*)/.*", str(path))
    return match.groups()[0] if match else None


# Get the project storage flags
def _get_project(paths: list[str], method: str | None = None) -> set[str]:
    projects = set()

    for path in paths:
        # Get the project for the path of the datastore/path itself
        projects |= {_get_project_code(path)}

        # Check the files in the datastore
        if method == "load":
            try:
                esm_ds = open_esm_datastore(path)
                projects |= set(esm_ds.df["path"].map(_get_project_code))
            except KeyError as e:
                # There's no 'path' in the processed source
                # KeyError left in as a precaution, it's not clear what situations
                # this protects again - hence raising a hopefully informative error.
                raise KeyError(
                    e.args[0] + f" - Unexpected missing 'path' in datastore: {path}"
                ) from e
            except FileNotFoundError:
                # The datastore (likely its project) is not available
                warnings.warn(
                    f"Unable to access datastore at {path} - may not be able to ingest."
                )

    projects = {p for p in projects if p is not None}

    return projects


def _confirm_project_access(projects: set[str]) -> tuple[bool, str]:
    """
    Return False and the missing project if the user can't access all necessary projects' /g/data spaces.

    Returns:
        tuple[bool, str]: Whether the user can access all projects, and a string of any missing projects
    """
    missing_projects = []
    for proj in sorted(projects):
        p = Path("/g/data") / proj
        if not p.exists():
            missing_projects.append(proj)

    if len(missing_projects) == 0:
        return True, ""

    return (
        False,
        f"Unable to access projects {', '.join(missing_projects)} - check your group memberships",
    )


def _write_catalog_yaml(
    cm: CatalogManager,
    build_base_path: str | Path,
    storage_flags: str,
    catalog_file: str,
    version: str,
) -> dict[str, Any]:
    """
    Write the catalog details out to YAML.
    """

    cat_name: T_catname = "access_nri" if not cm.use_parquet else "access_nri_pq"

    cat = cm.dfcat
    cat.name = cat_name
    cat.description = "ACCESS-NRI intake catalog"
    yaml_dict = yaml.safe_load(cat.yaml())

    yaml_dict["sources"][cat_name]["args"]["path"] = str(
        Path(build_base_path) / "{{version}}" / catalog_file
    )
    yaml_dict["sources"][cat_name]["args"]["mode"] = "r"
    yaml_dict["sources"][cat_name]["metadata"] = {
        "version": "{{version}}",
        "storage": storage_flags,
    }
    yaml_dict["sources"][cat_name]["parameters"] = {
        "version": {"description": "Catalog version", "type": "str", "default": version}
    }

    # Save the catalog
    cm.save()
    return yaml_dict


def build(  # noqa: PLR0912, PLR0915 # Allow this func to be long and branching
    argv: Sequence[str] | None = None,
):
    """
    Build an intake-dataframe-catalog from YAML configuration file(s).
    """

    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(
        description="Build an intake-dataframe-catalog from YAML configuration file(s)."
    )
    parser.add_argument(
        "config_yaml",
        type=str,
        nargs="+",
        help="Configuration YAML file(s) specifying the Intake source(s) to add.",
    )

    parser.add_argument(
        "--build_base_path",
        type=str,
        default="./",
        help=(
            "Directory in which to build the catalog and source(s). A directory with name equal to the "
            "version (see the `--version` argument) of the catalog being built will be created here. The "
            "catalog file (see the `--catalog_file` argument) will be written into this version directory, "
            "and any new intake source(s) will be written into a 'source' directory within the version "
            "directory. Defaults to the current work directory."
        ),
    )

    parser.add_argument(
        "--catalog_base_path",
        type=str,
        default="./",
        help=(
            "Directory in which to place the catalog.yaml file. This file is the descriptor of the catalog, "
            "and provides references to the data locations where the catalog data itself is stored (build_base_path). "
            "Defaults to the current work directory."
        ),
    )

    parser.add_argument(
        "--data_base_path",
        type=str,
        default="./",
        help=(
            "Home directory that contains the data referenced by the input experiment YAML"
            "files. Typically only required for testing. Defaults to None."
        ),
    )

    parser.add_argument(
        "--catalog_file",
        type=str,
        default=None,
        help="The name of the intake-dataframe-catalog. Defaults to 'metacatalog.csv' if `use_parquet` is False, or `metacatalog.parquet` if `use_parquet` is True",
    )

    parser.add_argument(
        "--version",
        type=str,
        default=datetime.datetime.now().strftime("v%Y-%m-%d"),
        help=(
            "The version of the catalog to build/add to. Defaults to the current date."
        ),
    )

    parser.add_argument(
        "--no_update",
        default=False,
        action="store_true",
        help=(
            "Set this if you don't want to update the access_nri_intake.data (e.g. if running a test)"
        ),
    )

    parser.add_argument(
        "--no_concretize",
        default=False,
        action="store_true",
        help=(
            "Set this if you don't want to concretize the build, ie. keep the new catalog in .$VERSION & don't update catalog.yaml"
        ),
    )

    parser.add_argument(
        "--use_parquet",
        default=False,
        action="store_true",
        help=("Set this if you want to use parquet files instead of csv files"),
    )

    args = parser.parse_args(argv)
    config_yamls = args.config_yaml
    build_base_path = args.build_base_path
    catalog_base_path = args.catalog_base_path
    data_base_path = args.data_base_path
    catalog_file = args.catalog_file
    version = args.version
    update = not args.no_update
    concretize = not args.no_concretize
    use_parquet = args.use_parquet

    if catalog_file is None:
        catalog_file = "metacatalog.parquet" if use_parquet else "metacatalog.csv"

    if not version.startswith("v"):
        version = f"v{version}"
    if not re.match(CATALOG_NAME_FORMAT, version):
        raise ValueError(
            f"Version number/name {version} is invalid. Must be vYYYY-MM-DD, minimum v2000-01-01."
        )

    # Create the build directories
    try:
        build_base_path, build_path, metacatalog_path = _parse_build_directory(
            build_base_path, version, catalog_file
        )
    except PermissionError:
        raise PermissionError(
            f"You lack the necessary permissions to create a catalog at {build_base_path}"
        )
    except FileNotFoundError:
        raise FileNotFoundError(f"Unable to locate {build_base_path}")
    except Exception as e:
        raise Exception(
            "An unexpected error occurred while trying to create the build directory Paths. Please contact ACCESS-NRI."
        ) from e

    # Parse inputs to pass to CatalogManager
    parsed_sources = _parse_build_inputs(config_yamls, build_path, data_base_path)
    _check_build_args([parsed_source[1] for parsed_source in parsed_sources])

    projects = set()
    # Determine the project list & storage flags for this build
    for method, src_args in parsed_sources:
        projects |= _get_project(src_args["path"], method)

    base_project = _get_project_code(build_base_path)
    if base_project is not None:
        projects |= {base_project}
    else:
        warnings.warn(f"Unable to determine project for base path {build_base_path}")

    storage_flags = "+".join(sorted([f"gdata/{proj}" for proj in projects if proj]))

    _valid_permissions, _err_msg = _confirm_project_access(projects)
    if not _valid_permissions:
        raise RuntimeError(_err_msg)

    # Now that that's all passed, create the physical build location
    try:
        Path(build_path).mkdir(parents=True, exist_ok=True)
    except PermissionError:
        raise PermissionError(
            f"You lack the necessary permissions to create a catalog at {build_path}"
        )

    # Build the catalog
    cm = CatalogManager(path=metacatalog_path, use_parquet=use_parquet)
    for method, src_args in parsed_sources:
        _add_source_to_catalog(cm, method, src_args, metacatalog_path, logger=logger)

    # Write catalog yaml file
    # Should fail LOUD
    try:
        yaml_dict = _write_catalog_yaml(
            cm, build_base_path, storage_flags, catalog_file, version
        )
    except Exception as e:
        raise RuntimeError(f"Catalog save failed: {str(e)}")

    if update:
        yaml_dict = VersionHandler(
            yaml_dict, catalog_base_path, build_base_path, version, use_parquet
        )()
    catalog_tmp_path = Path(build_base_path) / f".{version}"

    with Path(get_catalog_fp(basepath=catalog_tmp_path)).open(mode="w") as fobj:
        yaml.dump(yaml_dict, fobj)

    if concretize:
        _concretize_build(
            build_base_path,
            version,
            catalog_file,
            catalog_base_path,
            update,
            force=False,
        )
    else:
        # Dump out a string telling a user how to concretize the build
        print("*** Build Complete! ***")
        print(
            f"To concretize the build, run:\n"
            f"\t $ catalog-concretize --build_base_path {build_base_path} --version {version} --catalog_file {catalog_file} --catalog_base_path {catalog_base_path} \n"
        )

    print(
        "*** Build Complete! *** \n If you are happy with the build, please remember to update the forum topic: https://forum.access-hive.org.au/t/access-nri-intake-catalog-a-way-to-find-load-and-share-data-on-gadi/1659/"
    )


def concretize(argv: Sequence[str] | None = None):
    """
    Concretize a build by moving it to the final location and updating the paths in the catalog.json files.
    """
    parser = argparse.ArgumentParser(
        description="Concretize a build by moving it to the final location and updating the paths in the catalog.json files."
    )
    parser.add_argument(
        "--build_base_path",
        type=str,
        help="The base path for the build.",
    )
    parser.add_argument(
        "--version",
        type=str,
        help="The version of the build.",
    )
    parser.add_argument(
        "--catalog_file",
        type=str,
        help="The name of the catalog file.",
    )
    parser.add_argument(
        "--catalog_base_path",
        type=str,
        default=None,
        help=(
            "The base path for the catalog. If None, the catalog_base_path will be set to the build_base_path."
            " Defaults to None."
        ),
    )
    parser.add_argument(
        "--no_update",
        action="store_true",
        default=False,
        help=(
            "Set this if you don't want to update the catalog.yaml file. Defaults to False."
            " If False, the catalog.yaml file will be updated."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help=(
            "Force the concretization of the build, even if a version of the catalog with the specified "
            "version number already exists in the catalog_base_path. Defaults to False."
        ),
    )

    args = parser.parse_args(argv)
    try:
        _concretize_build(
            args.build_base_path,
            args.version,
            args.catalog_file,
            args.catalog_base_path,
            not args.no_update,
            args.force,
        )
    except DirectoryExistsError as e:
        raise DirectoryExistsError(
            f"Unable to concretize catalog build: Catalog version {args.version} "
            f"already exists in {args.catalog_base_path}. Use "
            f"`catalog-concretize --catalog_file {args.catalog_file} --build_base_path {args.build_base_path} --version {args.version} --catalog_base_path {args.catalog_base_path} --force` to overwrite it."
        ) from e


def _concretize_build(  # noqa: PLR0913 # Allow this func to have many arguments
    build_base_path: str | Path,
    version: str,
    catalog_file: str,
    catalog_base_path: str | Path | None = None,
    update: bool = True,
    force: bool = False,
) -> None:
    """
    Take the build in it's temporary location, update all the paths within the
    catalog.json files to point to the new location, and then finally move it out
    to the final location.

    Parameters
    ----------
    build_base_path : str | Path
        The base path for the build.
    version : str
        The version of the build.
    catalog_file : str
        The name of the catalog file.
    catalog_base_path : str | Path, optional
        The base path for the catalog. If None, the catalog_base_path will be
        set to the build_base_path. Defaults to None.
    update : bool
        Whether to update the catalog.yaml file. Defaults to True. If False, the
        catalog.yaml file will not be updated.
    force : bool
        Whether to concretize the build even if a catalog with the same version
        number already exists in the catalog_base_path.

    Raises
    ------
    DirectoryExistsError
        If the catalog version already exists in the catalog_base_path and force is False.
        If the build_base_path does not exist or is not a directory.
        If the catalog_base_path does not exist or is not a directory.
    """
    catalog_base_path = (
        Path(build_base_path) if catalog_base_path is None else Path(catalog_base_path)
    )

    # First, 'unhide' paths in the metacatalog.csv file
    metacatalog_path = Path(build_base_path) / f".{version}" / catalog_file

    if metacatalog_path.suffix[1:] == "csv":
        pl.scan_csv(metacatalog_path).with_columns(
            pl.col("yaml").str.replace(f".{version}", version, literal=True)
        ).collect().write_csv(metacatalog_path)
    else:
        pl.scan_parquet(metacatalog_path).with_columns(
            pl.col("yaml").str.replace(f".{version}", version, literal=True)
        ).collect().write_parquet(metacatalog_path)

    source_files = (Path(build_base_path) / f".{version}" / "source").glob("*.json")

    # Then 'unhide' the paths in the catalog.json files
    for f in source_files:
        pl.read_json(f).with_columns(
            pl.col("catalog_file").str.replace(f".{version}", version, literal=True)
        ).write_ndjson(f)

    # Now unhide the directory containing the catalog
    src = Path(build_base_path) / f".{version}"
    dst = Path(catalog_base_path) / version
    try:
        src.rename(dst)
    except OSError as e:
        if not force:
            raise DirectoryExistsError(
                f"Catalog version {version} already exists in {catalog_base_path}. Use "
                f"`catalog-concretize --catalog_file {catalog_file} --build_base_path {build_base_path} --version {version} --catalog_base_path {catalog_base_path} --force` to overwrite it."
            ) from e

        tmp = Path(catalog_base_path) / f".tmp-old-{version}"
        dst.rename(tmp)  # Move the existing version out of the way
        src.rename(dst)  # Move the new version to the final location
        shutil.rmtree(tmp)

    if update:
        # Move the catalog.yaml file to the new location, if we're updating it
        catalog_src = Path(catalog_base_path) / version / "catalog.yaml"
        catalog_dst = Path(catalog_base_path) / "catalog.yaml"
        catalog_src.rename(catalog_dst)


def _combine_storage_flags(a: str, b: str) -> str:
    """
    Return a combined storage flag string from two incoming strings.
    """
    aflags = re.findall(STORAGE_FLAG_PATTERN, a)
    bflags = re.findall(STORAGE_FLAG_PATTERN, b)
    # Sorting the return aids in testing & comparison,
    # plus makes it more human-readable/human-searchable
    return "+".join(sorted(list(set(aflags + bflags))))


def metadata_validate(argv: Sequence[str] | None = None):
    """
    Check provided metadata.yaml file(s) against the experiment schema
    """

    parser = argparse.ArgumentParser(description="Validate a metadata.yaml file")
    parser.add_argument(
        "file",
        nargs="+",
        help="The path to the metadata.yaml file. Multiple file paths can be passed.",
    )

    args = parser.parse_args(argv)
    files = args.file

    for f in files:
        if Path(f).is_file():
            print(f"Validating {f}... ")
            try:
                load_metadata_yaml(f, EXP_JSONSCHEMA)
                print("\nSuccess!")
            except jsonschema.ValidationError as e:  # Don't print the stacktrace
                print("\nVALIDATION FAILED:")
                print(e.message)
            except Exception as e:  # Not validation related, show stacktrace
                print(
                    "The script has failed, but it doesn't appear to be a validation error. See the stack trace below."
                )
                raise e
        else:
            raise FileNotFoundError(f"No such file(s): {f}")


def metadata_template(argv: Sequence[str] | None = None) -> None:
    """
    Create an empty template for a metadata.yaml file using the experiment schema.

    Writes the template to the current working directory by default.

    Parameters:
    -----
        loc (str, Path, optional): The directory in which to save the template.
        Defaults to the current working directory.

    Returns:
    -----
        None
    """

    parser = argparse.ArgumentParser(description="Create a template metadata.yaml file")
    parser.add_argument(
        "--loc",
        help="The directory in which to save the template. Defaults to the current working directory.",
        default=str(Path.cwd()),
    )

    args = parser.parse_args(argv)
    loc = Path(args.loc)

    argparse.ArgumentParser(description="Generate a template for metadata.yaml")

    template = {}
    for name, descr in EXP_JSONSCHEMA["properties"].items():
        if "const" in descr.keys():
            description = descr["const"]
        elif name in EXP_JSONSCHEMA["required"]:
            description = f"<REQUIRED {descr['description']}>"
        else:
            description = f"<{descr['description']}>"

        if _can_be_array(descr):
            description = [description]  # type: ignore

        template[name] = description

    with open((Path(loc) / "metadata.yaml"), "w") as outfile:
        yaml.dump(template, outfile, default_flow_style=False, sort_keys=False)


def use_esm_datastore(argv: Sequence[str] | None = None) -> int:
    """
    Either creates, verifies, or updates the intake-esm datastore
    """
    parser = argparse.ArgumentParser(
        description=(
            "Build an esm-datastore by inspecting a directory containing model outputs."
            " If no datastore exists, a new one will be created. If a datastore exists,"
            " its integrity will be verified, and the datastore regenerated if necessary."
        )
    )
    parser.add_argument(
        "--builder",
        type=str,
        help=(
            "Builder to use to create the esm-datastore."
            f" Builders are defined the {f_path}source.builders{f_reset} module. Currently available options are:"
            f" {f_info}{', '.join(builders.__all__)}{f_reset}."
            " To build a datastore for a new model, please contact the ACCESS-NRI team."
        ),
        required=False,
        # If we can, it would be nice to eventually relax this and try to automatically
        # determine the builder if possible.
    )

    parser.add_argument(
        "--builder-kwargs",
        type=parse_kwarg,
        nargs="*",
        help=(
            "Additional keyword arguments to pass to the builder."
            f" Should be in the form of {f_info}key=value{f_reset}."
        ),
    )

    parser.add_argument(
        "--expt-dir",
        type=str,
        default="./",
        help=(
            "Directory containing the model outputs to be added to the esm-datastore."
            " Defaults to the current working directory. Although builders support adding"
            " multiple directories, this tool only supports one directory at a time - at present."
        ),
    )

    parser.add_argument(
        "--cat-dir",
        type=str,
        help=(
            "Directory in which to place the catalog.json file."
            f" Defaults to the value of {f_info}--expt-dir{f_reset} if not set."
        ),
    )

    parser.add_argument(
        "--datastore-name",
        type=str,
        help=(
            "Name of the datastore to use. If not provided, this will default to"
            f" {f_info}'experiment_datastore'{f_reset}."
        ),
        default="experiment_datastore",
    )

    parser.add_argument(
        "--description",
        type=str,
        help=(
            "Description of the datastore. If not provided, a default description will be used:"
            f" 'esm_datastore for the model output in {f_info}{{--expt-dir}}{f_reset}'"
        ),
        default=None,
    )

    args = parser.parse_args(argv)
    builder = args.builder
    experiment_dir = Path(args.expt_dir)
    catalog_dir = Path(args.cat_dir) if args.cat_dir else experiment_dir
    builder_kwargs = (
        {k: v for k, v in args.builder_kwargs} if args.builder_kwargs else {}
    )
    datastore_name = args.datastore_name
    description = args.description

    try:
        builder = getattr(builders, builder)
    except AttributeError:
        builder = object
    except TypeError:
        builder = None
    finally:
        if builder is None:
            pass
        elif not isinstance(builder, type) or not issubclass(builder, builders.Builder):
            raise ValueError(
                f"Builder {builder} is not a valid builder. Please choose from {builders.__all__}"
            )

    if not experiment_dir.exists():
        raise FileNotFoundError(f"Directory {experiment_dir} does not exist.")
    if not catalog_dir.exists():
        raise FileNotFoundError(f"Directory {catalog_dir} does not exist.")

    validate_args(builder, builder_kwargs)

    use_datastore(
        experiment_dir,
        builder,
        catalog_dir,
        builder_kwargs=builder_kwargs,
        datastore_name=datastore_name,
        description=description,
        open_ds=False,
    )

    return 0


def scaffold_catalog_entry(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Scaffold a catalog entry for an esm-datastore, by providing information"
            " about how to integrate the datastore into the access-nri-intake catalog."
        )
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        default=False,
        required=False,
        help=(
            "Instead of dumping all the information at once, provide it in chunks"
            " and ask for confirmation after each chunk."
        ),
    )

    args = parser.parse_args(argv)

    interactive = args.interactive

    _scaffold_catalog_entry(interactive)

    return 0
