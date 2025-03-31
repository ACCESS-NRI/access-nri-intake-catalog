# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Command line interfaces for access-nri-intake"""

import argparse
import datetime
import logging
import re
import traceback
import warnings
from collections.abc import Sequence
from pathlib import Path

import jsonschema
import yaml
from intake import open_esm_datastore

from .catalog import EXP_JSONSCHEMA, translators
from .catalog.manager import CatalogManager
from .data import CATALOG_NAME_FORMAT
from .experiment import use_datastore
from .experiment.main import scaffold_catalog_entry as _scaffold_catalog_entry
from .experiment.utils import parse_kwarg, validate_args
from .source import builders
from .utils import _can_be_array, get_catalog_fp, load_metadata_yaml

STORAGE_FLAG_PATTERN = "gdata/[a-z]{1,2}[0-9]{1,2}"


class MetadataCheckError(Exception):
    pass


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
            f"Unable to add {src_args['name']} to catalog - continuing", source=e
        )


def _parse_build_directory(
    build_base_path: str | Path, version: str, catalog_file: str
) -> tuple[Path, Path, Path]:
    """
    Build the location for the new catalog

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
    build_path = Path(build_base_path) / version / "source"
    metacatalog_path = Path(build_base_path) / version / catalog_file

    return build_base_path, build_path, metacatalog_path


def _get_project_code(path: str | Path):
    match = re.match(r"/g/data/([^/]*)/.*", str(path))
    return match.groups()[0] if match else None


# Get the project storage flags
def _get_project(paths: list[str], method: str | None = None):
    project = set()
    if method == "load":
        # This is a hack but I don't know how else to get the storage from pre-built datastores
        esm_ds = open_esm_datastore(paths[0])
        project |= set(esm_ds.df["path"].map(_get_project_code))
    else:  # I know this isn't formally necessary, but I find it easier to read
        project |= {_get_project_code(path) for path in paths}

    project = {p for p in project if p is not None}

    return project


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
) -> dict:
    """
    Write the catalog details out to YAML.
    """
    cat = cm.dfcat
    cat.name = "access_nri"
    cat.description = "ACCESS-NRI intake catalog"
    yaml_dict = yaml.safe_load(cat.yaml())

    yaml_dict["sources"]["access_nri"]["args"]["path"] = str(
        Path(build_base_path) / "{{version}}" / catalog_file
    )
    yaml_dict["sources"]["access_nri"]["args"]["mode"] = "r"
    yaml_dict["sources"]["access_nri"]["metadata"] = {
        "version": "{{version}}",
        "storage": storage_flags,
    }
    yaml_dict["sources"]["access_nri"]["parameters"] = {
        "version": {"description": "Catalog version", "type": "str", "default": version}
    }

    # Save the catalog
    cm.save()
    return yaml_dict


def _compute_previous_versions(
    yaml_dict: dict,
    catalog_base_path: Path,
    build_base_path: Path,
    version: str,
) -> dict:
    """Calculate previous version information for a new catalog build.

    Parameters
    ----------
    yaml_dict : dict
        The existing YAML dictionary describing the new catalog
    catalog_base_path : Path
        The catalog base path.
    build_base_path : Path
        The catalog build base path.
    version : str
        The current version of the catalog (this has yet to enter `yaml_dict`).

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
    """
    cat_loc = get_catalog_fp(basepath=catalog_base_path)
    existing_cat = Path(cat_loc).exists()

    # See if there's an existing catalog
    if existing_cat:
        with Path(cat_loc).open(mode="r") as fobj:
            yaml_old = yaml.safe_load(fobj)

        # Check to see what has changed. We care if the following keys
        # have changed (ignoring the sources.access_nri at the head
        # of each dict path):
        # - args (all parts - mode should never change)
        # - driver

        args_new, args_old = (
            yaml_dict["sources"]["access_nri"]["args"],
            yaml_old["sources"]["access_nri"]["args"],
        )
        driver_new, driver_old = (
            yaml_dict["sources"]["access_nri"]["driver"],
            yaml_old["sources"]["access_nri"]["driver"],
        )
        vmin_old, vmax_old = (
            yaml_old["sources"]["access_nri"]["parameters"]["version"].get("min"),
            yaml_old["sources"]["access_nri"]["parameters"]["version"].get("max"),
        )
        storage_new, storage_old = (
            yaml_dict["sources"]["access_nri"]["metadata"]["storage"],
            yaml_old["sources"]["access_nri"]["metadata"]["storage"],
        )

        if (
            (args_new != args_old or driver_new != driver_old)
            and vmin_old is not None
            and vmax_old is not None
        ):
            # Move the old catalog out of the way
            # New catalog.yaml will have restricted version bounds
            if vmin_old == vmax_old:
                vers_str = vmin_old
            else:
                vers_str = f"{vmin_old}-{vmax_old}"
            Path(cat_loc).rename(Path(cat_loc).parent / f"catalog-{vers_str}.yaml")
            yaml_dict = _set_catalog_yaml_version_bounds(yaml_dict, version, version)
        elif storage_new != storage_old:
            yaml_dict["sources"]["access_nri"]["metadata"]["storage"] = (
                _combine_storage_flags(storage_new, storage_old)
            )

        # Set the minimum and maximum catalog versions, if they're not set already
        # in the 'new catalog' if statement above
        if (
            yaml_dict["sources"]["access_nri"]["parameters"]["version"].get("min")
            is None
        ):
            yaml_dict = _set_catalog_yaml_version_bounds(
                yaml_dict,
                min(version, vmin_old if vmin_old is not None else version),
                max(version, vmax_old if vmax_old is not None else version),
            )

    if (not existing_cat) or (vmin_old is None and vmax_old is None):
        # No existing catalog, so set min = max = current version,
        # unless there are folders with the right names in the write
        # directory
        existing_vers = [
            v.name
            for v in build_base_path.iterdir()
            if re.match(CATALOG_NAME_FORMAT, v.name)
        ]
        if len(existing_vers) > 1:
            yaml_dict = _set_catalog_yaml_version_bounds(
                yaml_dict,
                min(min(existing_vers), version),
                max(max(existing_vers), version),
            )
        else:
            yaml_dict = _set_catalog_yaml_version_bounds(yaml_dict, version, version)

    return yaml_dict


def build(argv: Sequence[str] | None = None):
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
        default="metacatalog.csv",
        help="The name of the intake-dataframe-catalog. Defaults to 'metacatalog.csv'",
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

    args = parser.parse_args(argv)
    config_yamls = args.config_yaml
    build_base_path = args.build_base_path
    catalog_base_path = args.catalog_base_path
    data_base_path = args.data_base_path
    catalog_file = args.catalog_file
    version = args.version
    update = not args.no_update

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

    project = set()
    # Determine the project list & storage flags for this build
    for method, src_args in parsed_sources:
        try:
            project |= _get_project(src_args["path"], method)
        except KeyError:  # There's no 'path' in the processed source
            warnings.warn(
                f"Unable to determine storage flags/projects for {src_args.get('name', '<no name either>')} - may not be able to be ingested"
            )

    base_project = _get_project_code(build_base_path)
    if base_project is not None:
        project |= {base_project}
    else:
        warnings.warn(f"Unable to determine project for base path {build_base_path}")

    storage_flags = "+".join(sorted([f"gdata/{proj}" for proj in project if proj]))

    _valid_permissions, _err_msg = _confirm_project_access(project)
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
    cm = CatalogManager(path=metacatalog_path)
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
        yaml_dict = _compute_previous_versions(
            yaml_dict, catalog_base_path, build_base_path, version
        )

        with Path(get_catalog_fp(basepath=catalog_base_path)).open(mode="w") as fobj:
            yaml.dump(yaml_dict, fobj)


def _set_catalog_yaml_version_bounds(d: dict, bl: str, bu: str) -> dict:
    """
    Set the version boundaries for the access_nri_intake_catalog.
    """
    d["sources"]["access_nri"]["parameters"]["version"]["min"] = bl
    d["sources"]["access_nri"]["parameters"]["version"]["max"] = bu

    return d


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
        else:
            if name in EXP_JSONSCHEMA["required"]:
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
            " Builders are defined the source.builders module. Currently available options are:"
            f" {', '.join(builders.__all__)}."
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
            " Should be in the form of key=value."
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
            " Defaults to the value of --expt-dir if not set."
        ),
    )

    parser.add_argument(
        "--datastore-name",
        type=str,
        help=(
            "Name of the datastore to use. If not provided, this will default to"
            " 'experiment_datastore'."
        ),
        default="experiment_datastore",
    )

    parser.add_argument(
        "--description",
        type=str,
        help=(
            "Description of the datastore. If not provided, a default description will be used:"
            " 'esm_datastore for the model output in {--expt-dir}'"
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
