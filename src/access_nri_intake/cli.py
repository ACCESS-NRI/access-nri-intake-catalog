# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Command line interfaces for access-nri-intake"""

import argparse
import datetime
import logging
import os
import re
from pathlib import Path

import jsonschema
import yaml
from intake import open_esm_datastore

from .catalog import EXP_JSONSCHEMA, translators
from .catalog.manager import CatalogManager
from .data import CATALOG_NAME_FORMAT
from .source import builders
from .utils import _can_be_array, get_catalog_fp, load_metadata_yaml

STORAGE_FLAG_PATTERN = "gdata/[a-z]{1,2}[0-9]{1,2}"


class MetadataCheckError(Exception):
    pass


def _parse_build_inputs(config_yamls, build_path):
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
            config_args["directory"] = build_path
            config_args["overwrite"] = True
        else:
            method = "load"

        for kwargs in sources:
            source_args = config_args

            source_args["path"] = kwargs.pop("path")
            metadata_yaml = kwargs.pop("metadata_yaml")
            try:
                metadata = load_metadata_yaml(metadata_yaml, EXP_JSONSCHEMA)
            except jsonschema.exceptions.ValidationError:
                raise MetadataCheckError(
                    f"Failed to validate metadata.yaml @ {os.path.dirname(metadata_yaml)}. See traceback for details."
                )
            source_args["name"] = metadata["name"]
            source_args["description"] = metadata["description"]
            source_args["metadata"] = metadata

            if translator:
                source_args["translator"] = getattr(translators, translator)

            args.append((method, source_args | kwargs))

    return args


def _check_build_args(args_list):
    """
    Run some checks on the parsed build argmuents to be passed to the CatalogManager
    """

    names = []
    uuids = []
    for args in args_list:
        names.append(args["name"])
        uuids.append(args["metadata"]["experiment_uuid"])

    if len(names) != len(set(names)):
        seen = set()
        dupes = [name for name in names if name in seen or seen.add(name)]
        raise MetadataCheckError(f"There are experiments with the same name: {dupes}")
    if len(uuids) != len(set(uuids)):
        seen = set()
        dupes = [uuid for uuid in uuids if uuid in seen or seen.add(uuid)]
        dupes = [name for name, uuid in zip(names, uuids) if uuid in dupes]
        raise MetadataCheckError(
            f"There are experiments with the same experiment_uuid: {dupes}"
        )


def build():
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

    args = parser.parse_args()
    config_yamls = args.config_yaml
    build_base_path = args.build_base_path
    catalog_base_path = args.catalog_base_path
    catalog_file = args.catalog_file
    version = args.version
    update = not args.no_update

    if not version.startswith("v"):
        version = f"v{version}"
    if not re.match(CATALOG_NAME_FORMAT, version):
        raise ValueError(
            f"Version number/name {version} is invalid. Must be vYYYY-MM-DD."
        )

    # Create the build directories
    build_base_path = os.path.abspath(build_base_path)
    build_path = os.path.join(build_base_path, version, "source")
    metacatalog_path = os.path.join(build_base_path, version, catalog_file)
    os.makedirs(build_path, exist_ok=True)

    # Parse inputs to pass to CatalogManager
    parsed_sources = _parse_build_inputs(config_yamls, build_path)
    _check_build_args([parsed_source[1] for parsed_source in parsed_sources])

    # Get the project storage flags
    def _get_project(path):
        match = re.match(r"/g/data/([^/]*)/.*", path)
        return match.groups()[0] if match else None

    project = set()
    for method, args in parsed_sources:
        if method == "load":
            # This is a hack but I don't know how else to get the storage from pre-built datastores
            esm_ds = open_esm_datastore(args["path"][0])
            project |= set(esm_ds.df["path"].map(_get_project))

        project |= {_get_project(path) for path in args["path"]}
    project |= {_get_project(build_base_path)}
    storage_flags = "+".join(sorted([f"gdata/{proj}" for proj in project]))

    # Build the catalog
    cm = CatalogManager(path=metacatalog_path)
    for method, args in parsed_sources:
        logger.info(f"Adding '{args['name']}' to metacatalog '{metacatalog_path}'")
        getattr(cm, method)(**args)

    # Write catalog yaml file
    cat = cm.dfcat
    cat.name = "access_nri"
    cat.description = "ACCESS-NRI intake catalog"
    yaml_dict = yaml.safe_load(cat.yaml())

    yaml_dict["sources"]["access_nri"]["args"]["path"] = os.path.join(
        build_base_path, "{{version}}", catalog_file
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

    if update:
        cat_loc = get_catalog_fp(basepath=catalog_base_path)
        existing_cat = os.path.exists(cat_loc)

        # See if there's an existing catalog
        if existing_cat:
            with Path(cat_loc).open(mode="r") as fobj:
                yaml_old = yaml.safe_load(fobj)

            # Check to see what has changed. We care if the following keys
            # have changed (ignoring the sources.access_nri at the head
            # of each dict path):
            # - args (all parts - mode should never change)
            # - driver
            # If these have changed, we need to move the old catalog aside,
            # labelled with its min and max version numbers
            # The exception to this rule is if the old catalog doesn't have
            # a min or max version - this makes it likely to be an old-style
            # catalog, so we'll need to grab its storage flags, but we don't
            # want to save it (we assume all existing catalog versions are
            # compatible with the new one).

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
                os.rename(
                    cat_loc,
                    os.path.join(os.path.dirname(cat_loc), f"catalog-{vers_str}.yaml"),
                )
                yaml_dict = _set_catalog_yaml_version_bounds(
                    yaml_dict, version, version
                )
            elif storage_new != storage_old:
                yaml_dict["sources"]["access_nri"]["metadata"][
                    "storage"
                ] = _combine_storage_flags(storage_new, storage_old)

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
            existing_vers = os.listdir(build_base_path)
            existing_vers = [
                v for v in existing_vers if re.match(CATALOG_NAME_FORMAT, v)
            ]
            if len(existing_vers) > 0:
                yaml_dict = _set_catalog_yaml_version_bounds(
                    yaml_dict,
                    min(min(existing_vers), version),
                    max(max(existing_vers), version),
                )
            else:
                yaml_dict = _set_catalog_yaml_version_bounds(
                    yaml_dict, version, version
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


def metadata_validate():
    """
    Check provided metadata.yaml file(s) against the experiment schema
    """

    parser = argparse.ArgumentParser(description="Validate a metadata.yaml file")
    parser.add_argument(
        "file",
        nargs="+",
        help="The path to the metadata.yaml file. Multiple file paths can be passed.",
    )

    args = parser.parse_args()
    files = args.file

    for f in files:
        if os.path.isfile(f):
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


def metadata_template():
    """
    Create an empty template for a metadata.yaml file using the experiment schema
    """

    argparse.ArgumentParser(description="Generate a template for metadata.yaml")

    template = {}
    for name, descr in EXP_JSONSCHEMA["properties"].items():
        if name in EXP_JSONSCHEMA["required"]:
            description = f"<REQUIRED {descr['description']}>"
        else:
            description = f"<{descr['description']}>"

        if _can_be_array(descr):
            description = [description]

        template[name] = description

    with open("./metadata.yaml", "w") as outfile:
        yaml.dump(template, outfile, default_flow_style=False, sort_keys=False)
