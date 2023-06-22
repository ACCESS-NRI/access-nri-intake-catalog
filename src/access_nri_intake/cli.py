# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Command line interface for access-nri-intake """

import argparse
import logging
import os

import jsonschema
import yaml
from intake import open_esm_datastore

from . import __version__
from .catalog import EXP_JSONSCHEMA, translators
from .catalog.manager import CatalogManager
from .source import builders
from .utils import load_metadata_yaml, validate_against_schema


class MetadataCheckError(Exception):
    pass


def _parse_inputs(config_yamls, build_path):
    """
    Parse inputs into a list of tuples of CatalogManager methods and args to
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
            metadata = load_metadata_yaml(metadata_yaml)
            source_args["name"] = metadata["name"]
            source_args["description"] = metadata["description"]
            source_args["metadata"] = metadata

            if translator:
                source_args["translator"] = getattr(translators, translator)

            args.append((method, source_args | kwargs))

    return args


def _check_args(args_list):
    """
    Run some checks on the parsed argmuents to be passed to the CatalogManager
    """

    names = []
    uuids = []
    for args in args_list:
        names.append(args["name"])
        uuids.append(args["metadata"]["experiment_uuid"])
        try:
            validate_against_schema(args["metadata"], EXP_JSONSCHEMA)
        except jsonschema.exceptions.ValidationError:
            raise MetadataCheckError(
                f"Failed to validate metadata.yaml for {args['name']}. See traceback for details."
            )

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
        "--catalog_file",
        type=str,
        default="metacatalog.csv",
        help="The name of the intake-dataframe-catalog. Defaults to 'metacatalog.csv'",
    )

    parser.add_argument(
        "--version",
        type=str,
        default=__version__,
        help=(
            "The version of the catalog to build/add to. Defaults to the current version of access-nri-intake."
        ),
    )

    args = parser.parse_args()
    config_yamls = args.config_yaml
    build_base_path = args.build_base_path
    catalog_file = args.catalog_file
    version = args.version

    if not version.startswith("v"):
        version = f"v{version}"

    # Create the build directories
    build_base_path = os.path.abspath(build_base_path)
    build_path = os.path.join(build_base_path, version, "source")
    metacatalog_path = os.path.join(build_base_path, version, catalog_file)
    os.makedirs(build_path, exist_ok=True)

    # Parse inputs to pass to CatalogManager
    parsed_sources = _parse_inputs(config_yamls, build_path)
    _check_args([parsed_source[1] for parsed_source in parsed_sources])

    # Get the project storage flags
    def _get_project(path):
        return os.path.normpath(path).split(os.sep)[3]

    project = set()
    for method, args in parsed_sources:
        if method == "load":
            # This is a hack but I don't know how else to get the storage from pre-built datastores
            esm_ds = open_esm_datastore(args["path"][0])
            project |= set(esm_ds.df["path"].map(_get_project))

        project |= {_get_project(path) for path in args["path"]}
    storage_flags = "+".join(sorted([f"gdata/{proj}" for proj in project]))

    # Build the catalog
    for method, args in parsed_sources:
        man = CatalogManager(path=metacatalog_path)
        logger.info(f"Adding '{args['name']}' to metacatalog '{metacatalog_path}'")
        getattr(man, method)(**args).add()

    # Write catalog yaml file
    cat = man.dfcat
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

    _here = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(_here, "data", "catalog.yaml"), "w") as fobj:
        yaml.dump(yaml_dict, fobj)
