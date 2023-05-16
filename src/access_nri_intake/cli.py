# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import argparse
import logging

import jsonschema
import yaml

from .esmcat import builders
from .metacat import METADATA_JSONSCHEMA, manager, translators
from .utils import load_metadata_yaml, validate_against_schema


class MetadataCheckError(Exception):
    pass


def _parse_inputs(config_yamls, build_path):
    """
    Parse inputs into a list of tuples of MetacatManager methods and args to
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
    Run some checks on the parsed argmuents to be passed to the MetacatManager
    """

    names = []
    uuids = []
    for args in args_list:
        names.append(args["name"])
        uuids.append(args["metadata"]["experiment_uuid"])
        try:
            validate_against_schema(args["metadata"], METADATA_JSONSCHEMA)
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
    Build/add intake catalog(s) specified in a YAML configuration file to an intake-dataframe-catalog metacatalog
    """

    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(
        description=(
            "Build/add intake catalog(s) specified in a YAML configuration file to an intake-dataframe-catalog "
            "metacatalog"
        )
    )
    parser.add_argument(
        "config_yaml",
        type=str,
        nargs="+",
        help="Configuration YAML file(s) specifying the intake catalog(s) to add",
    )

    parser.add_argument(
        "--build_path",
        type=str,
        default="./",
        help=(
            "Directory in which to build the intake catalog(s). Ignored if builder is null in config (i.e. if ",
            "adding an existing catalog)",
        ),
    )

    parser.add_argument(
        "--metacatalog_file",
        type=str,
        default="./metacatalog.csv",
        help="The path to the intake-dataframe-catalog metacatalog",
    )

    args = parser.parse_args()
    config_yamls = args.config_yaml
    build_path = args.build_path
    metacatalog_file = args.metacatalog_file

    parsed_sources = _parse_inputs(config_yamls, build_path)
    _check_args([parsed_source[1] for parsed_source in parsed_sources])

    for (method, args) in parsed_sources:
        man = manager.MetacatManager(path=metacatalog_file)
        logger.info(f"Adding '{args['name']}' to metacatalog '{metacatalog_file}'")
        getattr(man, method)(**args).add()
