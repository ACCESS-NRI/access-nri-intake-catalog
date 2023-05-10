import argparse
import logging

import jsonschema
import yaml

from catalog_manager import esmcat, metacat


class MetadataCheckError(Exception):
    pass


def _parse_config_yamls(config_yamls):
    """
    Parse a list of configuration YAML files into a list of tuples of
    MetacatManager methods and args to pass to the methods
    """

    class NoDatesSafeLoader(yaml.SafeLoader):
        @classmethod
        def remove_implicit_resolver(cls, tag_to_remove):
            """
            Remove implicit resolvers for a particular tag

            See https://stackoverflow.com/questions/34667108/ignore-dates-and-times-while-parsing-yaml
            """
            if "yaml_implicit_resolvers" not in cls.__dict__:
                cls.yaml_implicit_resolvers = cls.yaml_implicit_resolvers.copy()

            for first_letter, mappings in cls.yaml_implicit_resolvers.items():
                cls.yaml_implicit_resolvers[first_letter] = [
                    (tag, regexp) for tag, regexp in mappings if tag != tag_to_remove
                ]

    NoDatesSafeLoader.remove_implicit_resolver("tag:yaml.org,2002:timestamp")

    args = []
    for config_yaml in config_yamls:
        with open(config_yaml) as f:
            config = yaml.safe_load(f)

        builder = config.get("builder")
        translator = config.get("translator")
        subcatalog_dir = config.get("subcatalog_dir")
        subcatalogs = config.get("subcatalogs")

        config_args = {}
        if builder:
            manager = "build_esm"
            config_args["builder"] = getattr(esmcat, builder)
            config_args["directory"] = subcatalog_dir
            config_args["overwrite"] = True
        else:
            manager = "load"

        for kwargs in subcatalogs:
            subcat_args = config_args

            subcat_args["path"] = kwargs.pop("path")
            metadata_yaml = kwargs.pop("metadata_yaml")
            with open(metadata_yaml) as fpath:
                metadata = yaml.load(fpath, Loader=NoDatesSafeLoader)
            subcat_args["name"] = metadata["name"]
            subcat_args["description"] = metadata["short_description"]
            subcat_args["metadata"] = metadata

            if translator:
                subcat_args["translator"] = getattr(metacat.translators, translator)

            args.append((manager, subcat_args | kwargs))

    return args


def _check_args(args_list):
    """
    Run some checks on the parsed argmuents to be passed to the MetacatManager
    """

    # TO DO: This should come from a common source
    metadata_schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "experiment_uuid": {
                "type": "string",
                "format": "uuid",
            },
            "short_description": {"type": "string"},
            "long_description": {"type": "string"},
            "model": {
                "oneOf": [
                    {"type": ["string", "null"]},
                    {
                        "type": "array",
                        "items": {"type": ["string", "null"]},
                    },
                ]
            },
            "nominal_resolution": {
                "oneOf": [
                    {"type": ["string", "null"]},
                    {
                        "type": "array",
                        "items": {"type": ["string", "null"]},
                    },
                ]
            },
            "version": {"type": ["number", "null"]},
            "contact": {"type": ["string", "null"]},
            "email": {"type": ["string", "null"]},
            "created": {"type": ["string", "null"]},
            "reference": {"type": ["string", "null"]},
            "url": {"type": ["string", "null"]},
            "parent_experiment": {"type": ["string", "null"]},
            "related_experiments": {
                "type": ["array", "null"],
                "items": {"type": ["string", "null"]},
            },
            "notes": {"type": ["string", "null"]},
            "keywords": {
                "type": ["array", "null"],
                "items": {"type": ["string", "null"]},
            },
        },
        "required": [
            "name",
            "experiment_uuid",
            "short_description",
            "long_description",
            "model",
            "nominal_resolution",
            "version",
            "contact",
            "email",
            "created",
            "reference",
            "url",
            "parent_experiment",
            "related_experiments",
            "notes",
            "keywords",
        ],
    }

    names = []
    uuids = []
    for args in args_list:
        names.append(args["name"])
        uuids.append(args["metadata"]["experiment_uuid"])
        try:
            jsonschema.validate(args["metadata"], metadata_schema)
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
        "--catalog_name",
        type=str,
        default="dfcatalog.csv",
        help="The path to the intake-dataframe-catalog",
    )

    args = parser.parse_args()
    config_yamls = args.config_yaml
    catalog_name = args.catalog_name

    parsed_subcats = _parse_config_yamls(config_yamls)
    _check_args([parsed_subcat[1] for parsed_subcat in parsed_subcats])

    for (method, args) in parsed_subcats:
        manager = metacat.MetacatManager(path=catalog_name)
        logger.info(f"Adding '{args['name']}' to metacatalog '{catalog_name}'")
        getattr(manager, method)(**args).add()
