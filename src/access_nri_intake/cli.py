import argparse
import logging

import jsonschema
import yaml

from . import esmcat, metacat, utils


class MetadataCheckError(Exception):
    pass


def _load_metadata_yaml(path):
    """
    Load a metadata.yaml file, leaving dates as strings and loading arrays as tuples

    Parameters
    ----------
    paths: str
        The path to the metadata.yaml
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

    def tuple_constructor(self, node):
        """
        yaml constructor to make leaf sequences into tuples

        See https://stackoverflow.com/questions/39553008/how-to-read-a-python-tuple-using-pyyaml
        """
        seq = self.construct_sequence(node)
        if seq and isinstance(seq[0], (list, tuple)):
            return seq
        return tuple(seq)

    NoDatesSafeLoader.remove_implicit_resolver("tag:yaml.org,2002:timestamp")
    NoDatesSafeLoader.add_constructor("tag:yaml.org,2002:seq", tuple_constructor)

    with open(path) as fpath:
        metadata = yaml.load(fpath, Loader=NoDatesSafeLoader)

    return metadata


def _parse_config_yamls(config_yamls):
    """
    Parse a list of configuration YAML files into a list of tuples of
    MetacatManager methods and args to pass to the methods
    """

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
            metadata = _load_metadata_yaml(metadata_yaml)
            subcat_args["name"] = metadata["name"]
            subcat_args["description"] = metadata["description"]
            subcat_args["metadata"] = metadata

            if translator:
                subcat_args["translator"] = getattr(metacat.translators, translator)

            args.append((manager, subcat_args | kwargs))

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
            utils.validate_against_schema(args["metadata"], metacat.schema)
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
        default="metacatalog.csv",
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
