import argparse
import logging

import yaml

from catalog_manager import esmcat, metacat


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
        "config",
        type=str,
        help="Configuration YAML file specifying the intake catalog(s) to add",
    )
    parser.add_argument(
        "--catalog_name",
        type=str,
        default="dfcatalog.csv",
        help="The path to the intake-dataframe-catalog",
    )

    args = parser.parse_args()
    config = args.config
    catalog_name = args.catalog_name

    with open(config) as f:
        config = yaml.safe_load(f)

    builder = config.get("builder")
    translator = config.get("translator")
    subcatalog_dir = config.get("subcatalog_dir")
    subcatalogs = config.get("subcatalogs")

    args = {}
    if builder:
        msg = "Building intake-esm catalog"
        manager = metacat.MetacatManager(path=catalog_name).build_esm
        args["builder"] = getattr(esmcat, builder)
        args["directory"] = subcatalog_dir
        args["overwrite"] = True
    else:
        msg = "Loading intake catalog"
        manager = metacat.MetacatManager(path=catalog_name).load

    for kwargs in subcatalogs:
        cat_args = args

        cat_args["path"] = kwargs.pop("path")
        metadata_yaml = kwargs.pop("metadata_yaml")
        with open(metadata_yaml) as f:
            metadata = yaml.safe_load(f)
        cat_args["name"] = metadata.pop("name")
        # cat_args["uuid"] = metadata.pop("uuid")
        cat_args["description"] = metadata.pop("short_description")
        cat_args["metadata"] = metadata

        if translator:
            cat_args["translator"] = getattr(metacat.translators, translator)

        logger.info(
            f"{msg} '{cat_args['name']}' and adding to intake-dataframe-catalog '{catalog_name}'"
        )
        manager(**cat_args).add()
