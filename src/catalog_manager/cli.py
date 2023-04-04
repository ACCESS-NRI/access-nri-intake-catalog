import argparse
import logging

import yaml

from catalog_manager import esmcat, dfcat, translators


def main():
    """
    Add intake-esm catalogs specified in a config file to the ACCESS-NRI intake-dataframe-catalog
    """

    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(
        description="Add intake-esm catalogs specified in a config file to the ACCESS-NRI intake-dataframe-catalog"
    )
    parser.add_argument(
        "config",
        type=str,
        help="Configuration YAML file specifying the intake-esm catalog(s) to add",
    )
    parser.add_argument(
        "--catalog_dir",
        type=str,
        default="./",
        help="The directory to save the intake-dataframe-catalog to",
    )
    parser.add_argument(
        "--catalog_name",
        type=str,
        default="dfcatalog",
        help="The name of the intake-dataframe-catalog",
    )

    args = parser.parse_args()
    config = args.config
    catalog_dir = args.catalog_dir
    catalog_name = args.catalog_name

    with open(config) as f:
        config = yaml.safe_load(f)

    builder = config.get("builder")
    translator = config.get("translator")
    metadata = config.get("metadata") or {}
    subcatalog_dir = config.get("subcatalog_dir")
    catalogs = config.get("catalogs")

    args = {"metadata": metadata}
    if builder:
        msg = "Building intake-esm catalog"
        cat_generator = dfcat.CatalogManager.build_esm
        args["builder"] = getattr(esmcat, builder)
        args["directory"] = subcatalog_dir
        args["overwrite"] = True
    else:
        msg = "Loading intake-esm catalog"
        cat_generator = dfcat.CatalogManager.load_esm

    for name, kwargs in catalogs.items():
        cat_args = args
        cat_args["name"] = name
        cat_args["description"] = kwargs.pop("description")
        cat_args["path"] = kwargs.pop("path")

        if translator:
            cat_args["translator"] = getattr(translators, translator)

        logger.info(
            f"{msg} '{name}' and adding to intake-dataframe-catalog '{catalog_name}'"
        )
        cat_generator(**cat_args).add(name=catalog_name, directory=catalog_dir)
