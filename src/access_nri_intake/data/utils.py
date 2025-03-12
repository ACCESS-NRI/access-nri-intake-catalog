# Copyright 2024 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import re
import warnings
from pathlib import Path

import yaml

from ..utils import get_catalog_fp
from . import CATALOG_NAME_FORMAT

CATALOG_PATH_REGEX = r"^(?P<rootpath>.*?)\{\{version\}\}.*?$"


def _get_catalog_root():
    """
    Get the catalog root path.
    """
    with open(get_catalog_fp()) as fo:
        catalog_metadata = yaml.load(fo, yaml.FullLoader)

    try:
        catalog_fp = catalog_metadata["sources"]["access_nri"]["args"]["path"]
    except KeyError:
        raise RuntimeError(
            f"Catalog metadata {get_catalog_fp()} does not match expected format."
        )

    match = re.match(CATALOG_PATH_REGEX, catalog_fp)
    try:
        return Path(match.group("rootpath"))
    except AttributeError:  # Match failed
        raise RuntimeError(
            f"Catalog metadata {get_catalog_fp()} contains unexpected catalog filepath: {catalog_fp}"
        )


def available_versions(pretty: bool = True) -> list[str] | None:
    """
    Report the available versions of the `intake.cat.access_nri` catalog.

    Parameters
    ---------
    pretty : bool, optional
        Defines whether to return a pretty print-out of the available versions
        (True, default), or to provide a list of version numbers only (False).

    Returns
    -------
    none | list[str]
        If `pretty==True`, the available catalogs are printed to output, and
        the function returns None. If `pretty==False`, a list of available catalogs
        is returned.

        Additionally, if `pretty==True` only, expired catalogs are also printed to
        output. These catalogs require the user to place the necessary catalog
        file in their home directory.
    """
    # Work out where the catalogs are stored
    base_path = _get_catalog_root()

    # Grab the extant catalog and work out its min and max versions
    try:
        with open(get_catalog_fp()) as cat_file:
            cat_yaml = yaml.safe_load(cat_file)
            vers_min = cat_yaml["sources"]["access_nri"]["parameters"]["version"]["min"]
            vers_max = cat_yaml["sources"]["access_nri"]["parameters"]["version"]["max"]
            vers_def = cat_yaml["sources"]["access_nri"]["parameters"]["version"][
                "default"
            ]
    except FileNotFoundError:
        raise FileNotFoundError(f"Unable to find catalog at {get_catalog_fp()}")
    except KeyError:
        raise RuntimeError(f"Catalog at {get_catalog_fp()} not correctly formatted")

    # Grab all the catalog names
    cats_all = [
        dir_path.name
        for dir_path in base_path.iterdir()
        if re.search(CATALOG_NAME_FORMAT, dir_path.name) and dir_path.is_dir()
    ]
    cats_all.sort(reverse=True)

    # Extract the directory names for the 'live' catalog
    cats = [
        cat
        for cat in cats_all
        if (cat >= vers_min and cat <= vers_max) or cat == vers_def
    ]

    # Find all the symlinked versions
    symlinks = [s for s in cats_all if (Path(base_path) / s).is_symlink()]

    symlink_targets = {s: (base_path / s).readlink().name for s in symlinks}

    if pretty:
        for c in cats:
            if c in symlink_targets.keys():
                c += f"(-->{symlink_targets[c]})"
            if c == vers_def:
                c += "*"
            print(c)

        # In pretty mode, we want to look for & return the catalogs that are referred to
        # by outdated catalog files (catalog-YYYYMMDD-YYYYMMDD)
        # Locate the outdated catalog files
        catalog_loc = Path(get_catalog_fp()).parent
        # Recall globbing gives relative paths only
        old_cats = catalog_loc.glob("catalog-*-*.yaml")

        for cat in old_cats:
            try:
                with open(catalog_loc / cat) as old_cat_file:
                    old_cat_yaml = yaml.safe_load(old_cat_file)
                    vers_min = old_cat_yaml["sources"]["access_nri"]["parameters"][
                        "version"
                    ]["min"]
                    vers_max = old_cat_yaml["sources"]["access_nri"]["parameters"][
                        "version"
                    ]["max"]
                    vers_def = old_cat_yaml["sources"]["access_nri"]["parameters"][
                        "version"
                    ]["default"]
            except FileNotFoundError:
                warnings.warn(
                    f"Unable to find old catalog file {cat.name} - continuing",
                    category=UserWarning,
                )
                continue
            except KeyError:
                warnings.warn(
                    f"Old catalog file {cat.name} is improperly formatted - continuing",
                    category=UserWarning,
                )
                continue

            # Work out which catalogs are related to this yaml
            cats_this_yaml = [
                cat
                for cat in cats_all
                if (cat >= vers_min and cat <= vers_max) or cat == vers_def
            ]

            print("")
            print(f"Deprecated catalog {cat.name}:")
            for vers in cats_this_yaml:
                if vers == vers_def:
                    print(f"{vers}*")
                else:
                    print(vers)

        return None

    return cats
