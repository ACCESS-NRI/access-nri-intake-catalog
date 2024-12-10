# Copyright 2024 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import re
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
    cats = [
        dir_path.name
        for dir_path in base_path.iterdir()
        if re.search(CATALOG_NAME_FORMAT, dir_path.name)
        and dir_path.is_dir()
        and (
            (dir_path.name >= vers_min and dir_path.name <= vers_max)
            or dir_path.name == vers_def
        )
    ]
    cats.sort(reverse=True)

    # Find all the symlinked versions
    symlinks = [s for s in cats if (Path(base_path) / s).is_symlink()]

    symlink_targets = {s: (base_path / s).readlink().name for s in symlinks}

    if pretty:
        for c in cats:
            if c in symlink_targets.keys():
                c += f"(-->{symlink_targets[c]})"
            if c == vers_def:
                c += "*"
            print(c)
        return None

    return cats
