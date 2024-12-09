# Copyright 2024 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import os
import re

import yaml

from ..utils import get_catalog_fp
from . import CATALOG_NAME_FORMAT

CATALOG_PATH_REGEX = r"^(?P<rootpath>.*?)\{\{version\}\}.*?$"


def _get_catalog_rp():
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
        return match.group("rootpath")
    except AttributeError:  # Match failed
        raise RuntimeError(
            f"Catalog metadata {get_catalog_fp()} contains unexpected catalog filepath: {catalog_fp}"
        )


def available_versions(pretty: bool = True):
    """
    Report the available versions of the `intake.cat.access_nri` catalog.

    Parameters
    ---------
    pretty : bool, optional
        Defines whether to return a pretty print-out of the available versions
        (True, default), or to provide a list of version numbers only (False).
    """
    # Work out where the catalogs are stored
    base_path = _get_catalog_rp()

    # Grab all the catalog names
    cats = [d for d in os.listdir(base_path) if re.search(CATALOG_NAME_FORMAT, d)]
    cats.sort(reverse=True)

    # Find all the symlinked versions
    symlinks = [s for s in cats if os.path.islink(os.path.join(base_path, s))]

    symlink_targets = {
        s: os.path.basename(os.readlink(os.path.join(base_path, s))) for s in symlinks
    }

    if pretty:
        for i, c in enumerate(cats):
            if c in symlink_targets.keys():
                c += f"(-->{symlink_targets[c]})"
            print(c)
        return

    return cats
