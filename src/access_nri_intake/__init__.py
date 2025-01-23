# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from . import _version

__version__ = _version.get_versions()["version"]

CATALOG_LOCATION = "/g/data/xp65/public/apps/access-nri-intake-catalog/catalog.yaml"
USER_CATALOG_LOCATION = str(Path.home() / ".access_nri_intake_catalog/catalog.yaml")
