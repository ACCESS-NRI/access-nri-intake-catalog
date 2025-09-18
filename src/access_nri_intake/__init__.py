# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from . import _version

__version__ = _version.get_versions()["version"]

CATALOG_LOCATION = "/g/data/xp65/public/apps/access-nri-intake-catalog/catalog.yaml"
"""Location for 'live' master catalog YAML."""

USER_CATALOG_LOCATION = str(Path.home() / ".access_nri_intake_catalog/catalog.yaml")
"""Location where user can place a master catalog YAML to override standard 'live' version."""

STORAGE_ROOT = "/g/data"
"""Root storage location for catalog experiments"""

STORAGE_FLAG_PATTERN = r"gdata/[a-z]{1,2}[0-9]{1,2}"
"""Pattern for matching 'storage flags' - related to Gadi file access system"""

STORAGE_LOCATION_REGEX = r"^/g/data/(?P<proj>[a-z]{1,2}[0-9]{1,2})/.*?$"
"""Regular expression for matching the file path to experiments, and extracting a project ID"""
