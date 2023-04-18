# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import os

import intake

from . import _version

__version__ = _version.get_versions()["version"]

_here = os.path.abspath(os.path.dirname(__file__))
cat = intake.open_catalog(os.path.join(_here, "dfcatalog.yaml"))
data = cat.accessnri()
