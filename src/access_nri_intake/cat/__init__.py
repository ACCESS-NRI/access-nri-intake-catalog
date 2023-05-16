# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import os

import intake

from .. import __version__

_here = os.path.abspath(os.path.dirname(__file__))
_cat = intake.open_catalog(os.path.join(_here, "metacatalog.yaml"))(
    version_=f"v{__version__}"
)
data = _cat.access_nri
