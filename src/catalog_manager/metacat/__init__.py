# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import os
import json

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, "schema.json"), "r") as fpath:
    schema = json.load(fpath)

from .core import CatalogManager
