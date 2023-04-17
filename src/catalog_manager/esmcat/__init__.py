# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Tools for generating intake-esm catalogs """

import os
import yaml

_here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(_here, "schema.yaml"), "r") as fpath:
    schema = yaml.safe_load(fpath)

from .builders import AccessOm2Builder, AccessEsm15Builder, AccessCm2Builder
