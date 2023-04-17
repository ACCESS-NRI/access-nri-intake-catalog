# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Tools for managing intake-dataframe-catalog metacatalogs """

import os
import yaml

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, "schema.yaml"), "r") as fpath:
    schema = yaml.safe_load(fpath)

from .manager import MetacatManager

from .translators import (
    DefaultTranslator,
    Cmip6Translator,
    Cmip5Translator,
    EraiTranslator,
)
