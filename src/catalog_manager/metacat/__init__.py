# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Tools for managing intake-dataframe-catalog metacatalogs """

import os

import yaml

_here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(_here, "schema.yaml")) as fpath:
    schema = yaml.safe_load(fpath)
