# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import glob
import os
import shutil
from pathlib import Path, PosixPath
from unittest import mock
from frozendict import frozendict
import copy
from frozendict import frozendict
import copy

import intake
import pytest
import yaml

import access_nri_intake
from access_nri_intake.cloud import CatalogMirror


def test_entrypoint():
    """
    Check that entry point works
    """
    exit_status = os.system("mirror-to-cloud --help")
    assert exit_status == 0
