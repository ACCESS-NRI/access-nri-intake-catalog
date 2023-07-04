# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import intake
import pandas as pd
import pytest

from access_nri_intake.catalog import CORE_COLUMNS
from access_nri_intake.catalog.translators import (
    Cmip5Translator,
    Cmip6Translator,
    _cmip_frequency_translator,
    _cmip_realm_translator,
    _to_tuple,
)


@pytest.mark.parametrize(
    "input, expected",
    [
        (
            [
                "1hr",
                "3hr",
                "3hrPt",
                "6hr",
                "6hrPt",
                "daily",
                "day",
                "fx",
                "mon",
                "monC",
                "monClim",
                "monPt",
                "sem",
                "subhr",
                "subhrPt",
                "yr",
                "yrPt",
            ],
            [
                "1hr",
                "3hr",
                "3hr",
                "6hr",
                "6hr",
                "1day",
                "1day",
                "fx",
                "1mon",
                "1mon",
                "1mon",
                "1mon",
                "3mon",
                "subhr",
                "subhr",
                "1yr",
                "1yr",
            ],
        ),
        (["daily"], ["1day"]),
    ],
)
def test_cmip_frequency_translator(input, expected):
    """Test translation of entries in the CMIP frequency column"""
    series = pd.Series(input)
    translated = _cmip_frequency_translator(series)
    assert list(translated) == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        (
            [
                "aerosol",
                "atmos",
                "atmos atmosChem",
                "atmos land",
                "land",
                "landIce",
                "landIce land",
                "landonly",
                "na",
                "ocean",
                "ocean seaIce",
                "ocnBgChem",
                "ocnBgchem",
                "seaIce",
                "seaIce ocean",
                "seaice",
            ],
            [
                ("aerosol",),
                ("atmos",),
                ("atmos", "atmosChem"),
                ("atmos", "land"),
                ("land",),
                ("landIce",),
                ("landIce", "land"),
                ("land",),
                ("none",),
                ("ocean",),
                ("ocean", "seaIce"),
                ("ocnBgchem",),
                ("ocnBgchem",),
                ("seaIce",),
                ("seaIce", "ocean"),
                ("seaIce",),
            ],
        ),
        (["landonly"], [("land",)]),
        (["atmos atmosChem"], [("atmos", "atmosChem")]),
    ],
)
def test_cmip_realm_translator(input, expected):
    """Test translation of entries in the CMIP realm column"""
    series = pd.Series(input)
    translated = _cmip_realm_translator(series)
    assert list(translated) == expected


@pytest.mark.parametrize(
    "input",
    [
        ["a", "b", "c"],
        [0, 1, 2],
    ],
)
def test_to_tuple(input):
    """Test the _to_tuple function"""
    series = pd.Series(input)
    assert all(_to_tuple(series).map(type) == tuple)


def test_Cmip5Translator(test_data):
    esmds = intake.open_esm_datastore(test_data / "esm_datastore/cmip5-al33.json")
    Cmip5Translator(esmds, CORE_COLUMNS).translate()


def test_Cmip6Translator(test_data):
    esmds = intake.open_esm_datastore(test_data / "esm_datastore/cmip6-oi10.json")
    Cmip6Translator(esmds, CORE_COLUMNS).translate()
