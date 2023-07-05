# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import intake
import pandas as pd
import pytest

from access_nri_intake.catalog import CORE_COLUMNS, TRANSLATOR_GROUPBY_COLUMNS
from access_nri_intake.catalog.translators import (
    Cmip5Translator,
    Cmip6Translator,
    DefaultTranslator,
    EraiTranslator,
    TranslatorError,
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


@pytest.mark.parametrize("name", ["name", None])
@pytest.mark.parametrize("description", ["description", None])
@pytest.mark.parametrize("something", ["something", None])
def test_DefaultTranslator(test_data, name, description, something):
    """Test the various steps of the DefaultTranslator"""
    esmds = intake.open_esm_datastore(test_data / "esm_datastore/cmip5-al33.json")
    esmds.name = name
    esmds.description = description
    esmds.metadata = dict(something=something)
    columns = ["name", "description", "something", "variable", "version"]

    df = DefaultTranslator(esmds, columns).translate()
    assert all(df["name"].to_numpy() == name)
    assert all(df["description"].to_numpy() == description)
    assert all(df["something"].to_numpy() == something)
    assert all(df["variable"].map(type) == tuple)
    assert all(df["version"].str.startswith("v"))

    if name:
        with pytest.raises(TranslatorError) as excinfo:
            DefaultTranslator(esmds, columns).translate(["name"])
        assert "Column 'version' contains multiple values" in str(excinfo.value)

        columns.remove("version")
        df = DefaultTranslator(esmds, columns).translate(["name"])
        assert len(df) == 1


def test_DefaultTranslator_list2tuple(test_data):
    """Test that DefaultTranslator casts lists in .metadata to tuples"""
    esmds = intake.open_esm_datastore(test_data / "esm_datastore/cmip5-al33.json")
    esmds.metadata = dict(
        alist=[
            0,
        ]
    )
    df = DefaultTranslator(esmds, ["alist"]).translate()
    assert all(df["alist"] == (0,))


def test_DefaultTranslator_error(test_data):
    """Test error is raised when column is unavailable"""
    esmds = intake.open_esm_datastore(test_data / "esm_datastore/cmip5-al33.json")
    with pytest.raises(TranslatorError) as excinfo:
        DefaultTranslator(esmds, ["foo"]).translate()
    assert "Could not translate" in str(excinfo.value)


@pytest.mark.parametrize(
    "groupby, n_entries",
    [
        (None, 5),
        (TRANSLATOR_GROUPBY_COLUMNS, 5),
        (["realm"], 3),
        (["frequency"], 2),
        (["name"], 1),
    ],
)
def test_Cmip5Translator(test_data, groupby, n_entries):
    "Test CMIP5 datastore translator" ""
    esmds = intake.open_esm_datastore(test_data / "esm_datastore/cmip5-al33.json")
    esmds.name = "name"
    esmds.description = "description"
    df = Cmip5Translator(esmds, CORE_COLUMNS).translate(groupby)
    assert len(df) == n_entries


@pytest.mark.parametrize(
    "groupby, n_entries",
    [
        (None, 5),
        (TRANSLATOR_GROUPBY_COLUMNS, 5),
        (["variable"], 4),
        (["realm"], 2),
        (["frequency"], 2),
        (["description"], 1),
    ],
)
def test_Cmip6Translator(test_data, groupby, n_entries):
    "Test CMIP6 datastore translator" ""
    esmds = intake.open_esm_datastore(test_data / "esm_datastore/cmip6-oi10.json")
    esmds.name = "name"
    esmds.description = "description"
    df = Cmip6Translator(esmds, CORE_COLUMNS).translate(groupby)
    assert len(df) == n_entries


@pytest.mark.parametrize(
    "groupby, n_entries",
    [
        (None, 5),
        (TRANSLATOR_GROUPBY_COLUMNS, 4),
        (["variable"], 5),
        (["realm"], 2),
        (["frequency"], 3),
        (["description"], 1),
    ],
)
def test_EraiTranslator(test_data, groupby, n_entries):
    "Test ERA-Interim datastore translator" ""
    model = ("ERA-Interim",)
    esmds = intake.open_esm_datastore(test_data / "esm_datastore/erai.json")
    esmds.name = "name"
    esmds.description = "description"
    esmds.metadata = dict(model=model)
    df = EraiTranslator(esmds, CORE_COLUMNS).translate(groupby)
    assert all(df["model"] == model)
    assert len(df) == n_entries
