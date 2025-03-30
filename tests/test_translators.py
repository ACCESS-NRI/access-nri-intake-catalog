# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import intake
import pandas as pd
import pytest

from access_nri_intake.catalog import CORE_COLUMNS, TRANSLATOR_GROUPBY_COLUMNS
from access_nri_intake.catalog.translators import (
    FREQUENCY_TRANSLATIONS,
    BarpaTranslator,
    CcamTranslator,
    Cmip5Translator,
    Cmip6Translator,
    CordexTranslator,
    DefaultTranslator,
    Era5Translator,
    NarclimTranslator,
    TranslatorError,
    _cmip_realm_translator,
    _to_tuple,
    trace_failure,
    tuplify_series,
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
    translated = series.apply(lambda x: FREQUENCY_TRANSLATIONS.get(x, x))
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
    # Sort expected & translated to make the test less brittle
    translated = translated.apply(lambda x: tuple(sorted(x)))
    expected = [tuple(sorted(x)) for x in expected]
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
    assert all(_to_tuple(series).map(type) == tuple)  # noqa: E721


@pytest.mark.parametrize(
    "input_series, expected_output",
    [
        (pd.Series([1, 2, 3]), pd.Series([(1,), (2,), (3,)])),
    ],
)
def test_tuplify_series(input_series, expected_output):
    """Test the _tuplify_series function"""

    @tuplify_series
    def tuplify_func(series):
        return series

    class TestSeries:
        @tuplify_series
        def method(self, series):
            return series

    assert all(tuplify_func(input_series) == expected_output)
    assert all(TestSeries().method(input_series) == expected_output)


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
    assert all(df["variable"].map(type) == tuple)  # noqa: E721
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
    "colname, should_raise",
    [
        ("model", False),
        ("realm", False),
        ("frequency", False),
        ("variable", False),
        ("random_string", True),
    ],
)
def test_DefaultTranslator_set_dispatch(test_data, colname, should_raise):
    """Test that only valid translation setups are allowed"""
    esmds = intake.open_esm_datastore(test_data / "esm_datastore/cmip5-al33.json")
    dtrans = DefaultTranslator(esmds, CORE_COLUMNS)
    if should_raise:
        with pytest.raises(TranslatorError) as excinfo:
            dtrans.set_dispatch(colname, dtrans._model_translator, "model")
            assert "'core_colname' must be one of" in str(excinfo.value)
    else:
        dtrans.set_dispatch(colname, dtrans._model_translator, colname)
        assert dtrans._dispatch[colname] == dtrans._model_translator


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
    """Test CMIP5 datastore translator"""
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
    """Test CMIP6 datastore translator"""
    esmds = intake.open_esm_datastore(test_data / "esm_datastore/cmip6-oi10.json")
    esmds.name = "name"
    esmds.description = "description"
    df = Cmip6Translator(esmds, CORE_COLUMNS).translate(groupby)
    assert len(df) == n_entries


@pytest.mark.parametrize(
    "groupby, n_entries",
    [
        (None, 5),
        (["realm"], 1),
        (["variable"], 4),
        (["frequency"], 1),
    ],
)
def test_BarpaTranslator(test_data, groupby, n_entries):
    """Test BARPA datastore translator"""
    esmds = intake.open_esm_datastore(test_data / "esm_datastore/barpa-py18.json")
    esmds.name = "name"
    esmds.description = "description"
    df = BarpaTranslator(esmds, CORE_COLUMNS).translate(groupby)
    assert len(df) == n_entries


@pytest.mark.parametrize(
    "groupby, n_entries",
    [(None, 5), (["variable"], 4), (["frequency"], 2), (["realm"], 1)],
)
def test_CordexTranslator(test_data, groupby, n_entries):
    """Test CORDEX datastore translator"""
    esmds = intake.open_esm_datastore(test_data / "esm_datastore/cordex-ig45.json")
    esmds.name = "name"
    esmds.description = "description"
    df = CordexTranslator(esmds, CORE_COLUMNS).translate(groupby)
    assert len(df) == n_entries


@pytest.mark.parametrize(
    "groupby, n_entries",
    [
        (None, 5),
        (["variable"], 4),
        (["frequency"], 3),
        (["realm"], 1),
    ],
)
def test_Era5Translator(test_data, groupby, n_entries):
    """Test ERA5 datastore translator"""
    esmds = intake.open_esm_datastore(test_data / "esm_datastore/era5-rt52.json")
    esmds.name = "name"
    esmds.description = "description"
    df = Era5Translator(esmds, CORE_COLUMNS).translate(groupby)
    assert len(df) == n_entries


@pytest.mark.parametrize(
    "groupby, n_entries",
    [
        (None, 5),
        (["variable"], 4),
        (["frequency"], 3),
        (["model"], 2),
        (["realm"], 1),
    ],
)
def test_CcamTranslator(test_data, groupby, n_entries):
    """Test ERA5 datastore translator"""
    esmds = intake.open_esm_datastore(test_data / "esm_datastore/ccam-hq89.json")
    esmds.name = "name"
    esmds.description = "description"
    df = CcamTranslator(esmds, CORE_COLUMNS).translate(groupby)
    assert len(df) == n_entries


@pytest.mark.parametrize(
    "groupby, n_entries",
    [
        (None, 5),
        (["variable"], 4),
        (["frequency"], 3),
        (["model"], 2),
        (["realm"], 1),
    ],
)
def test_NarclimTranslator(test_data, groupby, n_entries):
    """Test ERA5 datastore translator"""
    esmds = intake.open_esm_datastore(test_data / "esm_datastore/narclim2-zz63.json")
    esmds.name = "name"
    esmds.description = "description"
    df = NarclimTranslator(esmds, CORE_COLUMNS).translate(groupby)
    assert len(df) == n_entries


def test_translator_failure(test_data):
    esmds = intake.open_esm_datastore(test_data / "esm_datastore/narclim2-zz63.json")
    esmds.name = "name"
    esmds.description = "description"
    translator = NarclimTranslator(esmds, CORE_COLUMNS)

    default = DefaultTranslator(esmds, CORE_COLUMNS)

    translator.set_dispatch(
        input_name="dud_name",
        core_colname="model",
        func=default._model_translator,
    )

    with pytest.raises(KeyError) as excinfo:
        translator.translate()

    assert (
        "Unable to translate 'model' column with translator 'DefaultTranslator'"
        in str(excinfo.value)
    )

    @trace_failure
    def _(x: int) -> int:
        return x

    with pytest.raises(TypeError) as excinfo:
        _(1)

    assert "Decorator can only be applied to translator class methods" in str(
        excinfo.value
    )
