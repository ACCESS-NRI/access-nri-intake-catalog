# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Tests for aliasing functionality"""

import json
import re
from unittest import mock
from unittest.mock import MagicMock
from pydantic_core import ValidationError

from intake_esm.core import esm_datastore
import intake
from typing import Any
import pytest
from intake_dataframe_catalog.core import DfFileCatalog

from access_nri_intake.aliases import (
    _CMIP_TO_ACCESS_MAPPINGS,
    DATAFRAME_FIELD_ALIASES,
    ESM_FIELD_ALIASES,
    VALUE_ALIASES,
    AliasedESMCatalog,
    AliasedDataframeCatalog,
)


class SpyEsmDatastore(esm_datastore):
    """Real ESMDataStore that records search calls across chains."""

    def __init__(self, *args, search_calls: list[Any] | None = None, **kwargs):
        super().__init__(*args, columns_with_iterables=["variable"], **kwargs)
        self.search_calls = search_calls or []
        self._captured_init_kwargs = {"metadata": {"version": "test"}}

    def search(self, require_all_on: str | list[str] | None = None, **kwargs):
        self.search_calls.append(kwargs)

        result = super().search(require_all_on, **kwargs)
        if isinstance(result, esm_datastore):
            result = self.__class__(
                result.esmcat,
                search_calls=self.search_calls,
            )

        return result

    def to_dask(self, **kwargs):
        return None


class TestAliasedESMCatalog:
    """Test the AliasedESMCatalog wrapper"""

    @pytest.mark.parametrize("show_warnings", [True, False])
    def test_no_field_aliases_for_esm(self, sample_datastore, show_warnings):
        """Test that ESM datastores use native field names (no field aliasing)"""
        mock_cat = SpyEsmDatastore(sample_datastore)
        wrapped_cat = AliasedESMCatalog(
            mock_cat,
            field_aliases=ESM_FIELD_ALIASES,
            value_aliases=VALUE_ALIASES,
            show_warnings=show_warnings,
        )

        assert "tos" not in mock_cat.unique().variable
        assert "tos" not in wrapped_cat.unique().variable

        # Test that field names pass through unchanged - ESM datastores use native names
        wrapped_cat.search(variable="tos")  # Use native ESM field name

        # Should have been called with the same field name (no field mapping)
        assert len(mock_cat.search_calls) == 1
        call_kwargs = mock_cat.search_calls[0]
        assert "variable" in call_kwargs  # Field name unchanged
        assert call_kwargs["variable"] == [
            "surface_temp",
            "tos",
        ]  # But value should be aliased (tos -> surface_temp)

    @pytest.mark.parametrize("show_warnings", [True, False])
    def test_value_aliases(self, sample_datastore, show_warnings):
        """Test that values are properly aliased"""
        mock_cat = SpyEsmDatastore(sample_datastore)
        wrapped_cat = AliasedESMCatalog(
            mock_cat,
            field_aliases=ESM_FIELD_ALIASES,
            value_aliases=VALUE_ALIASES,
            show_warnings=show_warnings,
        )

        # Test value alias mapping - using CMIP variable that maps to ACCESS variable
        wrapped_cat.search(
            variable="tas"
        )  # CMIP variable should map to ACCESS "fld_s03i236"

        # Should have been called with canonical values
        assert len(mock_cat.search_calls) == 1
        call_kwargs = mock_cat.search_calls[0]
        assert call_kwargs["variable"] == ["fld_s03i236", "tas"]

    @pytest.mark.parametrize("show_warnings", [True, False])
    def test_field_aliases(self, sample_datastore, show_warnings):
        """Test that fields are properly aliased"""
        mock_cat = SpyEsmDatastore(sample_datastore)
        wrapped_cat = AliasedESMCatalog(
            mock_cat,
            field_aliases={"varname": "variable"},
            value_aliases=VALUE_ALIASES,
            show_warnings=show_warnings,
        )

        # Test field alias mapping - using CMIP field names that should map to canonical names
        # Since ESM_FIELD_ALIASES is empty in the actual code, this tests the passthrough behavior
        wrapped_cat.search(
            varname="tas"
        )  # Should pass through as-is since no field aliases are defined for ESM

        # Should have been called with the same field name (no aliasing for ESM catalogs)
        assert len(mock_cat.search_calls) == 1
        call_kwargs = mock_cat.search_calls[0]
        assert "variable" in call_kwargs
        assert call_kwargs["variable"] == [
            "fld_s03i236",
            "tas",
        ]  # Value should still be aliased

    def test_combined_aliases(self, sample_datastore):
        """Test that field and value aliases work together"""
        mock_cat = SpyEsmDatastore(sample_datastore)
        wrapped_cat = AliasedESMCatalog(
            mock_cat, field_aliases=ESM_FIELD_ALIASES, value_aliases=VALUE_ALIASES
        )

        # Test combined field and value aliases - using CMIP variable names and frequency aliases
        wrapped_cat.search(variable="ci", frequency="daily")

        # Should have been called with all canonical names and values
        assert len(mock_cat.search_calls) == 1
        call_kwargs = mock_cat.search_calls[0]
        assert call_kwargs["variable"] == [
            "fld_s05i269",
            "ci",
        ]  # ci -> fld_s05i269 (CMIP to ACCESS mapping)
        assert call_kwargs["frequency"] == ["1day", "daily"]  # daily -> 1day

    @pytest.mark.parametrize("show_warnings", [True, False])
    def test_list_values(self, sample_datastore, show_warnings):
        """Test that lists of values are properly aliased"""
        mock_cat = SpyEsmDatastore(sample_datastore)
        wrapped_cat = AliasedESMCatalog(
            mock_cat, field_aliases=ESM_FIELD_ALIASES, value_aliases=VALUE_ALIASES
        )

        # Test list of values - using CMIP variable names that are in our mappings
        wrapped_cat.search(variable=["ci", "cl", "tas"])

        # Should have been called with aliased list
        assert len(mock_cat.search_calls) == 1
        call_kwargs = mock_cat.search_calls[0]
        expected_values = [
            "tas",
            "fld_s02i261",
            "fld_s05i269",
            "ci",
            "fld_s03i236",
            "cl",
        ]  # ci->fld_s05i269, cl->fld_s02i261, tas->fld_s03i236
        assert set(call_kwargs["variable"]) == set(expected_values)

        if show_warnings:
            # Should have issued a warning about value aliasing - one, and no nested
            # list nonsense
            with pytest.warns(UserWarning) as warning_record:
                wrapped_cat.search(variable=["ci", "cl", "tas"])
            assert len(warning_record) == 1

    def test_regex_values(self, sample_datastore):
        """Test that a regex value is passed through untouched"""
        mock_cat = SpyEsmDatastore(sample_datastore)
        wrapped_cat = AliasedESMCatalog(
            mock_cat, field_aliases=ESM_FIELD_ALIASES, value_aliases=VALUE_ALIASES
        )

        # Test list of values - using CMIP variable names that are in our mappings. The lsit is passed
        # through as a chain of regex 'OR'
        wrapped_cat.search(variable="ci|cl|tas")

        # Should *not* have been called with aliased list - it's a regex
        assert len(mock_cat.search_calls) == 1
        call_kwargs = mock_cat.search_calls[0]
        expected_values = [
            "fld_s05i269",
            "fld_s02i261",
            "fld_s03i236",
        ]  # ci->fld_s05i269, cl->fld_s02i261, tas->fld_s03i236
        assert call_kwargs["variable"] != expected_values
        assert call_kwargs["variable"] == "ci|cl|tas"

    def test_passthrough_unknown_fields(self, sample_datastore):
        """Test that unknown fields pass through unchanged"""
        mock_cat = SpyEsmDatastore(sample_datastore)
        wrapped_cat = AliasedESMCatalog(
            mock_cat, field_aliases=ESM_FIELD_ALIASES, value_aliases=VALUE_ALIASES
        )

        # Test unknown field
        with pytest.raises(
            ValidationError, match="Column unknown_field not in columns"
        ):
            wrapped_cat.search(unknown_field="unknown_value")

    def test_passthrough_unknown_values(self, sample_datastore):
        """Test that unknown values pass through unchanged"""
        mock_cat = SpyEsmDatastore(sample_datastore)
        wrapped_cat = AliasedESMCatalog(
            mock_cat, field_aliases=ESM_FIELD_ALIASES, value_aliases=VALUE_ALIASES
        )

        # Test unknown value for known field
        wrapped_cat.search(variable="unknown_variable")

        # Should pass through with canonical field name but original value
        assert len(mock_cat.search_calls) == 1
        call_kwargs = mock_cat.search_calls[0]
        assert call_kwargs["variable"] == "unknown_variable"

    def test_passthrough_attributes(self, sample_datastore):
        """Test that other attributes pass through to the wrapped catalog"""
        mock_cat = SpyEsmDatastore(sample_datastore)
        wrapped_cat = AliasedESMCatalog(
            mock_cat, field_aliases=ESM_FIELD_ALIASES, value_aliases=VALUE_ALIASES
        )

        # Test accessing attributes that aren't search
        assert wrapped_cat._captured_init_kwargs == mock_cat._captured_init_kwargs

    def test_dir_includes_wrapper_and_wrapped_attrs(self, sample_datastore):
        """Test that dir() includes both wrapper and wrapped catalog attributes"""
        mock_cat = SpyEsmDatastore(sample_datastore)
        wrapped_cat = AliasedESMCatalog(
            mock_cat, field_aliases=ESM_FIELD_ALIASES, value_aliases=VALUE_ALIASES
        )

        # Test __dir__ method
        dir_attrs = dir(wrapped_cat)
        assert "search" in dir_attrs
        assert "_captured_init_kwargs" in dir_attrs
        assert "field_aliases" in dir_attrs
        assert "value_aliases" in dir_attrs

    def test_cmip_mappings_loaded(self):
        """Test that ACCESS to CMIP mappings are loaded and available"""
        # Check that some mappings were loaded (assuming the JSON file is available)
        if _CMIP_TO_ACCESS_MAPPINGS:
            # Test a specific mapping we know should exist
            assert "ci" in _CMIP_TO_ACCESS_MAPPINGS
            assert _CMIP_TO_ACCESS_MAPPINGS["ci"] == "fld_s05i269"

        # Test that CMIP variable aliases are included in VALUE_ALIASES
        variable_aliases = VALUE_ALIASES.get("variable", {})
        if _CMIP_TO_ACCESS_MAPPINGS:
            # Test specific CMIP variables that should be preserved
            test_vars = [
                "ci",
                "rldscs",
                "rluscs",
            ]  # Should map to fld_s05i269, fld_s02i208, fld_s02i206
            for cmip_var in test_vars:
                if cmip_var in _CMIP_TO_ACCESS_MAPPINGS:
                    assert cmip_var in variable_aliases
                    assert (
                        variable_aliases[cmip_var] == _CMIP_TO_ACCESS_MAPPINGS[cmip_var]
                    )

    @pytest.mark.parametrize("show_warnings", [True, False])
    @pytest.mark.parametrize(
        "search, sent", [("ci", ["fld_s05i269", "ci"]), ("ci.*", "ci.*")]
    )
    def test_cmip_variable_search(self, show_warnings, search, sent, sample_datastore):
        """Test that CMIP variables can be found and return ACCESS variables"""
        mock_cat = SpyEsmDatastore(sample_datastore)
        wrapped_cat = AliasedESMCatalog(
            mock_cat,
            field_aliases=ESM_FIELD_ALIASES,
            value_aliases=VALUE_ALIASES,
            show_warnings=show_warnings,
        )

        # Test CMIP variable search (if mappings are available)
        if _CMIP_TO_ACCESS_MAPPINGS and "ci" in _CMIP_TO_ACCESS_MAPPINGS:
            wrapped_cat.search(variable=search)

            # Should have been called with the ACCESS model variable name
            assert len(mock_cat.search_calls) == 1
            call_kwargs = mock_cat.search_calls[0]
            assert call_kwargs["variable"] == sent

    def test_unwrap(self, sample_datastore):
        """Test that unwrap() returns the original catalog"""
        wrapped_cat = AliasedESMCatalog(
            sample_datastore,
            field_aliases=ESM_FIELD_ALIASES,
            value_aliases=VALUE_ALIASES,
        )

        unwrapped = wrapped_cat.unwrap()
        assert unwrapped is sample_datastore


class TestAliasedDataframeCatalog:
    """Test the AliasedDataframeCatalog wrapper"""

    @pytest.fixture
    def tmp_dataframe_catalog(self, tmp_path, test_data) -> DfFileCatalog:
        """

        Use a temporary, *real* dataframe catalog instead of a mock one, because there's too much wacky
        intake behaviour we can't easily mock that we need to test.

        stolen from tests/test_manager.py/test_CatalogManager_all
        """

        from access_nri_intake.catalog.manager import CatalogManager
        from access_nri_intake.catalog.translators import Cmip5Translator
        from access_nri_intake.source.builders import AccessOm2Builder, AccessOm3Builder
        from access_nri_intake.utils import load_metadata_yaml
        from access_nri_intake.catalog import EXP_JSONSCHEMA

        path = str(tmp_path / "cat.parquet")
        cat = CatalogManager(path)

        # Load source
        load_args = dict(
            name="cmip5-al33",
            description="cmip5-al33",
            path=str(test_data / "esm_datastore/cmip5-al33.json"),
            translator=Cmip5Translator,
        )
        cat.load(
            **load_args,  # type: ignore
        )

        # Build a couple of sources
        models = {"access-om2": AccessOm2Builder, "access-om3": AccessOm3Builder}
        for model, builder in models.items():
            metadata = load_metadata_yaml(
                str(test_data / model / "metadata.yaml"), EXP_JSONSCHEMA
            )
            cat.build_esm(
                name=model,
                description=model,
                builder=builder,
                path=str(test_data / model),
                metadata=metadata,
                directory=str(tmp_path),
            )

        # Check that entry with same name overwrites correctly
        cat.load(
            **load_args,  # type: ignore
        )
        cat.save()
        return intake.open_df_catalog(path)  # type: ignore

    def test_to_source_wraps_esm_datastore(self, tmp_dataframe_catalog):
        """Test that to_source() properly wraps ESM datastores with aliasing"""

        catalog = AliasedDataframeCatalog(
            tmp_dataframe_catalog,
            field_aliases=DATAFRAME_FIELD_ALIASES,
            value_aliases=VALUE_ALIASES,
        )

        subcat = catalog.search(model="ACCESS-OM2")
        # Since search() returns the result directly (not wrapped), we need to call to_source on the catalog
        esm_datastore = subcat.to_source()

        assert isinstance(esm_datastore, AliasedESMCatalog)

    def test___getitem___wraps_esm_datastore(self, tmp_dataframe_catalog):
        """Test that __getitem__() properly wraps ESM datastores with aliasing"""

        catalog = AliasedDataframeCatalog(
            tmp_dataframe_catalog,
            field_aliases=DATAFRAME_FIELD_ALIASES,
            value_aliases=VALUE_ALIASES,
        )

        subcat = catalog.search(model="ACCESS-OM2")
        assert isinstance(subcat, AliasedDataframeCatalog)

        # Since search() returns the result directly (not wrapped), we need to call to_source on the catalog
        esm_datastore = subcat["access-om2"]

        assert isinstance(esm_datastore, AliasedESMCatalog)

    def test_to_source_dict_wraps_multiple_datastores(self, tmp_dataframe_catalog):
        """Test that to_source_dict() wraps all returned datastores"""

        catalog = AliasedDataframeCatalog(
            tmp_dataframe_catalog,
            field_aliases=DATAFRAME_FIELD_ALIASES,
            value_aliases=VALUE_ALIASES,
        )

        # Test to_source_dict() workflow
        datastores_dict = catalog.to_source_dict()

        assert len(datastores_dict) == 3

        for val in datastores_dict.values():
            assert isinstance(val, AliasedESMCatalog)

    def test_getattr_passthrough(self, tmp_dataframe_catalog):
        """Test that __getattr__ passes through to underlying catalog"""

        # Mock an attribute
        mock_attr = MagicMock()
        tmp_dataframe_catalog.some_attribute = mock_attr

        catalog = AliasedDataframeCatalog(tmp_dataframe_catalog)
        result = catalog.some_attribute

        # Should return the raw attribute, not wrapped
        assert result is mock_attr

    def test_search_wraps_result(self, tmp_dataframe_catalog):
        """Test that search() wraps results that have to_source method"""

        catalog = AliasedDataframeCatalog(
            tmp_dataframe_catalog,
            field_aliases=DATAFRAME_FIELD_ALIASES,
            value_aliases=VALUE_ALIASES,
        )

        subcat = catalog.search(name="access-om2")

        assert isinstance(subcat, AliasedDataframeCatalog)

        esm_datastore = subcat.to_source()

        assert isinstance(esm_datastore, AliasedESMCatalog)

    def test_search_normalises_values(self, tmp_dataframe_catalog):
        """Test that search() normalizes values using value aliases"""

        catalog = AliasedDataframeCatalog(
            tmp_dataframe_catalog,
            field_aliases=DATAFRAME_FIELD_ALIASES,
            value_aliases=VALUE_ALIASES,
        )

        subcat = catalog.search(
            variable="bigthetao"
        )  # CMIP variable should map to ACCESS "temp"

        assert len(subcat._df) == 1  # Just the om2 dataset
        assert subcat.unique().variable == ["temp"]
        assert subcat.unique().model == ["ACCESS-OM2"]

    def test_search_normalises_list_values(self, tmp_dataframe_catalog):
        """Test that search() normalizes values using value aliases"""

        catalog = AliasedDataframeCatalog(
            tmp_dataframe_catalog,
            field_aliases=DATAFRAME_FIELD_ALIASES,
            value_aliases=VALUE_ALIASES,
        )

        with pytest.warns(
            UserWarning,
            match=re.escape(
                "Value aliasing: variable='bigthetao' → variable=['temp','bigthetao']"
            ),
        ):
            subcat = catalog.search(
                variable=["bigthetao"]
            )  # CMIP variable should map to ACCESS "temp"

        assert len(subcat._df) == 1  # Just the om2 dataset
        assert subcat.unique().variable == ["temp"]
        assert subcat.unique().model == ["ACCESS-OM2"]

    def test_unwrap(self, tmp_dataframe_catalog):
        """Test that unwrap() returns the original catalog"""

        catalog = AliasedDataframeCatalog(tmp_dataframe_catalog)
        unwrapped = catalog.unwrap()

        assert unwrapped is tmp_dataframe_catalog


@pytest.mark.parametrize(
    "mock_target,side_effect",
    [
        ("access_nri_intake.aliases.rsr.files", FileNotFoundError()),
        ("access_nri_intake.aliases.json.load", json.JSONDecodeError("", "", 0)),
        ("access_nri_intake.aliases.json.load", KeyError("test")),
    ],
)
def test_load_cmip_mappings_errors(mock_target, side_effect):
    from access_nri_intake.aliases import _load_cmip_mappings

    with mock.patch(mock_target, side_effect=side_effect):
        cmip_to_access = _load_cmip_mappings()

    assert isinstance(cmip_to_access, dict)
    assert len(cmip_to_access) == 0
