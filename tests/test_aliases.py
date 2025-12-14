# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Tests for aliasing functionality"""

import pytest
from unittest.mock import MagicMock

from access_nri_intake.aliases import AliasedESMCatalog, FIELD_ALIASES, VALUE_ALIASES, _CMIP_TO_ACCESS_MAPPINGS


class MockESMDatastore:
    """Mock ESM datastore for testing"""
    
    def __init__(self):
        self.search_calls = []
        self._captured_init_kwargs = {"metadata": {"version": "test"}}
    
    def search(self, **kwargs):
        """Record search calls for testing"""
        self.search_calls.append(kwargs)
        return self
        
    def to_dask(self):
        return None


class TestAliasedESMCatalog:
    """Test the AliasedESMCatalog wrapper"""
    
    def test_field_aliases(self):
        """Test that field names are properly aliased"""
        mock_cat = MockESMDatastore()
        wrapped_cat = AliasedESMCatalog(mock_cat, field_aliases=FIELD_ALIASES, value_aliases=VALUE_ALIASES)
        
        # Test field alias mapping - CMIP field names should map to ACCESS-NRI field names  
        wrapped_cat.search(variable_id="tas", source_id="ACCESS-CM2")
        
        # Should have been called with ACCESS-NRI catalog field names
        assert len(mock_cat.search_calls) == 1
        call_kwargs = mock_cat.search_calls[0]
        assert "variable" in call_kwargs    # variable_id -> variable
        assert "model" in call_kwargs       # source_id -> model
        assert call_kwargs["variable"] == "fld_s03i236"  # tas maps to ACCESS variable
        assert call_kwargs["model"] == "ACCESS-CM2"
        
    def test_value_aliases(self):
        """Test that values are properly aliased"""
        mock_cat = MockESMDatastore()
        wrapped_cat = AliasedESMCatalog(mock_cat, field_aliases=FIELD_ALIASES, value_aliases=VALUE_ALIASES)

        # Test value alias mapping - using CMIP variable that maps to ACCESS variable
        wrapped_cat.search(variable="tas")  # CMIP variable should map to ACCESS "fld_s03i236"

        # Should have been called with canonical values
        assert len(mock_cat.search_calls) == 1
        call_kwargs = mock_cat.search_calls[0]
        assert call_kwargs["variable"] == "fld_s03i236"

    def test_combined_aliases(self):
        """Test that field and value aliases work together"""
        mock_cat = MockESMDatastore()
        wrapped_cat = AliasedESMCatalog(mock_cat, field_aliases=FIELD_ALIASES, value_aliases=VALUE_ALIASES)
        
        # Test combined field and value aliases - using CMIP variable names and frequency aliases
        wrapped_cat.search(variable="ci", frequency="daily")
        
        # Should have been called with all canonical names and values
        assert len(mock_cat.search_calls) == 1
        call_kwargs = mock_cat.search_calls[0]
        assert call_kwargs["variable"] == "fld_s05i269"  # ci -> fld_s05i269 (CMIP to ACCESS mapping)
        assert call_kwargs["frequency"] == "1day"  # daily -> 1day
        
    def test_list_values(self):
        """Test that lists of values are properly aliased"""
        mock_cat = MockESMDatastore()
        wrapped_cat = AliasedESMCatalog(mock_cat, field_aliases=FIELD_ALIASES, value_aliases=VALUE_ALIASES)

        # Test list of values - using CMIP variable names that are in our mappings
        wrapped_cat.search(variable=["ci", "cl", "tas"])

        # Should have been called with aliased list
        assert len(mock_cat.search_calls) == 1
        call_kwargs = mock_cat.search_calls[0]
        expected_values = ["fld_s05i269", "fld_s02i261", "fld_s03i236"]  # ci->fld_s05i269, cl->fld_s02i261, tas->fld_s03i236
        assert call_kwargs["variable"] == expected_values

    def test_passthrough_unknown_fields(self):
        """Test that unknown fields pass through unchanged"""
        mock_cat = MockESMDatastore()
        wrapped_cat = AliasedESMCatalog(mock_cat, field_aliases=FIELD_ALIASES, value_aliases=VALUE_ALIASES)
        
        # Test unknown field
        wrapped_cat.search(unknown_field="unknown_value")
        
        # Should pass through unchanged
        assert len(mock_cat.search_calls) == 1
        call_kwargs = mock_cat.search_calls[0]
        assert call_kwargs["unknown_field"] == "unknown_value"
        
    def test_passthrough_unknown_values(self):
        """Test that unknown values pass through unchanged"""
        mock_cat = MockESMDatastore()
        wrapped_cat = AliasedESMCatalog(mock_cat, field_aliases=FIELD_ALIASES, value_aliases=VALUE_ALIASES)
        
        # Test unknown value for known field
        wrapped_cat.search(variable="unknown_variable")
        
        # Should pass through with canonical field name but original value
        assert len(mock_cat.search_calls) == 1
        call_kwargs = mock_cat.search_calls[0]
        assert call_kwargs["variable"] == "unknown_variable"
        
    def test_passthrough_attributes(self):
        """Test that other attributes pass through to the wrapped catalog"""
        mock_cat = MockESMDatastore()
        wrapped_cat = AliasedESMCatalog(mock_cat, field_aliases=FIELD_ALIASES, value_aliases=VALUE_ALIASES)
        
        # Test accessing attributes that aren't search
        assert wrapped_cat._captured_init_kwargs == mock_cat._captured_init_kwargs
        
    def test_dir_includes_wrapper_and_wrapped_attrs(self):
        """Test that dir() includes both wrapper and wrapped catalog attributes"""
        mock_cat = MockESMDatastore()
        wrapped_cat = AliasedESMCatalog(mock_cat, field_aliases=FIELD_ALIASES, value_aliases=VALUE_ALIASES)
        
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
            test_vars = ["ci", "rldscs", "rluscs"]  # Should map to fld_s05i269, fld_s02i208, fld_s02i206
            for cmip_var in test_vars:
                if cmip_var in _CMIP_TO_ACCESS_MAPPINGS:
                    assert cmip_var in variable_aliases
                    assert variable_aliases[cmip_var] == _CMIP_TO_ACCESS_MAPPINGS[cmip_var]
                
    def test_cmip_variable_search(self):
        """Test that CMIP variables can be found and return ACCESS variables"""
        mock_cat = MockESMDatastore()
        wrapped_cat = AliasedESMCatalog(mock_cat, field_aliases=FIELD_ALIASES, value_aliases=VALUE_ALIASES)
        
        # Test CMIP variable search (if mappings are available)
        if _CMIP_TO_ACCESS_MAPPINGS and "ci" in _CMIP_TO_ACCESS_MAPPINGS:
            wrapped_cat.search(variable="ci")
            
            # Should have been called with the ACCESS model variable name
            assert len(mock_cat.search_calls) == 1
            call_kwargs = mock_cat.search_calls[0]
            assert call_kwargs["variable"] == "fld_s05i269"


class TestAliasedDataframeCatalog:
    """Test the AliasedDataframeCatalog wrapper"""
    
    @pytest.fixture  
    def mock_dataframe_catalog(self):
        """Create a mock dataframe catalog"""
        mock_cat = MagicMock()
        
        # Mock to_source to return an ESM datastore
        mock_esm_datastore = MagicMock()
        mock_esm_datastore.search = MagicMock()
        mock_esm_datastore.esmcat = MagicMock()
        mock_cat.to_source.return_value = mock_esm_datastore
        
        # Mock to_source_dict to return multiple ESM datastores
        mock_esm1 = MagicMock()
        mock_esm1.search = MagicMock()
        mock_esm1.esmcat = MagicMock()
        
        mock_esm2 = MagicMock()
        mock_esm2.search = MagicMock()
        mock_esm2.esmcat = MagicMock()
        
        mock_cat.to_source_dict.return_value = {
            "datastore1": mock_esm1,
            "datastore2": mock_esm2
        }
        
        return mock_cat
    
    def test_to_source_wraps_esm_datastore(self, mock_dataframe_catalog):
        """Test that to_source() properly wraps ESM datastores with aliasing"""
        from access_nri_intake.aliases import AliasedDataframeCatalog, FIELD_ALIASES, VALUE_ALIASES
        
        catalog = AliasedDataframeCatalog(
            mock_dataframe_catalog,
            field_aliases=FIELD_ALIASES,
            value_aliases=VALUE_ALIASES
        )
        
        # Since search() returns the result directly (not wrapped), we need to call to_source on the catalog
        esm_datastore = catalog.to_source()
        
        # Should have called the underlying catalog's to_source
        mock_dataframe_catalog.to_source.assert_called_once()
        
        # The result should be an AliasedESMCatalog wrapping the ESM datastore
        from access_nri_intake.aliases import AliasedESMCatalog
        assert isinstance(esm_datastore, AliasedESMCatalog)
        
    def test_to_source_dict_wraps_multiple_datastores(self, mock_dataframe_catalog):
        """Test that to_source_dict() wraps all returned datastores"""
        from access_nri_intake.aliases import AliasedDataframeCatalog, FIELD_ALIASES, VALUE_ALIASES
        
        catalog = AliasedDataframeCatalog(
            mock_dataframe_catalog,
            field_aliases=FIELD_ALIASES, 
            value_aliases=VALUE_ALIASES
        )
        
        # Test to_source_dict() workflow
        datastores_dict = catalog.to_source_dict()
        
        # Should have called the underlying catalog's to_source_dict
        mock_dataframe_catalog.to_source_dict.assert_called_once()
        
        # Should return a dict of AliasedESMCatalog objects
        from access_nri_intake.aliases import AliasedESMCatalog
        assert len(datastores_dict) == 2
        assert isinstance(datastores_dict["datastore1"], AliasedESMCatalog)
        assert isinstance(datastores_dict["datastore2"], AliasedESMCatalog)

    def test_getitem_passthrough(self, mock_dataframe_catalog):
        """Test that __getitem__ passes through to underlying catalog without wrapping"""
        from access_nri_intake.aliases import AliasedDataframeCatalog
        
        # Mock a catalog entry 
        mock_entry = MagicMock()
        mock_dataframe_catalog.__getitem__.return_value = mock_entry
        
        catalog = AliasedDataframeCatalog(mock_dataframe_catalog)
        result = catalog["some_entry"]
        
        # Should return the raw entry, not wrapped
        assert result is mock_entry
        mock_dataframe_catalog.__getitem__.assert_called_once_with("some_entry")

    def test_getattr_passthrough(self, mock_dataframe_catalog):
        """Test that __getattr__ passes through to underlying catalog"""
        from access_nri_intake.aliases import AliasedDataframeCatalog
        
        # Mock an attribute
        mock_attr = MagicMock()
        mock_dataframe_catalog.some_attribute = mock_attr
        
        catalog = AliasedDataframeCatalog(mock_dataframe_catalog)
        result = catalog.some_attribute
        
        # Should return the raw attribute, not wrapped
        assert result is mock_attr


if __name__ == "__main__":
    pytest.main([__file__])