# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the ACCESS-MOPPy plugin functionality."""

import warnings
from unittest.mock import Mock, patch

import pytest


class TestMOPPyPlugin:
    """Test suite for ACCESS-MOPPy plugin functionality."""

    def test_plugin_enables_without_access_moppy(self):
        """Test that plugin can be enabled even without ACCESS-MOPPy installed."""
        from access_nri_intake.plugins.moppy import (
            disable_moppy_plugin,
            enable_moppy_plugin,
        )

        # Clean slate - disable first if already enabled
        disable_moppy_plugin()

        # Enable the plugin
        success = enable_moppy_plugin()
        assert success is True

        # Check that the method is available
        try:
            from intake_esm import esm_datastore

            assert hasattr(esm_datastore, "to_cmip")
            assert hasattr(esm_datastore, "_moppy_plugin_enabled")
        except ImportError:
            pytest.skip("intake-esm not available in test environment")

    def test_plugin_disable(self):
        """Test that plugin can be properly disabled."""
        from access_nri_intake.plugins.moppy import (
            disable_moppy_plugin,
            enable_moppy_plugin,
        )

        # Enable first
        enable_moppy_plugin()

        # Then disable
        success = disable_moppy_plugin()
        assert success is True

        # Check that the method is removed
        try:
            from intake_esm import esm_datastore

            assert not hasattr(esm_datastore, "to_cmip")
            assert not hasattr(esm_datastore, "_moppy_plugin_enabled")
        except ImportError:
            pytest.skip("intake-esm not available in test environment")

    def test_to_cmip_requires_access_moppy(self):
        """Test that to_cmip method raises ImportError when ACCESS-MOPPy is not available."""
        from access_nri_intake.plugins.moppy import enable_moppy_plugin

        # Enable plugin
        enable_moppy_plugin()

        try:
            from intake_esm import esm_datastore

            # Create a mock datastore
            mock_datastore = Mock(spec=esm_datastore)
            mock_datastore.to_dask.return_value = Mock()  # Mock xarray dataset

            # Add our method to the mock
            from access_nri_intake.plugins.moppy import to_cmip

            mock_datastore.to_cmip = to_cmip.__get__(mock_datastore, esm_datastore)

            # Should raise ImportError since ACCESS-MOPPy is not available
            with pytest.raises(ImportError, match="ACCESS-MOPPy is required"):
                mock_datastore.to_cmip()

        except ImportError:
            pytest.skip("intake-esm not available in test environment")

    def test_moppy_availability_check(self):
        """Test the is_moppy_available function."""
        from access_nri_intake.plugins.moppy import (
            disable_moppy_plugin,
            enable_moppy_plugin,
            is_moppy_available,
        )

        # Disable first
        disable_moppy_plugin()
        assert is_moppy_available() is False

        # Enable plugin
        enable_moppy_plugin()

        # Should still be False because ACCESS-MOPPy is not available
        # (MOPPY_AVAILABLE is set to False in the module)
        assert is_moppy_available() is False

    def test_plugin_prevents_override(self):
        """Test that plugin won't override existing to_cmip method."""
        from access_nri_intake.plugins.moppy import (
            disable_moppy_plugin,
            enable_moppy_plugin,
        )

        try:
            from intake_esm import esm_datastore

            # Clean slate
            disable_moppy_plugin()

            # Manually add a to_cmip method
            def fake_to_cmip(self):
                return "fake"

            esm_datastore.to_cmip = fake_to_cmip

            # Try to enable plugin - should warn and not override
            with warnings.catch_warnings(record=True) as w:
                success = enable_moppy_plugin()
                assert success is False
                assert len(w) == 1
                assert "already has a 'to_cmip' method" in str(w[0].message)

            # Cleanup
            delattr(esm_datastore, "to_cmip")

        except ImportError:
            pytest.skip("intake-esm not available in test environment")

    @patch("access_nri_intake.plugins.moppy.MOPPY_AVAILABLE", True)
    @patch("access_nri_intake.plugins.moppy.access_moppy")
    def test_to_cmip_with_mocked_access_moppy(self, mock_access_moppy):
        """Test to_cmip functionality with mocked ACCESS-MOPPy."""
        from access_nri_intake.plugins.moppy import enable_moppy_plugin, to_cmip

        # Setup mock
        mock_dataset = Mock()
        mock_access_moppy.cmorize.return_value = mock_dataset

        # Enable plugin
        enable_moppy_plugin()

        try:
            from intake_esm import esm_datastore

            # Create mock datastore
            mock_datastore = Mock(spec=esm_datastore)
            mock_datastore.to_dask.return_value = Mock()

            # Add our method
            mock_datastore.to_cmip = to_cmip.__get__(mock_datastore, esm_datastore)

            # Since we mocked CMOR_AVAILABLE to be True, this should not raise
            # but will still hit the placeholder code that just returns the original dataset
            with warnings.catch_warnings(record=True) as w:
                result = mock_datastore.to_cmip()
                # Should get the warning about not being implemented yet
                assert len(w) == 1
                assert "not yet implemented" in str(w[0].message)

        except ImportError:
            pytest.skip("intake-esm not available in test environment")


if __name__ == "__main__":
    # Simple test runner for development
    test_instance = TestMOPPyPlugin()

    print("Testing MOPPy plugin...")

    try:
        test_instance.test_plugin_enables_without_access_moppy()
        print("✓ Plugin enables without ACCESS-MOPPy")
    except Exception as e:
        print(f"✗ Plugin enable test failed: {e}")

    try:
        test_instance.test_plugin_disable()
        print("✓ Plugin disables correctly")
    except Exception as e:
        print(f"✗ Plugin disable test failed: {e}")

    try:
        test_instance.test_to_cmip_requires_access_moppy()
        print("✓ to_cmip requires ACCESS-MOPPy")
    except Exception as e:
        print(f"✗ to_cmip requirement test failed: {e}")

    try:
        test_instance.test_moppy_availability_check()
        print("✓ MOPPy availability check works")
    except Exception as e:
        print(f"✗ MOPPy availability test failed: {e}")

    print("\nAll tests completed!")
