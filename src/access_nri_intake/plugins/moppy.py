# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""
ACCESS-MOPPy plugin for adding CMORization capabilities to ESM datastores.

This plugin extends intake-esm's esm_datastore class with a to_cmip() method
that applies CMOR processing using ACCESS-MOPPy when available.
"""

import warnings
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from intake_esm import esm_datastore
    import xarray as xr

# Try to import ACCESS-MOPPy - this will be the actual import once the package exists
# For now, we'll use a placeholder that can be easily updated
try:
    # TODO: Update this import path once ACCESS-MOPPy package structure is finalized
    # import access_moppy
    # For now, we'll simulate the availability
    MOPPY_AVAILABLE = False  # Set to True when ACCESS-MOPPy is ready
    access_moppy = None
except ImportError:
    MOPPY_AVAILABLE = False
    access_moppy = None


def to_cmip(
    self, 
    xarray_open_kwargs: Optional[Dict[str, Any]] = None,
    cmor_table: Optional[str] = None,
    variable_id: Optional[str] = None,
    **cmor_kwargs
) -> "xr.Dataset":
    """
    Apply CMOR processing to the dataset using ACCESS-MOPPy.
    
    This method combines data loading and CMORization in one step, providing
    a seamless interface for users to get CMIP-compliant data from the catalog.
    
    Parameters
    ----------
    xarray_open_kwargs : dict, optional
        Keyword arguments passed to xarray.open_dataset when loading the data.
        Same as those accepted by to_dask().
    cmor_table : str, optional
        CMOR table to use for processing (e.g., 'Amon', 'Omon', 'day').
        If not provided, will attempt to auto-detect from metadata.
    variable_id : str, optional
        CMIP variable identifier to target. If not provided, will use
        the first variable found in the dataset.
    **cmor_kwargs : dict
        Additional keyword arguments passed to ACCESS-MOPPy's CMORization
        functions. These may include experiment metadata, institution info, etc.
        
    Returns
    -------
    xarray.Dataset
        CMORized dataset with CMIP-compliant metadata, coordinates, and
        variable attributes.
        
    Raises
    ------
    ImportError
        If ACCESS-MOPPy is not installed in the environment.
    ValueError
        If the dataset cannot be CMORized (e.g., unsupported variable,
        missing required metadata).
    RuntimeError
        If CMORization fails due to data or configuration issues.
        
    Examples
    --------
    >>> # Basic usage - auto-detect CMOR table and variable
    >>> ds = cat["ACCESS-ESM1-5"].search(variable="tas").to_cmip()
    
    >>> # Specify CMOR table and variable explicitly
    >>> ds = cat["ACCESS-OM2"].search(variable="temp").to_cmip(
    ...     cmor_table="Omon", 
    ...     variable_id="thetao"
    ... )
    
    >>> # Pass through xarray options and custom metadata
    >>> ds = cat["ACCESS-CM2"].search(variable="pr").to_cmip(
    ...     xarray_open_kwargs={"use_cftime": True},
    ...     cmor_table="Amon",
    ...     experiment_id="historical",
    ...     source_id="ACCESS-CM2"
    ... )
    """
    if not MOPPY_AVAILABLE:
        raise ImportError(
            "ACCESS-MOPPy is required for CMORization but is not installed. "
            "Please install it with: pip install access-moppy"
        )
    
    # Load the data using the standard to_dask method
    xarray_open_kwargs = xarray_open_kwargs or {}
    ds = self.to_dask(**xarray_open_kwargs)
    
    # TODO: Implement actual ACCESS-MOPPy integration once the package is ready
    # This is a placeholder implementation that will be replaced
    if access_moppy is None:
        # Placeholder: return the dataset with a warning
        warnings.warn(
            "ACCESS-MOPPy integration is not yet implemented. "
            "Returning uncmorized dataset.",
            UserWarning
        )
        return ds
    
    # Future implementation will look something like:
    # try:
    #     # Apply CMOR processing using ACCESS-MOPPy
    #     cmor_ds = access_moppy.cmorize(
    #         ds, 
    #         cmor_table=cmor_table,
    #         variable_id=variable_id,
    #         **cmor_kwargs
    #     )
    #     return cmor_ds
    # except Exception as e:
    #     raise RuntimeError(f"CMORization failed: {e}") from e
    
    # Placeholder return
    return ds


def enable_moppy_plugin() -> bool:
    """
    Enable the ACCESS-MOPPy plugin by monkey-patching esm_datastore with to_cmip method.
    
    This function safely adds the to_cmip method to intake_esm.esm_datastore
    objects, allowing users to call .to_cmip() on any datastore returned by
    the ACCESS-NRI catalog.
    
    Returns
    -------
    bool
        True if the plugin was successfully enabled, False otherwise.
        
    Examples
    --------
    >>> from access_nri_intake.plugins.moppy import enable_moppy_plugin
    >>> success = enable_moppy_plugin()
    >>> if success:
    ...     print("MOPPy plugin enabled!")
    """
    try:
        from intake_esm import esm_datastore
    except ImportError:
        warnings.warn(
            "intake-esm is not available. MOPPy plugin cannot be enabled.",
            UserWarning
        )
        return False
    
    # Check if plugin is already enabled
    if hasattr(esm_datastore, '_moppy_plugin_enabled'):
        return True
    
    # Safety check - ensure we're not overwriting an existing method
    if hasattr(esm_datastore, 'to_cmip'):
        warnings.warn(
            "esm_datastore already has a 'to_cmip' method. "
            "MOPPy plugin will not override it.",
            UserWarning
        )
        return False
    
    # Apply the monkey patch
    esm_datastore.to_cmip = to_cmip
    esm_datastore._moppy_plugin_enabled = True
    
    # Log success (only if ACCESS-MOPPy is actually available)
    if MOPPY_AVAILABLE:
        print("✓ MOPPy plugin enabled: .to_cmip() method added to esm_datastore")
    else:
        print("✓ MOPPy plugin enabled (ACCESS-MOPPy not available - install for full functionality)")
    
    return True


def disable_moppy_plugin() -> bool:
    """
    Disable the ACCESS-MOPPy plugin by removing the to_cmip method from esm_datastore.
    
    This is primarily useful for testing or if there are conflicts with other
    plugins.
    
    Returns
    -------
    bool
        True if the plugin was successfully disabled, False if it wasn't enabled.
    """
    try:
        from intake_esm import esm_datastore
    except ImportError:
        return False
    
    # Check if plugin is enabled
    if not hasattr(esm_datastore, '_moppy_plugin_enabled'):
        return False
    
    # Remove the method and flag
    if hasattr(esm_datastore, 'to_cmip'):
        delattr(esm_datastore, 'to_cmip')
    if hasattr(esm_datastore, '_moppy_plugin_enabled'):
        delattr(esm_datastore, '_moppy_plugin_enabled')
    
    print("✓ MOPPy plugin disabled")
    return True


def is_moppy_available() -> bool:
    """
    Check if ACCESS-MOPPy functionality is fully available.
    
    Returns
    -------
    bool
        True if both the plugin is enabled and ACCESS-MOPPy is available.
    """
    try:
        from intake_esm import esm_datastore
        plugin_enabled = hasattr(esm_datastore, '_moppy_plugin_enabled')
        return plugin_enabled and MOPPY_AVAILABLE
    except ImportError:
        return False