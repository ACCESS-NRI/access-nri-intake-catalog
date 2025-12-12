# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""
Aliasing support for intake catalogs to provide user-friendly field names and value mappings.

RATIONALE
---------
This module provides aliasing functionality to make ACCESS-NRI data more discoverable by 
researchers familiar with CMIP vocabularies and conventions. The primary goal is to enable 
users who typically work with CMIP data to find raw ACCESS model outputs using familiar 
variable names, model names, and other CMIP-standard terminology.

For example:
- CMIP users searching for "ci" (convection time fraction) will find the corresponding 
  raw ACCESS model variables (e.g., "fld_s05i269" in ACCESS-ESM1.6)
- Users searching for "tas" (near-surface air temperature) will find the corresponding 
  raw ACCESS model temperature fields 
- Users familiar with CMIP standard names can also search using intuitive aliases like 
  "temp", "temperature", or "air_temperature" - all of which map to find "tas" variables
- Users familiar with CMIP model names can use shortcuts like "ACCESS-ESM1" to find 
  "ACCESS-ESM1-5" model data
- Frequency specifications can use human-readable terms like "daily" instead of "1day"

This bridging functionality allows CMIP users to leverage their existing vocabulary knowledge 
to discover raw ACCESS model data, while maintaining full backward compatibility with existing workflows.

The aliasing works in both directions - field name aliasing (e.g., "variable" → "variable_id") 
and value aliasing (e.g., "temp" → "tas") - allowing for flexible and intuitive data discovery.
The system also includes CMIP-to-ACCESS variable mappings that allow users to search for CMIP
variable names and find the corresponding native ACCESS model variable names.
"""

import json
from pathlib import Path
from importlib import resources as rsr


def _load_cmip_mappings():
    """
    Load CMIP to ACCESS variable mappings from the data sources.
    This allows users to search for CMIP standard names (like "ci") and 
    find the corresponding ACCESS model variables (like "fld_s05i269") 
    that are actually stored in the catalog.
    
    Returns
    -------
    dict
        Dictionary mapping CMIP variable names to ACCESS model variable names
    """
    try:
        # Try to load from the package data sources
        mapping_file = rsr.files("access_nri_intake").joinpath("data/mappings/access-esm1-6-cmip-mappings.json")
        with mapping_file.open(mode="r") as f:
            mappings = json.load(f)
        
        # Extract CMIP variable -> ACCESS model variable mappings
        cmip_to_access = {}
        
        for component in ["atmosphere", "land", "ocean"]:
            if component in mappings:
                for cmip_var, details in mappings[component].items():
                    if "model_variables" in details and details["model_variables"]:
                        # Map CMIP variable to ACCESS model variables
                        # If multiple model variables, take the first one
                        access_var = details["model_variables"][0]
                        cmip_to_access[cmip_var] = access_var
        
        return cmip_to_access
        
    except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
        # If loading fails, return empty dict - aliasing will still work with manual aliases
        return {}


class AliasedESMCatalog:
    """
    Thin wrapper around an intake-esm ESMDataStore that:
      - maps public field names → canonical fields (FIELD_ALIASES)
      - maps value aliases → canonical values per field (VALUE_ALIASES)
    """
    def __init__(self, cat, field_aliases=None, value_aliases=None):
        self._cat = cat
        self.field_aliases = field_aliases or {}
        self.value_aliases = value_aliases or {}

    # ---- alias helpers -------------------------------------------------
    def _canonical_field(self, field):
        """Convert user-facing field name to canonical field name"""
        return self.field_aliases.get(field, field)

    def _normalise_value(self, field, value):
        """Convert user-facing value to canonical value for a given field"""
        aliases_for_field = self.value_aliases.get(field, {})

        # scalar string
        if isinstance(value, str):
            return aliases_for_field.get(value, value)

        # list/tuple/set of strings
        if isinstance(value, (list, tuple, set)):
            out = [aliases_for_field.get(v, v) for v in value]
            return type(value)(out)

        # anything else (regex, callable, etc.) – leave untouched
        return value

    def _normalise_kwargs(self, kwargs):
        """Normalise all kwargs by applying field and value aliases"""
        norm = {}
        for field, value in kwargs.items():
            canon_field = self._canonical_field(field)
            norm[canon_field] = self._normalise_value(canon_field, value)
        return norm

    # ---- public API ----------------------------------------------------
    def search(self, **kwargs):
        """Search the catalog with aliased field names and values"""
        norm = self._normalise_kwargs(kwargs)
        return self._cat.search(**norm)

    # pass-through everything else to underlying catalog
    def __getattr__(self, name):
        return getattr(self._cat, name)

    def __dir__(self):
        return sorted(set(dir(self._cat)) | set(self.__dict__.keys()))


class AliasedDataframeCatalog:
    """
    Wrapper around an intake dataframe catalog that provides alias support
    for catalog entries that are ESM datastores.
    """
    def __init__(self, cat, field_aliases=None, value_aliases=None):
        self._cat = cat
        self.field_aliases = field_aliases or {}
        self.value_aliases = value_aliases or {}

    def __getattr__(self, name):
        """
        When accessing catalog entries, wrap ESM datastores with AliasedESMCatalog
        """
        attr = getattr(self._cat, name)
        
        # If this is an ESM datastore source, wrap it with aliasing
        if hasattr(attr, 'search') and hasattr(attr, '_captured_init_kwargs'):
            # This looks like an ESM datastore, wrap it
            return AliasedESMCatalog(
                attr,
                field_aliases=self.field_aliases,
                value_aliases=self.value_aliases
            )
        
        # Otherwise return as-is
        return attr

    def __dir__(self):
        return sorted(set(dir(self._cat)) | set(self.__dict__.keys()))


# Load CMIP to ACCESS variable mappings
_CMIP_TO_ACCESS_MAPPINGS = _load_cmip_mappings()

# Define alias mappings
FIELD_ALIASES = {
    # User-facing → actual df column
    "variable": "variable_id",
    "model": "source_id",
    "experiment": "experiment_id",
    "member": "member_id",
    "time_range": "time_range",
    "realm": "realm",
    "frequency": "frequency",
    "version": "version",
}

# Create variable aliases combining manual aliases with CMIP mappings
_MANUAL_VARIABLE_ALIASES = {
}

# Create combined variable aliases with CMIP mappings and manual aliases
# CMIP variables (like ci) map to ACCESS model variables (like fld_s05i269)
# Manual aliases (like temp) map to CMIP names (like tas)
# Manual aliases take precedence over CMIP mappings to avoid conflicts
_VARIABLE_ALIASES_COMBINED = {**_CMIP_TO_ACCESS_MAPPINGS, **_MANUAL_VARIABLE_ALIASES}

VALUE_ALIASES = {
    "variable_id": _VARIABLE_ALIASES_COMBINED,
    "source_id": {
        # ACCESS model aliases
    },
    "experiment_id": {
        # Common experiment aliases
        "historical": "historical",
        "hist": "historical",
        "control": "piControl",
        "pi-control": "piControl",
        "pre-industrial": "piControl",
        "rcp85": "ssp585", 
        "rcp45": "ssp245",
        "rcp26": "ssp126",
        "ssp5-85": "ssp585",
        "ssp2-45": "ssp245", 
        "ssp1-26": "ssp126",
    },
    "frequency": {
        # Frequency aliases
        "daily": "1day",
        "day": "1day",
        "monthly": "1mon",
        "month": "1mon",
        "yearly": "1yr",
        "annual": "1yr",
        "year": "1yr",
        "hourly": "1hr",
        "hour": "1hr",
        "3hourly": "3hr",
        "6hourly": "6hr",
    },
    "realm": {
        # Realm aliases
        "atmosphere": "atmos",
        "atm": "atmos",
        "ocean": "ocean",
        "oceanic": "ocean", 
        "land": "land",
        "terrestrial": "land",
        "ice": "seaIce",
        "sea_ice": "seaIce",
        "sea-ice": "seaIce",
    }
}