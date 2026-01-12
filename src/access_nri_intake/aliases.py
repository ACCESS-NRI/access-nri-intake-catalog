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
import warnings
from collections.abc import Collection
from importlib import resources as rsr
from typing import Any, TypeVar

from intake_dataframe_catalog.core import DfFileCatalog
from intake_esm.core import esm_datastore

T = TypeVar("T", esm_datastore, Any)


def _load_cmip_mappings() -> dict[str, str]:
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
        mapping_file = rsr.files("access_nri_intake").joinpath(
            "data/mappings/access-esm1-6-cmip-mappings.json"
        )
        with mapping_file.open() as f:
            mappings = json.load(f)

        # Extract CMIP variable -> ACCESS model variable mappings
        cmip_to_access: dict[str, Any] = {}

        for component in ["atmosphere", "land", "ocean"]:
            if component in mappings:
                for cmip_var, details in mappings[component].items():
                    if "model_variables" in details and details["model_variables"]:
                        # Map CMIP variable to ACCESS model variables
                        # If multiple model variables, take the first one
                        access_var = details["model_variables"][0]
                        cmip_to_access[cmip_var] = access_var

        return cmip_to_access

    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        # If loading fails, return empty dict - aliasing will still work with manual aliases
        return {}


class AliasedESMCatalog:
    """
    Thin wrapper around an intake-esm ESMDataStore that:
      - maps public field names → canonical fields (FIELD_ALIASES)
      - maps value aliases → canonical values per field (VALUE_ALIASES)

    Parameters
    ----------
    cat : ESMDataStore
        The underlying ESM datastore to wrap
    field_aliases : dict, optional
        Mapping of user-facing field names to canonical field names
    value_aliases : dict, optional
        Mapping of field names to value alias dictionaries
    show_warnings : bool, default True
        Whether to show warnings when aliasing occurs
    """

    def __init__(
        self,
        cat: esm_datastore,
        field_aliases: dict[str, str] | None = None,
        value_aliases: dict[str, Any] | None = None,
        show_warnings: bool = True,
    ):
        self._cat = cat
        self.field_aliases = field_aliases or {}
        self.value_aliases = value_aliases or {}
        self.show_warnings = show_warnings

    def _canonical_field(self, field: str) -> str:
        """Convert user-facing field name to canonical field name"""
        canonical = self.field_aliases.get(field, field)
        if canonical != field and self.show_warnings:
            warnings.warn(
                f"Field name aliasing: '{field}' → '{canonical}'",
                UserWarning,
                stacklevel=4,
            )
        return canonical

    def _normalise_value(self, field: str, value: str | Collection[str] | Any) -> Any:
        """
        Convert user-facing value to canonical value for a given field

        Value can be:
            - scalar string
            - Collection of strings (list, tuple, set)
            - anything else (regex, callable, etc.) – leave untouched
        """
        aliases_for_field = self.value_aliases.get(field, {})

        # scalar string
        if isinstance(value, str):
            normalized = aliases_for_field.get(value, value)
            if normalized != value and self.show_warnings:
                warnings.warn(
                    f"Value aliasing: {field}='{value}' → {field}='{normalized}'",
                    UserWarning,
                    stacklevel=4,
                )
            return normalized

        # list/tuple/set of strings
        if isinstance(value, (list | tuple | set)):
            out = []
            for v in value:
                normalized = aliases_for_field.get(v, v)
                if normalized != v and self.show_warnings:
                    warnings.warn(
                        f"Value aliasing: {field}='{v}' → {field}='{normalized}'",
                        UserWarning,
                        stacklevel=4,
                    )
                out.append(normalized)
            return type(value)(out)

        # anything else (regex, callable, etc.) – leave untouched
        return value

    def _normalise_kwargs(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        """Normalise all kwargs by applying field and value aliases"""
        norm = {}
        for field, value in kwargs.items():
            canon_field = self._canonical_field(field)
            norm[canon_field] = self._normalise_value(canon_field, value)
        return norm

    def search(self, **kwargs) -> esm_datastore:
        """Search the catalog with aliased field names and values"""
        norm: dict[str, Any] = self._normalise_kwargs(kwargs)
        return self._cat.search(**norm)

    # pass-through everything else to underlying catalog
    def __getattr__(self, name):
        return getattr(self._cat, name)

    def __dir__(self) -> list[str]:
        return sorted(
            set(dir(self._cat)) | set(self.__dict__.keys())
        )  # pragma: no cover


class AliasedDataframeCatalog:
    """
    Wrapper around an intake dataframe catalog that provides alias support
    for catalog entries that are ESM datastores.

    Parameters
    ----------
    cat : DataframeCatalog
        The underlying dataframe catalog to wrap
    field_aliases : dict, optional
        Mapping of user-facing field names to canonical field names (for dataframe level)
    value_aliases : dict, optional
        Mapping of field names to value alias dictionaries
    show_warnings : bool, default True
        Whether to show warnings when aliasing occurs in ESM datastores
    """

    def __init__(
        self,
        cat: DfFileCatalog,
        field_aliases: dict[str, Any] | None = None,
        value_aliases: dict[str, Any] | None = None,
        show_warnings: bool = True,
    ):
        self._cat = cat
        self.field_aliases = field_aliases or {}
        self.value_aliases = value_aliases or {}
        self.show_warnings = show_warnings

    def _wrap_if_esm_datastore(self, obj: T) -> AliasedESMCatalog | T:
        """
        Wrap an object with AliasedESMCatalog if it appears to be an ESM datastore.
        ESM datastores should use their native field names, not field aliases.
        """
        # Check if this is an ESM datastore (has search method and looks like intake-esm)
        if hasattr(obj, "search") and hasattr(obj, "esmcat"):
            return AliasedESMCatalog(
                obj,
                field_aliases=ESM_FIELD_ALIASES,  # Empty - use native field names
                value_aliases=self.value_aliases,
                show_warnings=self.show_warnings,
            )
        # Otherwise return as-is
        return obj

    def __getitem__(self, key: str) -> Any:
        """
        Handle catalog['entry-name'] access - delegate to underlying catalog
        """
        return self._cat[key]

    def search(self, **kwargs):
        """
        Search the dataframe catalog - apply aliases and wrap results
        """
        # For dataframe catalogs, search operates on the catalog metadata
        # We don't need to alias these searches since they're on the catalog structure
        result = self._cat.search(**kwargs)

        # If the result has to_source method (i.e., it's a searchable catalog result), wrap it
        if hasattr(result, "to_source"):
            return AliasedDataframeCatalog(
                result,
                field_aliases=self.field_aliases,
                value_aliases=self.value_aliases,
                show_warnings=self.show_warnings,
            )

        return result

    def to_source(self, **kwargs):
        """
        Get ESM datastore from catalog search result - wrap with aliasing
        """
        result = self._cat.to_source(**kwargs)
        return self._wrap_if_esm_datastore(result)

    def to_source_dict(self, **kwargs):
        """
        Get dict of ESM datastores from catalog search result - wrap each with aliasing
        """
        result = self._cat.to_source_dict(**kwargs)
        # Wrap each datastore in the dictionary
        wrapped_result = {}
        for key, datastore in result.items():
            wrapped_result[key] = self._wrap_if_esm_datastore(datastore)
        return wrapped_result

    # pass-through everything else to underlying catalog
    def __getattr__(self, name):
        return getattr(self._cat, name)

    def __dir__(self) -> list[str]:
        return sorted(
            set(dir(self._cat)) | set(self.__dict__.keys())
        )  # pragma: no cover


# Load CMIP to ACCESS variable mappings
_CMIP_TO_ACCESS_MAPPINGS: dict[str, str] = _load_cmip_mappings()

# Field aliases - only used at dataframe catalog level, not for ESM datastores
# ESM datastores use their native field names (variable_id, variable, etc.)
DATAFRAME_FIELD_ALIASES: dict[str, str] = {
    # User-facing → ACCESS-NRI dataframe catalog column names
    "source_id": "model",  # CMIP-style field name → ACCESS-NRI field name
    "variable_id": "variable",  # CMIP-style field name → ACCESS-NRI field name
    "table_id": "realm",  # CMIP-style field name → ACCESS-NRI field name
    "member_id": "ensemble",  # CMIP-style field name → ACCESS-NRI field name
    "experiment_id": "experiment",  # CMIP-style field name → ACCESS-NRI field name
    "source": "model",  # Alternative alias
    "var": "variable",  # Short alias
}

# ESM datastores should NOT use field aliases - they use native field names
ESM_FIELD_ALIASES: dict[str, str] = {}

# Create variable aliases combining manual aliases with CMIP mappings
_MANUAL_VARIABLE_ALIASES: dict[str, str] = {}

# Create combined variable aliases with CMIP mappings and manual aliases
# CMIP variables (like ci) map to ACCESS model variables (like fld_s05i269)
# Manual aliases (like temp) map to CMIP names (like tas)
# Manual aliases take precedence over CMIP mappings to avoid conflicts
_VARIABLE_ALIASES_COMBINED = {**_CMIP_TO_ACCESS_MAPPINGS, **_MANUAL_VARIABLE_ALIASES}

VALUE_ALIASES = {
    # Variable aliases - support both ACCESS-NRI and CMIP field names
    "variable": _VARIABLE_ALIASES_COMBINED,  # ACCESS-NRI catalog field name
    "variable_id": _VARIABLE_ALIASES_COMBINED,  # CMIP/ESM datastore field name
    # Model aliases - support both ACCESS-NRI and CMIP field names
    "model": {
        # ACCESS model aliases - maps to ACCESS-NRI catalog's "model" field
        "ACCESS-ESM1": "ACCESS-ESM1-5",
        "ACCESS-CM2": "ACCESS-CM2",
        "ACCESS-OM2": "ACCESS-OM2",
    },
    "source_id": {
        # CMIP/ESM datastore field name - same aliases as model
        "ACCESS-ESM1": "ACCESS-ESM1-5",
        "ACCESS-CM2": "ACCESS-CM2",
        "ACCESS-OM2": "ACCESS-OM2",
    },
    "experiment_id": {
        # Common experiment aliases (if catalog has experiment_id field)
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
        # Frequency aliases - ACCESS-NRI uses frequency field
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
        # Realm aliases - ACCESS-NRI uses realm field
        "atmosphere": "atmos",
        "atm": "atmos",
        "ocean": "ocean",
        "oceanic": "ocean",
        "land": "land",
        "terrestrial": "land",
        "ice": "seaIce",
        "sea_ice": "seaIce",
        "sea-ice": "seaIce",
    },
}
