# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Optional ESGF discovery for an ACCESS-NRI CMIP7 intake-esm source.

The two catalogs have different row granularity: intake-esm lists files, while
intake-esgf lists datasets. This module reconciles dataset identities for display
and keeps the original catalogs for opening data.
"""

from __future__ import annotations

import re
import warnings
from collections.abc import Mapping
from importlib import import_module
from typing import TYPE_CHECKING, Any, Literal

import pandas as pd

if TYPE_CHECKING:
    from access_nri_intake.aliases import AliasedESMCatalog


CMIP7_IDENTITY = (
    "drs_specs",
    "project",
    "activity_id",
    "institution_id",
    "source_id",
    "experiment_id",
    "variant_label",
    "region",
    "frequency",
    "variable_id",
    "variable_branding_suffix",
    "grid_label",
    "version",
)
REQUIRED_IDENTITY = (
    "source_id",
    "experiment_id",
    "variant_label",
    "frequency",
    "variable_id",
    "grid_label",
)
SEARCH_FACETS = set(CMIP7_IDENTITY) | {"member_id"}
_UNKNOWN_LOCAL_VERSION = object()
_UNKNOWN_ESGF_VERSION = object()


def _is_pattern(value: str | re.Pattern[str]) -> bool:
    """Mirror intake-esm's pattern detection for strings and compiled regexes."""
    if isinstance(value, re.Pattern):
        return True
    unescaped = value
    for char in "*?$^":
        unescaped = unescaped.replace(rf"\{char}", "")
    return any(char in unescaped for char in "*?$^")


def _esgf_facets(kwargs: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and translate intake-esm CMIP7 facets for ESGF filtering."""
    unsupported = set(kwargs) - SEARCH_FACETS
    if unsupported:
        raise ValueError(
            "ESGF CMIP7 search cannot translate these intake-esm fields: "
            f"{', '.join(sorted(unsupported))}"
        )
    facets = dict(kwargs)
    if "member_id" in facets:
        if "variant_label" in facets:
            raise ValueError("Use either member_id or variant_label, not both")
        facets["variant_label"] = facets.pop("member_id")
    if "project" in facets and facets["project"] not in ("CMIP7", ["CMIP7"]):
        raise ValueError("ESGF federation is restricted to project='CMIP7'")
    facets.pop("project", None)
    for field, value in facets.items():
        values = list(value) if isinstance(value, (list, tuple, set)) else [value]
        if not values or any(
            not isinstance(item, (str, re.Pattern)) for item in values
        ):
            raise ValueError(f"ESGF facet {field!r} needs strings or regex patterns")
        for item in values:
            if isinstance(item, str) and _is_pattern(item):
                try:
                    re.compile(item)
                except re.error as exc:
                    raise ValueError(f"Invalid regex for ESGF facet {field!r}") from exc
        facets[field] = values if len(values) > 1 else values[0]
    return facets


def _server_facets(facets: Mapping[str, Any], *, allow_broad: bool) -> dict[str, Any]:
    """Send only exact values to ESGF indices, which do not share regex syntax."""
    exact = {}
    for field, value in facets.items():
        values = value if isinstance(value, list) else [value]
        if not any(_is_pattern(item) for item in values):
            exact[field] = value
    if not allow_broad and not (
        set(exact) & {"source_id", "variable_id", "experiment_id"}
    ):
        raise ValueError(
            "Regex ESGF search needs an exact source_id, variable_id, or "
            "experiment_id; pass esgf_broad=True to scan the CMIP7 project"
        )
    return exact


def _filter_esgf_frame(frame: pd.DataFrame, facets: Mapping[str, Any]) -> pd.DataFrame:
    """Apply intake-esm's exact/regex query semantics to ESGF dataset rows."""
    result = frame
    for field, value in facets.items():
        if field not in result:
            raise ValueError(f"ESGF results have no {field!r} facet to filter")
        values = value if isinstance(value, list) else [value]
        series = result[field].astype("string")
        mask = pd.Series(False, index=result.index)
        for item in values:
            if _is_pattern(item):
                mask |= series.str.contains(item, regex=True, na=False)
            else:
                mask |= series.eq(item).fillna(False)
        result = result.loc[mask]
    return result.copy()


def _local_frame(local: Any) -> pd.DataFrame:
    frame = local.df.copy()
    if "member_id" in frame and "variant_label" not in frame:
        frame = frame.rename(columns={"member_id": "variant_label"})
    return frame


def _local_search_kwargs(kwargs: Mapping[str, Any]) -> dict[str, Any]:
    """Pass scalar compiled patterns as sequences for intake-esm search."""
    return {
        field: [value] if isinstance(value, re.Pattern) else value
        for field, value in kwargs.items()
    }


def _identity_columns(local: pd.DataFrame, remote: pd.DataFrame) -> list[str]:
    shared = [col for col in CMIP7_IDENTITY if col in local and col in remote]
    missing = set(REQUIRED_IDENTITY) - set(shared)
    if missing and not local.empty and not remote.empty:
        raise ValueError(
            "Cannot safely reconcile local and ESGF CMIP7 datasets; missing "
            f"identity fields: {', '.join(sorted(missing))}"
        )
    if remote.empty:
        return [col for col in CMIP7_IDENTITY if col in local]
    return shared


def _identity(
    row: pd.Series, columns: list[str], *, unknown_version: object
) -> tuple[Any, ...]:
    values = []
    for col in columns:
        value = "" if pd.isna(row[col]) else str(row[col])
        if col == "version":
            if not value:
                values.append(unknown_version)
                continue
            value = value.removeprefix("v")
        values.append(value)
    return tuple(values)


class FederatedCMIP7Catalog:
    """A searched CMIP7 source with local and ESGF dataset matches.

    ``df``/``matches`` has one row per dataset, with ``availability`` equal to
    ``local``, ``both``, or ``esgf``. ``local_df`` and ``esgf_df`` retain the
    original catalog rows. Only ESGF-only rows are passed to intake-esgf's
    ``to_dataset_dict``; that call may download data into its configured cache.
    """

    def __init__(
        self,
        source: AliasedESMCatalog,
        local: Any,
        esgf: Any,
        *,
        esgf_search_status: Literal["complete", "incomplete"] = "complete",
    ):
        self._source = source
        self.local = local
        self.esgf = esgf
        self.esgf_search_status = esgf_search_status
        self.local_df = _local_frame(local)
        self.esgf_df = (
            esgf.df.copy() if esgf is not None else pd.DataFrame(columns=CMIP7_IDENTITY)
        )
        self._reconcile()

    def _reconcile(self) -> None:
        local_identity = self.local_df
        remote_identity = self.esgf_df
        if not local_identity.empty and not remote_identity.empty:
            # An absent version cannot prove that the NCI and ESGF rows are the
            # same dataset. Keep the ESGF version eligible for transfer.
            if "version" not in local_identity:
                local_identity = local_identity.assign(version=pd.NA)
            if "version" not in remote_identity:
                remote_identity = remote_identity.assign(version=pd.NA)
        columns = _identity_columns(local_identity, remote_identity)
        local_unique = (
            local_identity.drop_duplicates(subset=columns)
            if columns
            else local_identity.copy()
        )
        local_ids = {
            _identity(row, columns, unknown_version=_UNKNOWN_LOCAL_VERSION)
            for _, row in local_unique.iterrows()
        }
        esgf_ids = {
            _identity(row, columns, unknown_version=_UNKNOWN_ESGF_VERSION)
            for _, row in remote_identity.iterrows()
        }
        local_matches = local_unique.copy()
        local_matches["availability"] = [
            "both"
            if _identity(row, columns, unknown_version=_UNKNOWN_LOCAL_VERSION)
            in esgf_ids
            else "local"
            for _, row in local_matches.iterrows()
        ]
        remote_keys = [
            _identity(row, columns, unknown_version=_UNKNOWN_ESGF_VERSION)
            not in local_ids
            for _, row in remote_identity.iterrows()
        ]
        self.esgf_only_df = self.esgf_df.loc[remote_keys].copy()
        remote_matches = self.esgf_only_df.copy()
        remote_matches["availability"] = "esgf"
        self.matches = pd.concat(
            [local_matches, remote_matches], ignore_index=True, sort=False
        )

    @property
    def df(self) -> pd.DataFrame:
        """Dataset-level union with an availability column."""
        return self.matches

    def search(self, **kwargs: Any) -> FederatedCMIP7Catalog:
        """Narrow both search results without transferring files."""
        norm = self._source._normalise_kwargs(kwargs)
        facets = _esgf_facets(norm)
        local = self.local.search(**_local_search_kwargs(norm))
        if self.esgf is None:
            return FederatedCMIP7Catalog(
                self._source,
                local,
                None,
                esgf_search_status=self.esgf_search_status,
            )
        remote = self.esgf.clone()
        remote.project = self.esgf.project
        remote.last_search = self.esgf.last_search.copy()
        remote.file_start = self.esgf.file_start
        remote.file_end = self.esgf.file_end
        remote.df = _filter_esgf_frame(self.esgf_df, facets)
        if remote.df.empty:
            remote = None
        return FederatedCMIP7Catalog(
            self._source,
            local,
            remote,
            esgf_search_status=self.esgf_search_status,
        )

    def to_dataset_dict(
        self, *, esgf_kwargs: dict[str, Any] | None = None, **local_kwargs: Any
    ) -> dict[str, Any]:
        """Open local matches, then download/open ESGF-only matches on demand.

        Native intake-esm options go in ``local_kwargs``. Optional intake-esgf
        options go in ``esgf_kwargs``; ``add_measures`` and ``minimal_keys`` are
        fixed to avoid extra downloads and keep deterministic remote keys.
        """
        datasets = (
            self.local.to_dataset_dict(**local_kwargs)
            if not self.local_df.empty
            else {}
        )
        if self.esgf is None or self.esgf_only_df.empty:
            return datasets
        options = dict(esgf_kwargs or {})
        if set(options) & {"add_measures", "minimal_keys"}:
            raise ValueError("add_measures and minimal_keys are fixed by federation")
        self.esgf.df = self.esgf_only_df.copy()
        remote = self.esgf.to_dataset_dict(
            add_measures=False, minimal_keys=False, **options
        )
        for key, dataset in remote.items():
            datasets[f"esgf:{key}"] = dataset
        return datasets


def search_cmip7_esgf(
    source: AliasedESMCatalog,
    local_kwargs: dict[str, Any],
    *,
    allow_broad: bool = False,
) -> FederatedCMIP7Catalog:
    """Search the NCI source and ESGF, with no data transfer."""
    facets = _esgf_facets(local_kwargs)
    server_facets = _server_facets(facets, allow_broad=allow_broad)
    try:
        ESGFCatalog = import_module("intake_esgf").ESGFCatalog
        NoSearchResults = import_module("intake_esgf.exceptions").NoSearchResults
    except ImportError as exc:
        raise ImportError(
            "ESGF search needs intake-esgf and Python >=3.12; "
            "install with pip install 'access_nri_intake[esgf]'"
        ) from exc

    local = source.unwrap().search(**_local_search_kwargs(local_kwargs))
    esgf = ESGFCatalog()
    with warnings.catch_warnings(record=True) as search_warnings:
        warnings.simplefilter("always")
        try:
            esgf.search(
                project="CMIP7",
                quiet=True,
                latest="version" not in facets,
                **server_facets,
            )
        except NoSearchResults:
            esgf = None
    failed_indices = [
        str(warning.message)
        for warning in search_warnings
        if "results may be incomplete" in str(warning.message)
    ]
    for warning in search_warnings:
        if "results may be incomplete" not in str(warning.message):
            warnings.warn(warning.message, warning.category, stacklevel=2)
    esgf_search_status: Literal["complete", "incomplete"] = (
        "incomplete" if failed_indices else "complete"
    )
    if failed_indices:
        warnings.warn(
            "ESGF search incomplete; local-only results do not prove there are "
            "no ESGF matches. " + "; ".join(failed_indices),
            UserWarning,
            stacklevel=2,
        )
    if esgf is not None:
        esgf.df = _filter_esgf_frame(esgf.df, facets)
        if esgf.df.empty:
            esgf = None
    return FederatedCMIP7Catalog(
        source, local, esgf, esgf_search_status=esgf_search_status
    )
