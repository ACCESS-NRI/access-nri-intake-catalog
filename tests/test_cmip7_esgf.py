# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Offline tests for opt-in CMIP7 ESGF federation."""

import re
import warnings
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest
from intake_esm.core import esm_datastore

from access_nri_intake import aliases
from access_nri_intake import cmip7_esgf as integration
from access_nri_intake.aliases import AliasedESMCatalog
from access_nri_intake.cmip7_esgf import (
    FederatedCMIP7Catalog,
    _esgf_facets,
    _filter_esgf_frame,
    _local_search_kwargs,
    _server_facets,
    search_cmip7_esgf,
)


def _row(variable, version="20260101", **extra):
    return {
        "project": "CMIP7",
        "source_id": "ACCESS-ESM1-6",
        "experiment_id": "historical",
        "variant_label": "r1i1p1f1",
        "frequency": "mon",
        "variable_id": variable,
        "grid_label": "gn",
        "version": version,
        **extra,
    }


def _catalog(local_rows, remote_rows):
    local = MagicMock()
    local.df = pd.DataFrame(local_rows)
    local.to_dataset_dict.return_value = {"local.tas": object()}
    esgf = MagicMock()
    esgf.df = pd.DataFrame(remote_rows)
    esgf.to_dataset_dict.return_value = {"CMIP7.pr": object()}
    source = MagicMock()
    source._normalise_kwargs.side_effect = lambda kwargs: kwargs
    return FederatedCMIP7Catalog(source, local, esgf), local, esgf


def test_search_reconciles_local_files_and_remote_datasets_without_download():
    local_rows = [_row("tas", path="a.nc"), _row("tas", path="b.nc")]
    remote_rows = [_row("tas"), _row("pr")]
    result, local, esgf = _catalog(local_rows, remote_rows)

    assert result.df.set_index("variable_id").availability.to_dict() == {
        "tas": "both",
        "pr": "esgf",
    }
    assert result.local_df.path.tolist() == ["a.nc", "b.nc"]
    assert len(result.esgf_only_df) == 1
    local.to_dataset_dict.assert_not_called()
    esgf.to_dataset_dict.assert_not_called()


def test_open_fetches_only_esgf_only_matches_and_keeps_local_keys():
    result, local, esgf = _catalog([_row("tas")], [_row("tas"), _row("pr")])

    datasets = result.to_dataset_dict(esgf_kwargs={"quiet": True})

    assert set(datasets) == {"local.tas", "esgf:CMIP7.pr"}
    local.to_dataset_dict.assert_called_once_with()
    assert esgf.df.variable_id.tolist() == ["pr"]
    esgf.to_dataset_dict.assert_called_once_with(
        add_measures=False, minimal_keys=False, quiet=True
    )


def test_remote_new_version_is_not_suppressed_by_local_version():
    result, _, _ = _catalog([_row("tas")], [_row("tas", version="20260201")])
    assert result.df.availability.tolist() == ["local", "esgf"]


def test_version_prefix_is_reconciled():
    result, _, _ = _catalog([_row("tas", version="v20260101")], [_row("tas")])
    assert result.df.availability.tolist() == ["both"]


def test_no_remote_results_only_opens_local():
    result, local, esgf = _catalog([_row("tas")], [])
    assert result.df.availability.tolist() == ["local"]
    assert set(result.to_dataset_dict()) == {"local.tas"}
    local.to_dataset_dict.assert_called_once()
    esgf.to_dataset_dict.assert_not_called()


def test_missing_core_identity_fails_closed():
    local = MagicMock()
    local.df = pd.DataFrame([{"source_id": "ACCESS-ESM1-6"}])
    esgf = SimpleNamespace(df=pd.DataFrame([_row("tas")]))
    with pytest.raises(ValueError, match="missing identity fields"):
        FederatedCMIP7Catalog(MagicMock(), local, esgf)


def test_esgf_facets_translate_and_validate():
    assert _esgf_facets({"source_id": "ACCESS-ESM1-6", "member_id": "r1i1p1f1"}) == {
        "source_id": "ACCESS-ESM1-6",
        "variant_label": "r1i1p1f1",
    }
    assert _esgf_facets({"variable_id": re.compile("^tas$")}) == {
        "variable_id": re.compile("^tas$")
    }
    with pytest.raises(ValueError, match="cannot translate"):
        _esgf_facets({"variable_id": "tas", "time_range": "2000-2010"})
    with pytest.raises(ValueError, match="Invalid regex"):
        _esgf_facets({"source_id": "ACCESS-*["})
    with pytest.raises(ValueError, match="either member_id or variant_label"):
        _esgf_facets({"member_id": "r1i1p1f1", "variant_label": "r2i1p1f1"})
    with pytest.raises(ValueError, match="restricted to project='CMIP7'"):
        _esgf_facets({"project": "CMIP6", "variable_id": "tas"})
    with pytest.raises(ValueError, match="needs strings or regex patterns"):
        _esgf_facets({"variable_id": []})
    with pytest.raises(ValueError, match="needs strings or regex patterns"):
        _esgf_facets({"variable_id": 42})


def test_esgf_filter_rejects_missing_metadata_facet():
    with pytest.raises(ValueError, match="no 'variable_id' facet"):
        _filter_esgf_frame(
            pd.DataFrame([{"source_id": "ACCESS-ESM1-6"}]), {"variable_id": "tas"}
        )


def test_local_member_id_is_reconciled_with_esgf_variant_label():
    local_row = _row("tas")
    local_row["member_id"] = local_row.pop("variant_label")
    result, _, _ = _catalog([local_row], [_row("tas")])
    assert result.df.availability.tolist() == ["both"]
    assert result.local_df.variant_label.tolist() == ["r1i1p1f1"]


def test_regex_facets_are_filtered_locally_and_exact_facets_go_to_esgf():
    facets = _esgf_facets(
        {"source_id": re.compile(r"^ACCESS-.*$"), "variable_id": "tas"}
    )
    assert _server_facets(facets, allow_broad=False) == {"variable_id": "tas"}
    frame = pd.DataFrame(
        [
            _row("tas", source_id="ACCESS-ESM1-6"),
            _row("tas", source_id="CanESM5"),
            _row("pr", source_id="ACCESS-ESM1-6"),
        ]
    )
    assert _filter_esgf_frame(frame, facets).source_id.tolist() == ["ACCESS-ESM1-6"]


def test_regex_only_needs_explicit_broad_scan():
    facets = _esgf_facets({"source_id": "^ACCESS-.*$"})
    with pytest.raises(ValueError, match="esgf_broad=True"):
        _server_facets(facets, allow_broad=False)
    assert _server_facets(facets, allow_broad=True) == {}


def test_mixed_exact_and_regex_values_preserve_union():
    facets = _esgf_facets({"variable_id": ["tas", re.compile(r"^pr$")]})
    assert _server_facets(facets, allow_broad=True) == {}
    frame = pd.DataFrame([_row("tas"), _row("pr"), _row("tos")])
    assert _filter_esgf_frame(frame, facets).variable_id.tolist() == ["tas", "pr"]


def test_compiled_regex_flags_are_preserved():
    facets = _esgf_facets({"source_id": re.compile("^access", re.IGNORECASE)})
    frame = pd.DataFrame([_row("tas"), _row("tas", source_id="CanESM5")])
    assert _filter_esgf_frame(frame, facets).source_id.tolist() == ["ACCESS-ESM1-6"]


def test_scalar_compiled_regex_is_wrapped_for_intake_esm():
    pattern = re.compile("t[s]")
    assert _local_search_kwargs(
        {"source_id": "ACCESS-ESM1-6", "variable_id": pattern}
    ) == {
        "source_id": "ACCESS-ESM1-6",
        "variable_id": [pattern],
    }


def test_refining_federated_results_keeps_local_and_remote_in_sync():
    result, local, esgf = _catalog([_row("tas"), _row("pr")], [_row("tas"), _row("pr")])
    narrowed_local = MagicMock()
    narrowed_local.df = pd.DataFrame([_row("pr")])
    local.search.return_value = narrowed_local
    esgf.last_search = {"source_id": "ACCESS-ESM1-6"}
    esgf.file_start = None
    esgf.file_end = None
    remote_clone = esgf.clone.return_value

    narrowed = result.search(variable_id=re.compile("^pr$"))

    local.search.assert_called_once_with(variable_id=[re.compile("^pr$")])
    assert narrowed.df.variable_id.tolist() == ["pr"]
    assert narrowed.df.availability.tolist() == ["both"]
    assert remote_clone.df.variable_id.tolist() == ["pr"]

    empty = result.search(variable_id="not_present")
    assert empty.esgf is None
    assert empty.esgf_df.empty


def test_refining_local_only_federated_result_stays_local():
    result, local, _ = _catalog([_row("tas")], [])
    result.esgf = None
    narrowed_local = MagicMock()
    narrowed_local.df = pd.DataFrame([_row("tas")])
    local.search.return_value = narrowed_local

    narrowed = result.search(variable_id="tas")

    assert narrowed.esgf is None
    assert narrowed.df.availability.tolist() == ["local"]


def test_to_dataset_dict_rejects_overriding_fixed_esgf_options():
    result, local, esgf = _catalog([], [_row("tas")])
    with pytest.raises(ValueError, match="fixed by federation"):
        result.to_dataset_dict(esgf_kwargs={"add_measures": True})
    local.to_dataset_dict.assert_not_called()
    esgf.to_dataset_dict.assert_not_called()


def test_regex_search_does_not_download_and_reconciles(monkeypatch):
    local = MagicMock()
    local.df = pd.DataFrame([_row("tas")])
    local.to_dataset_dict.return_value = {"local.tas": object()}
    local_catalog = MagicMock()
    local_catalog.search.return_value = local
    source = MagicMock()
    source.unwrap.return_value = local_catalog
    remote = MagicMock()
    remote.df = pd.DataFrame([_row("tas"), _row("tas", source_id="ACCESS-CM2")])
    remote.search.return_value = remote
    remote.to_dataset_dict.return_value = {"CMIP7.remote": object()}

    class NoResults(Exception):
        pass

    def fake_import(name):
        if name == "intake_esgf":
            return SimpleNamespace(ESGFCatalog=lambda: remote)
        return SimpleNamespace(NoSearchResults=NoResults)

    monkeypatch.setattr(integration, "import_module", fake_import)
    result = search_cmip7_esgf(
        source, {"source_id": re.compile("^ACCESS-.*$"), "variable_id": "tas"}
    )
    remote.search.assert_called_once_with(
        project="CMIP7", quiet=True, latest=True, variable_id="tas"
    )
    local_catalog.search.assert_called_once_with(
        source_id=[re.compile("^ACCESS-.*$")], variable_id="tas"
    )
    assert result.df.availability.tolist() == ["both", "esgf"]
    remote.to_dataset_dict.assert_not_called()
    datasets = result.to_dataset_dict()
    assert set(datasets) == {"local.tas", "esgf:CMIP7.remote"}
    assert remote.df.source_id.tolist() == ["ACCESS-CM2"]
    remote.to_dataset_dict.assert_called_once_with(
        add_measures=False, minimal_keys=False
    )


def test_missing_optional_esgf_dependency_gives_install_hint(monkeypatch):
    source = MagicMock()

    def missing_import(_):
        raise ImportError("intake_esgf is absent")

    monkeypatch.setattr(integration, "import_module", missing_import)
    with pytest.raises(ImportError, match="access_nri_intake\\[esgf\\]"):
        search_cmip7_esgf(source, {"variable_id": "tas"})
    source.unwrap.assert_not_called()


def test_unreachable_esgf_indices_warn_and_return_local_matches(monkeypatch):
    local = MagicMock()
    local.df = pd.DataFrame([_row("tas")])
    source = MagicMock()
    source.unwrap.return_value.search.return_value = local
    remote = MagicMock()

    class NoResults(Exception):
        pass

    def unavailable(**_):
        warnings.warn(
            "ESGF index failed to return a response, results may be incomplete"
        )
        raise NoResults()

    remote.search.side_effect = unavailable
    monkeypatch.setattr(
        integration,
        "import_module",
        lambda name: (
            SimpleNamespace(ESGFCatalog=lambda: remote)
            if name == "intake_esgf"
            else SimpleNamespace(NoSearchResults=NoResults)
        ),
    )

    with pytest.warns(UserWarning, match="results may be incomplete"):
        result = search_cmip7_esgf(source, {"variable_id": "tas"})

    assert result.df.availability.tolist() == ["local"]
    assert result.esgf is None


def test_esgf_results_outside_requested_facets_leave_local_matches(monkeypatch):
    local = MagicMock()
    local.df = pd.DataFrame([_row("tas")])
    source = MagicMock()
    source.unwrap.return_value.search.return_value = local
    remote = MagicMock()
    remote.df = pd.DataFrame([_row("pr")])

    class NoResults(Exception):
        pass

    monkeypatch.setattr(
        integration,
        "import_module",
        lambda name: (
            SimpleNamespace(ESGFCatalog=lambda: remote)
            if name == "intake_esgf"
            else SimpleNamespace(NoSearchResults=NoResults)
        ),
    )

    result = search_cmip7_esgf(source, {"variable_id": "tas"})

    assert result.df.availability.tolist() == ["local"]
    assert result.esgf is None


def test_source_search_opt_in_does_not_change_normal_search(
    sample_datastore_path, monkeypatch
):
    source = esm_datastore(sample_datastore_path, columns_with_iterables=["variable"])
    source.name = "cmip7_replicated"
    wrapped = AliasedESMCatalog(source)
    sentinel = object()
    called = []

    def fake_search(wrapper, kwargs, *, allow_broad):
        called.append((wrapper, kwargs, allow_broad))
        return sentinel

    monkeypatch.setattr(aliases, "search_cmip7_esgf", fake_search)
    assert wrapped.search(esgf=True, variable_id="tas") is sentinel
    pattern = re.compile(r"^ta.*$")
    assert wrapped.search(esgf=True, esgf_broad=True, variable_id=pattern) is sentinel
    assert called == [
        (wrapped, {"variable_id": "tas"}, False),
        (wrapped, {"variable_id": pattern}, True),
    ]
    assert isinstance(wrapped.search(variable="tas"), esm_datastore)


def test_esgf_options_require_opt_in_and_cmip7_source(sample_datastore_path):
    source = esm_datastore(sample_datastore_path, columns_with_iterables=["variable"])
    source.name = "cmip6"
    wrapped = AliasedESMCatalog(source)

    with pytest.raises(ValueError, match="esgf_broad=True requires esgf=True"):
        wrapped.search(esgf_broad=True, variable_id="tas")
    with pytest.raises(ValueError, match="only supported for CMIP7 sources"):
        wrapped.search(esgf=True, variable_id="tas")
