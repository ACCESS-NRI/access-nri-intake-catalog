"""Tests for the optional ACCESS-ESM1.6 to MOPPy bridge."""

import sys
from pathlib import Path
from types import ModuleType

import pytest
import xarray as xr
from intake_esm.core import esm_datastore

from access_nri_intake.aliases import AliasedESMCatalog
from access_nri_intake import cmorise as cmorise_module


@pytest.fixture
def source_and_selection(sample_datastore_path, monkeypatch):
    source = esm_datastore(sample_datastore_path, columns_with_iterables=["variable"])
    source.name = "raw-esm1-6"
    source.metadata = {"model": ["ACCESS-ESM1-6"]}
    source.esmcat.df["member"] = "r1i1p1f1"
    wrapped = AliasedESMCatalog(source)
    selected = wrapped.search(frequency="1mon", realm="ocean")
    monkeypatch.setattr(cmorise_module, "_check_gadi_projects", lambda paths: None)
    return wrapped, selected


def _call_cmorise(source, selected):
    return source.cmorise(
        selected,
        compound_name="Omon.tos",
        cmip_version="CMIP6",
        experiment_id="historical",
        variant_label="r1i1p1f1",
        activity_id="CMIP",
        grid_label="gn",
    )


def test_cmorise_uses_only_selected_paths_and_returns_dataset(
    source_and_selection, monkeypatch
):
    source, selected = source_and_selection
    calls = []
    dataset = xr.Dataset({"tos": ("time", [1.0])})
    fake_moppy = ModuleType("access_moppy")

    class FakeCMORiser:
        def __init__(self, **kwargs):
            calls.append(("init", kwargs))

        def run(self):
            calls.append(("run",))

        def to_dataset(self):
            calls.append(("to_dataset",))
            return dataset

        def write(self):
            pytest.fail("cmorise must not write output files")

    fake_moppy.ACCESS_ESM_CMORiser = FakeCMORiser
    monkeypatch.setitem(sys.modules, "access_moppy", fake_moppy)

    # Search remains an ordinary Intake-ESM operation; MOPPy starts in step two.
    assert type(selected) is esm_datastore
    assert "cmorise" in dir(source)
    assert calls == []
    assert _call_cmorise(source, selected) is dataset
    assert [call[0] for call in calls] == ["init", "run", "to_dataset"]
    assert calls[0][1] == {
        "input_data": sorted(selected.df.path.unique().tolist()),
        "compound_name": "Omon.tos",
        "cmip_version": "CMIP6",
        "experiment_id": "historical",
        "source_id": "ACCESS-ESM1-6",
        "variant_label": "r1i1p1f1",
        "activity_id": "CMIP",
        "grid_label": "gn",
        "parent_info": None,
        "model_id": "ACCESS-ESM1-6",
    }


@pytest.mark.parametrize(
    "model",
    [None, ["ACCESS-ESM1-5"], ["ACCESS-ESM1-6", "ACCESS-CM2"]],
)
def test_cmorise_rejects_unsupported_or_ambiguous_models(source_and_selection, model):
    source, selected = source_and_selection
    source.unwrap().metadata = {"model": model}
    with pytest.raises(ValueError, match="only a source whose model metadata"):
        _call_cmorise(source, selected)


def test_cmorise_accepts_native_model_spelling(source_and_selection, monkeypatch):
    source, selected = source_and_selection
    source.unwrap().metadata = {"model": "ACCESS-ESM1.6"}
    fake_moppy = ModuleType("access_moppy")

    class FakeCMORiser:
        def run(self):
            pass

        def to_dataset(self):
            return xr.Dataset()

    fake_moppy.ACCESS_ESM_CMORiser = lambda **kwargs: FakeCMORiser()
    monkeypatch.setitem(sys.modules, "access_moppy", fake_moppy)
    assert isinstance(_call_cmorise(source, selected), xr.Dataset)


def test_cmorise_rejects_foreign_selection(source_and_selection):
    source, selected = source_and_selection
    selected.esmcat.df.loc[selected.df.index[0], "path"] = "/tmp/other-run.nc"
    with pytest.raises(ValueError, match="do not belong"):
        _call_cmorise(source, selected)


def test_cmorise_rejects_member_mismatch(source_and_selection):
    source, selected = source_and_selection
    selected.esmcat.df["member"] = "r2i1p1f1"
    with pytest.raises(ValueError, match="do not match variant_label"):
        _call_cmorise(source, selected)


def test_cmorise_rejects_existing_cmip_source(source_and_selection):
    source, selected = source_and_selection
    source.unwrap().esmcat.df["variable_id"] = "tos"
    with pytest.raises(ValueError, match="raw model output"):
        _call_cmorise(source, selected)


def test_cmorise_rejects_empty_selection(source_and_selection):
    source, selected = source_and_selection
    selected.esmcat.df.drop(selected.df.index, inplace=True)
    with pytest.raises(ValueError, match="contains no files"):
        _call_cmorise(source, selected)


def test_cmorise_requires_a_searched_datastore(source_and_selection):
    source, _ = source_and_selection
    with pytest.raises(TypeError, match="Intake-ESM datastore"):
        _call_cmorise(source, object())


def test_cmorise_rejects_non_raw_source(source_and_selection):
    source, selected = source_and_selection
    source.unwrap().esmcat.df.drop(columns="realm", inplace=True)
    with pytest.raises(ValueError, match="path, realm and variable columns"):
        _call_cmorise(source, selected)


def test_cmorise_rejects_missing_selected_paths(source_and_selection):
    source, selected = source_and_selection
    selected.esmcat.df.drop(columns="path", inplace=True)
    with pytest.raises(ValueError, match="no path column"):
        _call_cmorise(source, selected)


def test_cmorise_rejects_relative_paths(source_and_selection):
    source, selected = source_and_selection
    selected.esmcat.df["path"] = "relative.nc"
    with pytest.raises(ValueError, match="absolute local file paths"):
        _call_cmorise(source, selected)


def test_cmorise_rejects_other_source_id(source_and_selection):
    source, selected = source_and_selection
    selected.esmcat.id = "other-source"
    with pytest.raises(ValueError, match="does not belong to this catalog source"):
        _call_cmorise(source, selected)


def test_cmorise_rejects_non_netcdf_assets(source_and_selection):
    source, selected = source_and_selection
    source.unwrap().esmcat.assets.format = "zarr"
    with pytest.raises(ValueError, match="only local NetCDF assets"):
        _call_cmorise(source, selected)


@pytest.mark.parametrize(
    "field", ["compound_name", "experiment_id", "variant_label", "activity_id"]
)
def test_cmorise_requires_cmip_metadata(source_and_selection, field):
    source, selected = source_and_selection
    kwargs = {
        "compound_name": "Omon.tos",
        "cmip_version": "CMIP6",
        "experiment_id": "historical",
        "variant_label": "r1i1p1f1",
        "activity_id": "CMIP",
    }
    kwargs[field] = " "
    with pytest.raises(ValueError, match=f"{field} must be a non-empty string"):
        source.cmorise(selected, **kwargs)


def test_cmorise_rejects_unsupported_cmip_version(source_and_selection):
    source, selected = source_and_selection
    with pytest.raises(ValueError, match="cmip_version must be one of"):
        source.cmorise(
            selected,
            compound_name="Omon.tos",
            cmip_version="CMIP5",
            experiment_id="historical",
            variant_label="r1i1p1f1",
            activity_id="CMIP",
        )


def test_cmorise_reports_missing_optional_dependency(source_and_selection, monkeypatch):
    source, selected = source_and_selection
    monkeypatch.setitem(sys.modules, "access_moppy", None)
    with pytest.raises(ImportError, match=r"access_nri_intake\[moppy\]"):
        _call_cmorise(source, selected)


def test_cmorise_preserves_nested_import_errors(source_and_selection, monkeypatch):
    source, selected = source_and_selection

    def missing_moppy_dependency(name):
        raise ModuleNotFoundError("missing MOPPy dependency", name="other_module")

    monkeypatch.setattr(cmorise_module, "import_module", missing_moppy_dependency)
    with pytest.raises(ModuleNotFoundError, match="missing MOPPy dependency"):
        _call_cmorise(source, selected)


def test_cmorise_checks_gadi_project_mounts(monkeypatch):
    monkeypatch.setattr(Path, "is_dir", lambda self: False)
    with pytest.raises(FileNotFoundError, match="storage flags"):
        cmorise_module._check_gadi_projects(["/g/data/p73/archive/file.nc"])
