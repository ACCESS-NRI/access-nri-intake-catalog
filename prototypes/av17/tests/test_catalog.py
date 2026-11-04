# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file.
# SPDX-License-Identifier: Apache-2.0

import json
from pathlib import Path
import shutil

import intake
import intake_esm  # register driver
import netCDF4
import numpy as np
import pyarrow.parquet as pq
import pytest

from prototypes.av17.av17_catalog import (
    CCD_ROOT,
    export,
    parse_netcdf,
    registry_entries,
    scan,
)


def write_nc(
    path, value=1, times=None, calendar="standard", units="days since 2000-01-01"
):
    path.parent.mkdir(parents=True, exist_ok=True)
    with netCDF4.Dataset(path, "w") as ds:
        ds.createDimension("x", 2)
        ds.createVariable("x", "f8", ("x",))[:] = [0, 1]
        dims = ("x",)
        if times is not None:
            ds.createDimension("time", len(times))
            t = ds.createVariable("time", "f8", ("time",))
            t.units, t.calendar = units, calendar
            t[:] = times
            dims = ("time", "x")
        ds.createVariable("crs", "i4").grid_mapping_name = "polar_stereographic"
        bed = ds.createVariable("bed", "f4", dims)
        bed.units, bed.standard_name, bed.grid_mapping = "m", "bedrock_altitude", "crs"
        bed[:] = value
    return path


@pytest.fixture
def registry():
    return {
        "datasets": {
            "measures_bedmachine_antarctica": {
                "path": str(
                    CCD_ROOT / "elevation_geometry" / "measures_bedmachine_antarctica"
                ),
                "description": "Fixture bed elevation",
                "extension": "nc",
                "doi": {"v1": "10.example/one", "v2": "10.example/two"},
                "ignore_files": {"v1": ["ignored.nc"]},
            },
            "bedmap": {
                "path": str(CCD_ROOT / "elevation_geometry" / "bedmap"),
                "subdatasets": {
                    "v3": {
                        "gridded": {"subpath": "gridded_data", "extension": "nc"},
                        "points": {
                            "subpath": "point_data",
                            "extension": "csv",
                            "skip_lines": 18,
                        },
                    }
                },
            },
        }
    }


@pytest.fixture
def root(tmp_path, registry):
    base = tmp_path / "data"
    product = base / "elevation_geometry" / "measures_bedmachine_antarctica"
    write_nc(product / "v1" / "bed.nc", 11)
    write_nc(product / "v2" / "bed.nc", 22)
    (product / "v1" / "ignored.nc").write_text("ignored by CCD")
    (product / "v1" / "broken.nc").write_text("bad netcdf")
    write_nc(
        base / "elevation_geometry" / "bedmap" / "v3" / "gridded_data" / "bed.nc", 33
    )
    points = base / "elevation_geometry" / "bedmap" / "v3" / "point_data"
    points.mkdir()
    (points / "points.csv").write_text("x,y\n1,2\n")
    return base


def test_roundtrip_search_and_load_keeps_versions_and_products_separate(
    root, registry, tmp_path
):
    snapshot = scan(root, registry)
    assert snapshot["complete"]
    descriptor = export(snapshot, tmp_path / "output")
    cat = intake.open_esm_datastore(str(descriptor))
    assert len(cat.df) == 3
    assert len(cat.search(variable="bed").keys()) == 3
    values = []
    # intake-esm-access 2026.4.15 still uses an outer thread pool even with
    # threaded=False. Exercise one selection at a time for netCDF-C safety.
    for asset_id in cat.df.asset_id:
        ds = cat.search(asset_id=asset_id, variable="bed").to_dask(
            threaded=False, xarray_open_kwargs={"decode_coords": "all"}
        )
        try:
            values.append(float(ds.bed.mean()))
            assert ds.crs.attrs["grid_mapping_name"] == "polar_stereographic"
        finally:
            ds.close()
    assert sorted(values) == [11, 22, 33]
    assert (
        len(cat.search(dataset="measures_bedmachine_antarctica", version="v1").df) == 1
    )
    assert set(cat.df.frequency) == {"fx"}
    assert "crs" not in cat.df.variable.iloc[0]
    assert cat.esmcat.aggregation_control.aggregations == []
    assert (
        pq.read_metadata(
            descriptor.with_name("cryosphere_av17_uniqs.parquet")
        ).metadata[b"num_records"]
        == b"3"
    )
    # Descriptor/table can be moved together; Gadi asset paths stay unchanged.
    shutil.copytree(tmp_path / "output", tmp_path / "relocated")
    assert (
        len(intake.open_esm_datastore(str(tmp_path / "relocated" / descriptor.name)).df)
        == 3
    )


def test_inventory_reports_mixed_formats_and_corrupt_files(root, registry):
    result = scan(root, registry)
    by_name = {Path(r["path"]).name: r for r in result["inventory"]}
    assert by_name["points.csv"]["status"] == "inventory_only"
    assert by_name["broken.nc"]["status"] == "invalid"
    assert by_name["ignored.nc"]["status"] == "excluded"
    assert len(result["rows"]) == 3


def test_limits_are_visible(root, registry, tmp_path):
    result = scan(root, registry, max_files=3)
    assert not result["complete"]
    export(result, tmp_path / "output")
    report = json.loads((tmp_path / "output" / "build-report.json").read_text())
    assert not report["complete"] and not report["publication_ready"]


def test_symlinks_cannot_index_external_assets(root, registry, tmp_path):
    external = write_nc(tmp_path / "external.nc")
    (
        root
        / "elevation_geometry"
        / "measures_bedmachine_antarctica"
        / "v1"
        / "link.nc"
    ).symlink_to(external)
    result = scan(root, registry)
    assert len(result["rows"]) == 3
    assert (
        next(r for r in result["inventory"] if r["path"].endswith("link.nc"))["status"]
        == "excluded"
    )


def test_unknown_versions_and_missing_datasets_reported(root, registry):
    (root / "elevation_geometry" / "bedmap" / "v4").mkdir()
    registry["datasets"]["missing"] = {
        "path": str(CCD_ROOT / "ice_velocity" / "missing"),
        "extension": "nc",
    }
    _, issues = registry_entries(registry, root)
    assert {r["status"] for r in issues} == {"unregistered_version", "missing_dataset"}


@pytest.mark.parametrize(
    ("times", "calendar", "frequency"),
    [
        ([0, 31, 60], "standard", "1mon"),
        ([0, 30, 60], "360_day", "1mon"),
        ([0, 1, 2], "standard", "1day"),
        ([14, 44, 74], "standard", "30day"),
        ([0, 1, 3], "standard", "irregular"),
        ([0, 1, 1], "standard", "irregular"),
        ([0], "standard", "unknown"),
    ],
)
def test_time_frequencies(tmp_path, times, calendar, frequency):
    path = write_nc(tmp_path / "test.nc", times=times, calendar=calendar)
    entry = dict(
        project="av17",
        category="surface_forcing",
        dataset="test",
        version="v1",
        subdataset="none",
        realm="landIce",
        model="test",
        description="",
        display_name="",
        tags=[],
        doi="",
    )
    row = parse_netcdf(path, entry, tmp_path)
    assert row["frequency"] == frequency
    assert row["calendar"] == calendar


def test_unknown_time_is_not_silently_static(tmp_path):
    path = write_nc(tmp_path / "annual_velocity_2001.nc")
    entry = dict(
        project="av17",
        category="ice_velocity",
        dataset="test",
        version="v1",
        subdataset="none",
        realm="landIce",
        model="not_applicable",
        description="",
        display_name="",
        tags=[],
        doi="",
    )
    assert parse_netcdf(path, entry, tmp_path)["frequency"] == "unknown"
    write_nc(path, times=[0, 1], units="bad units")
    assert parse_netcdf(path, entry, tmp_path)["temporal_status"] == "undecodable_time"


def test_deterministic_asset_ids_and_doi_resolution(root, registry):
    a, b = scan(root, registry), scan(root, registry)
    assert [r["asset_id"] for r in a["rows"]] == [r["asset_id"] for r in b["rows"]]
    assert {
        r["version"]: r["doi"]
        for r in a["rows"]
        if r["dataset"] == "measures_bedmachine_antarctica"
    } == {"v1": "10.example/one", "v2": "10.example/two"}


def test_grouped_netcdf_requires_specialist_loader(root, registry):
    path = (
        root
        / "elevation_geometry"
        / "measures_bedmachine_antarctica"
        / "v1"
        / "grouped.nc"
    )
    with netCDF4.Dataset(path, "w") as ds:
        ds.createGroup("observations")
    result = scan(root, registry)
    assert (
        next(r for r in result["inventory"] if r["path"].endswith("grouped.nc"))[
            "status"
        ]
        == "invalid"
    )


def test_duplicate_snapshot_assets_rejected(root, registry, tmp_path):
    result = scan(root, registry)
    result["rows"].append(result["rows"][0])
    with pytest.raises(ValueError, match="Duplicate"):
        export(result, tmp_path / "output")


def test_same_shape_files_remain_separate_and_sampling_is_explicit(
    root, registry, tmp_path
):
    path = (
        root
        / "elevation_geometry"
        / "measures_bedmachine_antarctica"
        / "v1"
        / "another_grid.nc"
    )
    write_nc(path, 99)
    with netCDF4.Dataset(path, "a") as ds:
        ds["x"][:] = [1000, 2000]
    result = scan(root, registry)
    cat = intake.open_esm_datastore(str(export(result, tmp_path / "full")))
    assert len(cat.search(dataset="measures_bedmachine_antarctica", version="v1")) == 2
    sample = scan(root, registry, samples_per_entry=1)
    assert sample["complete"] and not sample["header_scan_complete"]
    assert any(r["status"] == "not_sampled" for r in sample["inventory"])


def test_registry_path_traversal_rejected(root, registry):
    registry["datasets"]["bedmap"]["subdatasets"]["v3"]["escape"] = {
        "subpath": "../../../../outside",
        "extension": "nc",
    }
    with pytest.raises(ValueError, match="escapes root"):
        registry_entries(registry, root)


def test_declared_monthly_cadence_preserves_coordinate_spacing(tmp_path):
    path = write_nc(tmp_path / "monthly.nc", times=[15, 44, 75])
    entry = dict(
        project="av17",
        category="surface_forcing",
        dataset="racmo2.4p1_monthly_11km_1979-2023",
        version="v1",
        subdataset="none",
        realm="landIce",
        model="RACMO2.4p1",
        description="",
        display_name="",
        tags=[],
        doi="",
    )
    row = parse_netcdf(path, entry, tmp_path)
    assert row["frequency"] == "1mon"
    assert row["coordinate_frequency"] == "irregular"
    assert row["frequency_source"] == "ccdtools_product_description"
    write_nc(path, times=[0, 0, 1])
    row = parse_netcdf(path, entry, tmp_path)
    assert row["frequency"] == "irregular"  # Product cadence cannot mask bad ordering.
