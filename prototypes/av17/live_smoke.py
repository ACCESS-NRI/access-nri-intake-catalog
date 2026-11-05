# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file.
# SPDX-License-Identifier: Apache-2.0

"""Load one small value from each selected real asset, sequentially on Gadi.

Takes an exported datastore with --catalog, or a snapshot JSON on stdin.
Results go to stdout. No source files are written.
Only a single array element per dataset is computed; all other data remain lazy.
"""

import argparse
import json
import sys
import time

import intake
import intake_esm
import pandas as pd

SELECTIONS = [
    ("measures_bedmachine_antarctica", "v1", "bed"),
    ("measures_bedmachine_antarctica", "v3", "bed"),
    ("bedmap", "v3", "bed_topography"),
    ("antarctic_geothermal_heat_flow_model_aq1", "v1", "Q"),
    ("measures_annual_antarctic_ice_velocity_maps", "v1", "VX"),
    ("racmo2.3p2_monthly_27km_1979-2022", "v1", "ff10m"),
    ("racmo2.4p1_monthly_11km_1979-2023", "v1", "gbot"),
]


def run_catalog(catalog, selections=SELECTIONS):
    """Exercise the actual exported descriptor, selecting one asset at a time."""
    results = []
    for dataset, version, variable in selections:
        matching = catalog.search(dataset=dataset, version=version, variable=variable)
        if matching.df.empty:
            results.append(
                {"dataset": dataset, "version": version, "status": "missing_test_asset"}
            )
            continue
        row = matching.df.sort_values("path").iloc[0]
        started = time.monotonic()
        result = {
            "dataset": dataset,
            "version": version,
            "path": row["path"],
            "variable": variable,
        }
        try:
            ds = matching.search(asset_id=row["asset_id"]).to_dask(
                threaded=False,
                xarray_open_kwargs={"chunks": {}, "decode_coords": "all"},
            )
            try:
                array = ds[variable]
                sample = (
                    array.isel({dim: size // 2 for dim, size in array.sizes.items()})
                    .compute(scheduler="single-threaded")
                    .item()
                )
                result.update(
                    status="passed",
                    dimensions=dict(ds.sizes),
                    value=str(sample),
                    coords=list(ds.coords),
                )
            finally:
                ds.close()
        except Exception as exc:
            result.update(status="failed", error=f"{type(exc).__name__}: {exc}")
        result["seconds"] = time.monotonic() - started
        results.append(result)
    return {
        "intake_version": intake.__version__,
        "intake_esm_version": intake_esm.__version__,
        "results": results,
    }


def run(snapshot):
    """Retain the original snapshot-on-stdin assessment interface."""
    frame = pd.DataFrame(snapshot["rows"])
    esmcat = {
        "esmcat_version": "0.1.0",
        "id": "cryosphere_live_test",
        "description": "In-memory live smoke test",
        "attributes": [{"column_name": k, "vocabulary": ""} for k in frame],
        "assets": {"column_name": "path", "format": "netcdf"},
        "aggregation_control": {
            "variable_column_name": "variable",
            "groupby_attrs": ["dataset", "version", "asset_id"],
            "aggregations": [],
        },
    }
    return run_catalog(intake.open_esm_datastore({"esmcat": esmcat, "df": frame}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--catalog", help="Path to the exported Intake-ESM JSON descriptor"
    )
    args = parser.parse_args()
    report = (
        run_catalog(intake.open_esm_datastore(args.catalog))
        if args.catalog
        else run(json.load(sys.stdin))
    )
    print(json.dumps(report, indent=2))
    sys.exit(0 if all(r["status"] == "passed" for r in report["results"]) else 1)
