# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file.
# SPDX-License-Identifier: Apache-2.0

"""Load one small value from each selected real asset, sequentially on Gadi.

Takes a snapshot JSON on stdin. Results go to stdout. No remote files are written.
Only a single array element per dataset is computed; all other data remain lazy.
"""

import json
import sys
import time

import intake
import intake_esm
import pandas as pd


def run(snapshot):
    selections = [
        ("measures_bedmachine_antarctica", "v1", "bed"),
        ("measures_bedmachine_antarctica", "v3", "bed"),
        ("bedmap", "v3", "bed_topography"),
        ("antarctic_geothermal_heat_flow_model_aq1", "v1", "Q"),
        ("measures_annual_antarctic_ice_velocity_maps", "v1", "VX"),
        ("racmo2.3p2_monthly_27km_1979-2022", "v1", "ff10m"),
        ("racmo2.4p1_monthly_11km_1979-2023", "v1", "gbot"),
    ]
    results = []
    for dataset, version, variable in selections:
        matching = [
            r
            for r in snapshot["rows"]
            if r["dataset"] == dataset
            and r["version"] == version
            and variable in r["variable"]
        ]
        if not matching:
            results.append(
                {"dataset": dataset, "version": version, "status": "missing_test_asset"}
            )
            continue
        row = matching[0]
        esmcat = {
            "esmcat_version": "0.1.0",
            "id": "cryosphere_live_test",
            "description": "In-memory live smoke test",
            "attributes": [{"column_name": k, "vocabulary": ""} for k in row],
            "assets": {"column_name": "path", "format": "netcdf"},
            "aggregation_control": {
                "variable_column_name": "variable",
                "groupby_attrs": ["dataset", "version", "asset_id"],
                "aggregations": [],
            },
        }
        started = time.monotonic()
        result = {
            "dataset": dataset,
            "version": version,
            "path": row["path"],
            "variable": variable,
        }
        try:
            cat = intake.open_esm_datastore(
                {"esmcat": esmcat, "df": pd.DataFrame([row])}
            )
            ds = cat.search(variable=variable).to_dask(
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


if __name__ == "__main__":
    print(json.dumps(run(json.load(sys.stdin)), indent=2))
