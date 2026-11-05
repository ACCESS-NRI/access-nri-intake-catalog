# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file.
# SPDX-License-Identifier: Apache-2.0

"""Prototype: registry-driven CCD inventory and conservative Intake-ESM export.

The scanner only opens NetCDF headers and small time coordinates. It never reads
science arrays. Every asset remains a separate Intake dataset: combining different
versions, projections, regions, grids or temporal products requires an explicit
product-specific policy, not an inference from equal dimension lengths.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import time
from pathlib import Path

import netCDF4
import numpy as np
import yaml

CCD_ROOT = Path("/g/data/av17/access-nri/cryosphere-data-pool")
STATIC_PRODUCTS = {
    "measures_bedmachine_antarctica",
    "bedmap",
    "antarctic_geothermal_heat_flow_model_aq1",
}
# Product cadence from the checked-in ccdtools display names/descriptions. Keep
# the coordinate-spacing result as a separate field: monthly products can use
# midpoint timestamps whose day/hour vary. These rules never authorize joins.
DECLARED_FREQUENCIES = {
    "racmo2.3p2_monthly_27km_1979-2022": "1mon",
    "racmo2.4p1_monthly_11km_1979-2023": "1mon",
    "measures_annual_antarctic_ice_velocity_maps": "1yr",
    "measures_its_live_antarctic_quarterly_ice_shelf_height_change": "3mon",
}
MAX_TIME_VALUES = 10000
SECONDS_PER_HOUR = 3600
GROUPBY = ["project", "category", "dataset", "version", "subdataset", "asset_id"]


def resolve(meta, sub, version, key, default=None):
    """Match ccdtools' dataset/version/subdataset metadata precedence."""
    if key in sub:
        value = sub[key]
        return value.get(version, value) if isinstance(value, dict) else value
    value = meta.get(key, default)
    return value.get(version, default) if isinstance(value, dict) else value


def as_list(value):
    return [] if value is None else [value] if isinstance(value, str) else list(value)


def within(path, root):
    return path.resolve().is_relative_to(root.resolve())


def registry_entries(registry, root):
    """Expand only registered, available versions; report missing roots separately."""
    entries, issues = [], []
    for dataset, meta in sorted(registry["datasets"].items()):
        relative = Path(meta["path"]).relative_to(CCD_ROOT)
        base = root / relative
        if not within(base, root):
            raise ValueError(f"Registry path escapes root: {base}")
        if not base.is_dir():
            issues.append({"path": str(base), "status": "missing_dataset"})
            continue
        for version_path in sorted(base.iterdir()):
            if not version_path.is_dir() or not re.fullmatch(
                r"v\d[\w.-]*", version_path.name
            ):
                continue
            version = version_path.name
            subsets = meta.get("subdatasets", {}).get(version)
            if "subdatasets" in meta and subsets is None:
                issues.append(
                    {"path": str(version_path), "status": "unregistered_version"}
                )
                continue
            for subdataset, sub in (subsets or {"none": {}}).items():
                path = version_path / sub.get("subpath", "")
                if not within(path, root):
                    raise ValueError(f"Registry subpath escapes root: {path}")
                if not path.is_dir():
                    issues.append({"path": str(path), "status": "missing_subdataset"})
                    continue
                entry = {
                    "dataset": dataset,
                    "category": relative.parts[0],
                    "version": version,
                    "subdataset": subdataset,
                    "root": str(path),
                    "description": meta.get("description", ""),
                    "display_name": meta.get("display_name", dataset),
                    "tags": meta.get("tags", []),
                    "realm": "landIce",
                    "project": "av17",
                    "model": (
                        dataset.split("_")[0].upper()
                        if dataset.startswith("racmo")
                        else "not_applicable"
                    ),
                }
                for key in (
                    "extension",
                    "doi",
                    "loader",
                    "ignore_dirs",
                    "ignore_files",
                    "static_patterns",
                    "resolutions",
                    "skip_lines",
                    "no_data_value",
                ):
                    entry[key] = resolve(meta, sub, version, key)
                if not isinstance(entry["extension"], str):
                    issues.append({"path": str(path), "status": "missing_extension"})
                    continue
                entries.append(entry)
    return entries, issues


def temporal_metadata(  # noqa: PLR0912 -- explicit time-quality cases
    nc, entry, filename
):
    candidates = [
        v
        for name, v in nc.variables.items()
        if v.ndim == 1
        and (
            name.lower() == "time"
            or getattr(v, "standard_name", "") == "time"
            or getattr(v, "axis", "") == "T"
        )
    ]
    result = {
        "frequency": "unknown",
        "start_date": "none",
        "end_date": "none",
        "calendar": "none",
        "time_dimension": "none",
        "temporal_status": "no_time_axis",
    }
    if not candidates:
        if entry["dataset"] in STATIC_PRODUCTS or any(
            p in filename for p in as_list(entry.get("static_patterns"))
        ):
            result.update(frequency="fx", temporal_status="declared_static")
        return result
    if len(candidates) != 1:
        result["temporal_status"] = "ambiguous_time_axes"
        return result
    coordinate = candidates[0]
    result["time_dimension"] = coordinate.dimensions[0]
    calendar = getattr(coordinate, "calendar", "standard")
    result["calendar"] = calendar
    count = len(coordinate)
    units = getattr(coordinate, "units", "")
    if not count or "since" not in units:
        result["temporal_status"] = "undecodable_time"
        return result
    try:
        # Bound coordinate reads, even for sub-hourly products.
        values = (
            coordinate[:] if count <= MAX_TIME_VALUES else coordinate[[0, count - 1]]
        )
        if np.ma.getmaskarray(values).any():
            raise ValueError("masked time coordinate")
        dates = netCDF4.num2date(values, units, calendar=calendar)
        result.update(
            start_date=dates[0].strftime("%Y-%m-%d, %H:%M:%S"),
            end_date=dates[-1].strftime("%Y-%m-%d, %H:%M:%S"),
        )
        if count == 1 or count > MAX_TIME_VALUES:
            result["temporal_status"] = (
                "singleton_time" if count == 1 else "time_endpoints_only"
            )
            return result
        seconds = np.array([(b - a).total_seconds() for a, b in zip(dates, dates[1:])])
        months = np.array(
            [
                (b.year - a.year) * 12 + b.month - a.month
                for a, b in zip(dates, dates[1:])
            ]
        )
        if np.any(seconds <= 0):
            result.update(temporal_status="non_monotonic_time", frequency="irregular")
            return result
        # Only infer calendar months when day and clock time agree. A fixed
        # 30-day series must not become monthly merely because dates are close.
        same_day = len({(d.day, d.hour, d.minute, d.second) for d in dates}) == 1
        if same_day and np.all(months == months[0]) and months[0] > 0:
            frequency = (
                f"{months[0] // 12}yr" if months[0] % 12 == 0 else f"{months[0]}mon"
            )
        elif np.allclose(seconds, seconds[0], rtol=0, atol=0.01):
            if seconds[0] % 86400 == 0:
                frequency = f"{int(seconds[0] / 86400)}day"
            elif seconds[0] % 3600 == 0:
                frequency = f"{int(seconds[0] / 3600)}hr"
            else:
                frequency = "subhr" if seconds[0] < SECONDS_PER_HOUR else "irregular"
        else:
            frequency = "irregular"
        result.update(frequency=frequency, temporal_status="decoded_time")
    except (ValueError, TypeError, OverflowError) as exc:
        result.update(temporal_status="undecodable_time", time_error=str(exc))
    return result


def parse_netcdf(path, entry, root):
    with netCDF4.Dataset(path, "r") as nc:
        if nc.groups:
            raise ValueError("NetCDF groups require an explicit group-aware loader")
        # Coordinates and CF support variables are retained when loading, but not
        # advertised as independently searchable science variables.
        support = set()
        for var in nc.variables.values():
            for attr in ("bounds", "grid_mapping", "coordinates"):
                support.update(str(getattr(var, attr, "")).split())
        names = [
            name
            for name, v in nc.variables.items()
            if v.dimensions != (name,) and name not in support
        ]
        if not names:
            raise ValueError("No science variables in the root NetCDF group")
        relative = path.relative_to(root).as_posix()
        row = {
            key: entry[key]
            for key in (
                "project",
                "category",
                "dataset",
                "version",
                "subdataset",
                "realm",
                "model",
                "display_name",
                "tags",
                "doi",
            )
        }
        row["product_description"] = entry["description"]
        row.update(
            path=str(path),
            asset_id=hashlib.sha256(relative.encode()).hexdigest(),
            format="netcdf",
            variable=names,
            dimensions=json.dumps(
                {k: len(v) for k, v in nc.dimensions.items()}, sort_keys=True
            ),
            ccdtools_loader=entry.get("loader") or "default",
            **temporal_metadata(nc, entry, path.name),
        )
        row["coordinate_frequency"] = row["frequency"]
        row["frequency_source"] = "file_time_coordinate_or_static_rule"
        if entry["dataset"] in DECLARED_FREQUENCIES and row["temporal_status"] in (
            "decoded_time",
            "no_time_axis",
        ):
            row["frequency"] = DECLARED_FREQUENCIES[entry["dataset"]]
            row["frequency_source"] = "ccdtools_product_description"
        for attr in ("long_name", "standard_name", "units", "cell_methods"):
            row["variable_" + attr] = [str(getattr(nc[n], attr, "")) for n in names]
        row["grid_mapping"] = json.dumps(
            {
                name: {a: str(nc[name].getncattr(a)) for a in nc[name].ncattrs()}
                for name in support
                if name in nc.variables and "grid_mapping_name" in nc[name].ncattrs()
            },
            sort_keys=True,
        )
        row["source_time_coverage"] = json.dumps(
            {
                a: str(nc.getncattr(a))
                for a in nc.ncattrs()
                if a.startswith("time_coverage")
            },
            sort_keys=True,
        )
        return row


def scan(  # noqa: PLR0912, PLR0915 -- explicit inventory outcomes
    root, registry, max_files=1000, max_seconds=45, samples_per_entry=None
):
    root = Path(root).resolve()
    if not root.is_dir():
        raise FileNotFoundError(root)
    started = time.monotonic()
    entries, issues = registry_entries(registry, root)
    rows, inventory, seen = [], [], set()
    complete = True
    for entry in entries:
        attempted = 0

        def walk_error(error):
            issues.append(
                {"path": error.filename, "status": "walk_error", "reason": str(error)}
            )

        for directory, dirs, files in os.walk(
            entry["root"], followlinks=False, onerror=walk_error
        ):
            dirs[:] = sorted(d for d in dirs if not Path(directory, d).is_symlink())
            for name in sorted(files):
                if (
                    len(inventory) >= max_files
                    or time.monotonic() - started > max_seconds
                ):
                    complete = False
                    break
                path = Path(directory, name)
                if str(path) in seen:
                    continue
                seen.add(str(path))
                item = {
                    k: entry[k]
                    for k in ("dataset", "category", "version", "subdataset", "doi")
                }
                item.update(path=str(path), format=path.suffix.lower().lstrip("."))
                inventory.append(item)
                if not within(path, root) or path.is_symlink():
                    item.update(status="excluded", reason="symlink_or_outside_root")
                    continue
                relative = path.relative_to(Path(entry["root"])).as_posix()
                exclusions = as_list(entry.get("ignore_dirs")) + as_list(
                    entry.get("ignore_files")
                )
                if any(pattern in relative for pattern in exclusions):
                    item.update(status="excluded", reason="ccdtools_ignore_rule")
                    continue
                if item["format"] != entry["extension"].lower().lstrip("."):
                    item.update(status="ancillary", reason="not_registry_extension")
                    continue
                if item["format"] not in ("nc", "nc4", "cdf"):
                    item.update(
                        status="inventory_only", reason="use_ccdtools_format_loader"
                    )
                    continue
                if samples_per_entry is not None and attempted >= samples_per_entry:
                    item.update(status="not_sampled", reason="header_sample_limit")
                    continue
                attempted += 1
                try:
                    stat = path.stat()
                    row = parse_netcdf(path, entry, root)
                    row.update(size_bytes=stat.st_size, mtime_ns=stat.st_mtime_ns)
                    rows.append(row)
                    item["status"] = "indexed"
                except Exception as exc:
                    item.update(status="invalid", reason=f"{type(exc).__name__}: {exc}")
            if not complete:
                break
        if not complete:
            break
    # A walk can finish after skipping an unreadable directory, and registry
    # expansion can omit missing roots. Neither constitutes a complete scan.
    # Apply this after traversal so errors do not stop other available entries.
    complete = complete and not any(
        issue["status"]
        in ("missing_dataset", "missing_subdataset", "missing_extension", "walk_error")
        for issue in issues
    )
    return {
        "root": str(root),
        "observed_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "complete": complete,
        "elapsed_seconds": time.monotonic() - started,
        "limits": {
            "max_files": max_files,
            "max_seconds": max_seconds,
            "samples_per_entry": samples_per_entry,
        },
        "header_scan_complete": complete
        and not any(r["status"] == "not_sampled" for r in inventory),
        "issues": issues,
        "entries": entries,
        "rows": rows,
        "inventory": inventory,
    }


def export(snapshot, output):
    """Emit a relocatable JSON/Parquet datastore, inventory and build report."""
    import pandas as pd  # noqa: PLC0415 -- scanner can run without export dependencies

    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    if not snapshot["rows"]:
        raise ValueError("No NetCDF assets were indexed")
    name = "cryosphere_av17"
    frame = pd.DataFrame(snapshot["rows"]).sort_values("path").reset_index(drop=True)
    if frame["path"].duplicated().any() or frame["asset_id"].duplicated().any():
        raise ValueError("Duplicate assets in snapshot")
    frame.to_parquet(output / f"{name}.parquet", index=False)
    descriptor = {
        "esmcat_version": "0.1.0",
        "id": name,
        "description": "PROTOTYPE: Cryosphere Community Datapool NetCDF assets; one dataset per file.",
        "catalog_file": f"{name}.parquet",
        "attributes": [{"column_name": c, "vocabulary": ""} for c in frame.columns],
        "assets": {"column_name": "path", "format": "netcdf"},
        "aggregation_control": {
            "variable_column_name": "variable",
            "groupby_attrs": GROUPBY,
            "aggregations": [],
        },
    }
    (output / f"{name}.json").write_text(json.dumps(descriptor, indent=2) + "\n")
    pd.DataFrame(snapshot["inventory"]).to_parquet(
        output / "cryosphere_inventory.parquet", index=False
    )
    summary = {
        k: v for k, v in snapshot.items() if k not in ("rows", "inventory", "entries")
    }
    summary.update(
        indexed_files=len(frame),
        registered_entries=len(snapshot["entries"]),
        status_counts=pd.Series([r["status"] for r in snapshot["inventory"]])
        .value_counts()
        .to_dict(),
        frequency_counts=frame.frequency.value_counts().to_dict(),
        publication_ready=False,
        aggregation_policy="One asset per dataset; no automatic joins or unions.",
    )
    (output / "build-report.json").write_text(json.dumps(summary, indent=2) + "\n")
    # Existing interactive catalogue uses this sidecar layout for eager facets.
    uniqs = {
        c: [
            sorted(
                {
                    str(v)
                    for values in frame[c]
                    for v in (values if isinstance(values, list) else [values])
                    if v is not None
                }
            )
        ]
        for c in frame.columns
        if c not in ("path", "asset_id")
    }
    import pyarrow as pa  # noqa: PLC0415 -- only required for export
    import pyarrow.parquet as pq  # noqa: PLC0415 -- only required for export

    sidecar = pa.Table.from_pandas(pd.DataFrame(uniqs), preserve_index=False)
    sidecar = sidecar.replace_schema_metadata(
        {**(sidecar.schema.metadata or {}), b"num_records": str(len(frame)).encode()}
    )
    pq.write_table(sidecar, output / f"{name}_uniqs.parquet")
    return output / f"{name}.json"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    scanner = sub.add_parser("scan")
    scanner.add_argument("--root", type=Path, default=CCD_ROOT)
    scanner.add_argument("--registry", type=Path, required=True)
    scanner.add_argument("--max-files", type=int, default=1000)
    scanner.add_argument("--max-seconds", type=float, default=45)
    scanner.add_argument(
        "--samples-per-entry",
        type=int,
        help="Optional number of NetCDF headers per registered product/version/subdataset",
    )
    writer = sub.add_parser("export")
    writer.add_argument("snapshot", type=Path)
    writer.add_argument("output", type=Path)
    args = parser.parse_args()
    if args.command == "scan":
        registry = yaml.safe_load(args.registry.read_text())
        print(
            json.dumps(
                scan(
                    args.root,
                    registry,
                    args.max_files,
                    args.max_seconds,
                    args.samples_per_entry,
                ),
                indent=2,
            )
        )
    else:
        print(export(json.loads(args.snapshot.read_text()), args.output))


if __name__ == "__main__":
    main()
