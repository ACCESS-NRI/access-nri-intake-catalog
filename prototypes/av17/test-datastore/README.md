# Cryosphere datastore for headmetal's review

This standalone Intake-ESM datastore contains **978 NetCDF assets across 14 CCD
products** from the complete registered-root scan on 4 November 2026 UTC
(5 November in Brisbane). Each file remains an independent dataset. It references
existing files in `/g/data/av17/access-nri/cryosphere-data-pool`; no science data
are copied into this directory.

Searches work locally. Loading arrays requires **Gadi and access to project av17**.
Use a Python environment with `intake`, `intake-esm-access`, `xarray`, `netCDF4`,
`pandas<3` and `pyarrow`. No installation of the catalog-building package or
experimental schema patch is needed to open this datastore directly.

The seven representative checks also passed against this exported snapshot using
Gadi's `conda/analysis3-26.11` (Intake 2.0.8, intake-esm-access 2026.4.15). If you
use that shared environment, membership of `xp65` is also required:

```bash
module use /opt/Modules/modulefiles /apps/Modules/modulefiles /g/data/xp65/public/modules
module load conda/analysis3-26.11
```

## Get the datastore

From a checkout of PR #704, the files are in `prototypes/av17/test-datastore/`.
Alternatively, download just the two required files on Gadi:

```bash
mkdir -p av17-cryosphere-review
cd av17-cryosphere-review
base=https://raw.githubusercontent.com/ACCESS-NRI/access-nri-intake-catalog/codex/av17-cryosphere-prototype/prototypes/av17/test-datastore
curl -fLO "$base/cryosphere_av17.json"
curl -fLO "$base/cryosphere_av17.parquet"
```

Keep the JSON and Parquet together. Run the following Python from their directory
(or replace the descriptor with its full path):

```python
import intake
import intake_esm  # registers the driver

cat = intake.open_esm_datastore("cryosphere_av17.json")
print(len(cat.df), len(cat.keys()))  # 978 rows, 978 independent datasets
print(cat.df.groupby(["dataset", "version"]).size())

selection = cat.search(
    dataset="measures_bedmachine_antarctica", version="v3", variable="bed"
)
assert len(selection.df) == 1
print(
    selection.df[
        [
            "dataset",
            "version",
            "frequency",
            "coordinate_frequency",
            "frequency_source",
            "path",
        ]
    ]
)

ds = selection.to_dask(
    threaded=False,
    xarray_open_kwargs={"chunks": {}, "decode_coords": "all"},
)
try:
    bed = ds["bed"]
    sample = bed.isel({dim: size // 2 for dim, size in bed.sizes.items()})
    print(sample.compute(scheduler="single-threaded"))
finally:
    ds.close()
```

For a search with multiple files, iterate `selection.df.asset_id`, select one
`asset_id` at a time and close the dataset before opening the next. The tested
Intake version can still open files concurrently with `threaded=False`; the
one-file selection avoids the observed NetCDF-C concurrency failure.

## Run the seven representative loading checks

From the PR checkout root, on Gadi:

```bash
python prototypes/av17/live_smoke.py \
  --catalog prototypes/av17/test-datastore/cryosphere_av17.json \
  > /tmp/av17-cryosphere-smoke.json
```

This opens the exported datastore and reads one central array element from
BedMachine v1/v3, Bedmap v3, AQ1, annual velocity and both RACMO products, in
sequence. It exits unsuccessfully if a selection is missing or a load fails.
Seven `passed` results are expected; a `NaN` is a valid missing value, not an I/O
failure. This is a small loading check, not scientific validation of all assets.

## What to review

- Product/version/subdataset identity, exclusions and missing products or versions.
- Searchable variables, units and retained projection metadata.
- `frequency` (declared product cadence where available) versus
  `coordinate_frequency` (observed spacing), with `frequency_source` explaining
  the choice. Missing time coordinates do not automatically mean static data.
- Provisional `realm=landIce` and RACMO/`not_applicable` model labels, especially
  atmospheric forcing, and the per-product license metadata needed for publication.
- Whether the companion inventory complements ccdtools' format-specific loaders.

See [FIELDS.md](../FIELDS.md) for field meanings and missing values. The current
production schema rejects `unknown` and `irregular`; this standalone source works
without that schema change. Production registration still requires the decisions
in [ASSESSMENT.md](../ASSESSMENT.md).

## Included files and provenance

`cryosphere_av17.json` and `cryosphere_av17.parquet` are the loadable datastore.
`cryosphere_av17_uniqs.parquet` contains discovery facets. The companion
`cryosphere_inventory.parquet` records all 3,219 examined files, including 333
registered non-NetCDF assets, exclusions and ancillary files. Those non-NetCDF
files are not included in the NetCDF loader.

`build-report.json` records coverage and keeps `publication_ready=false`.
`manifest.json` records snapshot provenance and SHA-256 checksums of the five
generated files. This is a frozen review snapshot, not an automatically refreshed
catalog. Source files may change after the recorded scan date.
