# Fields in the av17 cryosphere prototype

The NetCDF datastore has one row per file. Fields below are metadata for finding
and opening an asset; they are not new variables written into the source files.
Strings such as `none`, `unknown` and `not_applicable` are deliberate prototype
sentinels. Variable attribute lists retain the same ordering as `variable`.

## Identity and discovery

| Field | Type | Source and purpose |
| --- | --- | --- |
| `project` | string | `av17`, the NCI storage project required for these assets. ISMIP6 in `cm45` is outside this scanner. |
| `category` | string | First directory below the published CCD root, e.g. `elevation_geometry`, `ice_velocity` or `surface_forcing`. Used to browse families. |
| `dataset` | string | Stable product key from the ccdtools registry, e.g. `measures_bedmachine_antarctica`. This is the product identifier, not the filename. |
| `version` | string | Published version directory, e.g. `v1` or `v3`. Versions are never merged. |
| `subdataset` | string | Registry subdataset key, e.g. `gridded`, `points`, `ANT` or `RGI01A`; `none` when the registry has no subdivision. It can describe a format or region, not a universal grid identifier. |
| `asset_id` | string | SHA-256 of the file path relative to the CCD root. Distinguishes files within the same product/version/subdataset. Stable under relocation of the root; **not a content checksum**. |
| `path` | string | Absolute published Gadi file path used by Intake to open the asset. Not a download URL. |
| `format` | string | `netcdf` in the Intake-ESM table. Other formats are in the companion inventory. |
| `display_name` | string | Human-readable product title from ccdtools. |
| `product_description` | string | Per-product description from ccdtools. Kept separate from the common datastore `description` so `DefaultTranslator` can merge collection summaries. |
| `tags` | list of strings | ccdtools discovery keywords, such as `antarctica` and `ice thickness`. |
| `doi` | string or null | Dataset/version/subdataset DOI resolved with ccdtools' override precedence. Missing values remain null. A DOI is not a license statement. |
| `realm` | string | Currently the provisional collection-level label `landIce`. This needs review for surface-forcing/atmospheric variables; it is not yet a validated per-variable realm mapping. |
| `model` | string | Uppercase RACMO name from the product key, or `not_applicable` for observational/derived products. This provisional label prevents observations being assigned an ACCESS model. |
| `ccdtools_loader` | string | Registry loader name (`default`, `measures_velocity`, `racmo`, etc.). Records provenance; this prototype does not invoke the specialist loader. |

## Variables, grids and file provenance

| Field | Type | Source and purpose |
| --- | --- | --- |
| `variable` | list of strings | Root NetCDF variables, excluding dimension coordinates and variables referenced by CF `coordinates`, `bounds` or `grid_mapping`. Provider-specific helper variables can remain and need curator review. |
| `variable_long_name` | list of strings | Each variable's `long_name`; empty string if absent. |
| `variable_standard_name` | list of strings | Each variable's CF `standard_name`; no aliasing or inferred standard names. |
| `variable_units` | list of strings | Each variable's `units`; no conversion is performed. |
| `variable_cell_methods` | list of strings | Each variable's `cell_methods`; retained to distinguish temporal/statistical meaning. |
| `dimensions` | JSON string | Map of NetCDF dimension names to sizes. Useful for inspecting shape, but **equal sizes do not prove grid compatibility**. |
| `grid_mapping` | JSON string | Attributes of referenced variables having `grid_mapping_name`. Original NetCDF attribute values are serialized as strings for discovery. Scientific loading uses the source metadata with `decode_coords="all"`. |
| `size_bytes` | integer | File size from `stat`, for provenance and future change detection. |
| `mtime_ns` | integer | File modification time in nanoseconds, for future change detection. Together with size this is not a checksum or integrity guarantee. |

## Time and frequency

| Field | Type | Source and purpose |
| --- | --- | --- |
| `frequency` | string | Discovery cadence: coordinate-derived when reliable, or a source-backed declared product cadence. `fx` is used only for explicit static-product rules; `unknown` means cadence is unresolved; `irregular` records nonuniform or non-monotonic coordinates. |
| `coordinate_frequency` | string | The result from time-coordinate/static-rule inspection before applying a product cadence. Preserves the evidence when, for example, monthly RACMO timestamps have varying mid-month dates. |
| `frequency_source` | string | `file_time_coordinate_or_static_rule`, or `ccdtools_product_description` when an explicit product rule supplies the cadence. |
| `start_date`, `end_date` | string | First and last decoded coordinate timestamps, formatted `YYYY-MM-DD, HH:MM:SS`, or `none`. These are coordinate endpoints, not time bounds, and are not inferred from filenames. A non-monotonic coordinate remains flagged. |
| `calendar` | string | NetCDF time calendar; CF default `standard` when absent on a time coordinate, or `none` without an identified time axis. Non-Gregorian calendars are preserved. |
| `time_dimension` | string | Dimension of the identified one-dimensional CF time coordinate, or `none`. |
| `temporal_status` | string | How time was handled; see values below. |
| `time_error` | optional string | Decoding error for files whose time coordinate cannot be interpreted. The asset can still be discovered and opened with appropriate loader options. |
| `source_time_coverage` | JSON string | Original global attributes beginning with `time_coverage`, retained separately from decoded coordinate endpoints. |

`temporal_status` values are `declared_static`, `no_time_axis`,
`ambiguous_time_axes`, `undecodable_time`, `singleton_time`, `time_endpoints_only`,
`non_monotonic_time` and `decoded_time`. Above 10,000 coordinate values only the
endpoints are read, and cadence is left unresolved. A singleton cannot establish
cadence. `no_time_axis` does not mean the physical product is timeless.

Explicit product cadence rules cover the two monthly RACMO products (`1mon`),
MEaSUREs annual Antarctic velocity (`1yr`), and ITS_LIVE quarterly ice-shelf height
change (`3mon`). These rules do not override non-monotonic time or authorize any
aggregation. Annual velocity files still have `none` coordinate endpoints where
they have no time axis; filename-based dating is future product-adapter work.

The current ACCESS schemas reject `unknown` and `irregular`. The included patch
and integration test demonstrate a possible extension; the PR does not apply it
to the shipped schemas. The canonical schema version and vocabulary require a
separate decision before production registration.

## Grouping and companion outputs

The Intake descriptor uses
`project, category, dataset, version, subdataset, asset_id` as `groupby_attrs`,
`variable` as `variable_column_name`, and **no aggregation operations**. Each file
is independently loadable even when version, variable and array shape match.
Grid/calendar/units/temporal semantics must be tested before introducing joins.

The inventory contains `dataset`, `category`, `version`, `subdataset`, `doi`,
`path`, file-extension `format`, `status` and, where needed, `reason`. Its statuses
are `indexed`, `inventory_only`, `ancillary`, `excluded`, `invalid` and
`not_sampled`. CSV/GeoPackage/GeoTIFF/shapefile assets are never labelled NetCDF.

The build report distinguishes directory traversal completeness (`complete`)
from header sampling completeness (`header_scan_complete`), records limits,
issues and counts, and keeps `publication_ready=false`. Completeness refers to
registered roots, not every file in av17. The one-row `*_uniqs.parquet` sidecar
contains discovery values and the `num_records` Parquet metadata used by the web
catalog. The metacatalog's `name`, `description` and grouped summaries are produced
by the existing `CatalogManager`/`DefaultTranslator`.
