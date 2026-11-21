# Collection boundaries and production work

Published data were inspected at `/g/data/av17/access-nri`. There are eight
top-level collections. A 15-second-per-collection filesystem survey supplied the
observations below; the counts are lower bounds, except for the empty CABLE
directory. The separate registered CCD header scan was complete.

| Collection | Files observed | Suggested indexing boundary |
| --- | ---: | --- |
| AM3 | 3,373 | Separate model experiments, ERA5 nudging and U-Surf/ESA CCI ancillary products. Assess model-output builders only for the experiments. |
| CABLE | 0 (complete) | No datastore until a published collection exists. |
| OM2 | 808 | Retain online-MLT; index RYF forcing separately if useful. Exclude restart, executable and log files. |
| OM3 | 1,003 | Product-specific observation/forcing stores. WOA23 already has a configuration. Assess TPXO10, EN4, GEBCO, GLODAP and other products separately. |
| SHEP | 635 | Separate observations and model outputs, with site/station, configuration, variable and time. Further header/owner metadata work is needed. |
| cryosphere-data-pool | 2,984 in broad survey | Dedicated CCD NetCDF datastore plus mixed-format inventory; deeper registry scan indexed 978 NetCDF files. |
| esmvaltool | 635 | Separate native and CMORised products; check overlap with the existing ESMValTool observations catalog and retain access terms. |
| rOM3 | 810 | Configuration-specific model-output entries. Inspect headers before selecting a builder; exclude job logs and restarts. |

`config/woa.yaml` and `config/online-mlt.yaml` already provide av17 entry points.
This is evidence of configuration, not a guarantee of deployed-snapshot freshness.
ISMIP6 is under `/g/data/cm45/access-nri/ismip6`, not the av17 root; it requires
its own project/source metadata. See the [CCD documentation](https://docs.access-hive.org.au/model_evaluation/data/cryosphere_datapool/)
and [data-sharing guidance](https://docs.access-hive.org.au/model_evaluation/data/sharing/).

## Why start with one CCD NetCDF datastore?

978 file records are small enough for a single discovery table. Category,
product, version and subdataset provide useful filters. Individual asset IDs
prevent accidental merging, including matching-shaped grids, regional tiles,
composites and annual products. More stores should follow independently owned
refresh or aggregation rules, not arbitrary directory depth.

Before enabling aggregation, test product-specific coordinate equality,
projection, units, masks, calendar and temporal meaning. RACMO variable alignment
and MEaSUREs/ITS_LIVE filename-time/resolution conventions need their own adapters.
Reuse ccdtools knowledge rather than inferring all of these from dimensions or
filename fragments. Original product descriptions are retained separately from
the common collection description for compatibility with `DefaultTranslator`.

## Remaining implementation work

The [standalone review datastore](test-datastore/README.md) is ready for headmetal
to test on Gadi. It can be opened directly with Intake-ESM while the production
decisions below remain open. Its JSON/Parquet files do not register a source in
the production ACCESS metacatalog.

1. Review the [field definitions](FIELDS.md), especially the provisional realm
   and model labels, cadence provenance, filename-based coverage and asset
   identity. Product licenses still need explicit owner-approved metadata; DOI
   alone does not supply license terms.
2. Decide how the canonical ACCESS schemas represent unresolved/irregular time.
   The included patch demonstrates one option but does not version or release
   that schema. Publishing only the subset with resolved cadence is another
   option, provided coverage limitations remain visible.
3. Integrate the adapter as a CCD builder or prebuild feeding
   `CatalogManager.load`. Register collection metadata/storage requirements in
   the normal build and mirror pipeline. No production source is registered here.
4. Reuse normal cloud mirroring for descriptor, Parquet, unique-value sidecar,
   content metadata and metacatalog. The prototype sidecar includes
   `num_records`. Inspection of the interactive catalog's dynamic columns/list
   handling suggests the NetCDF source fits it; deployment/API/browser testing
   remains outstanding. Mixed formats require a separate loader/access-code path.
5. Pin registry revisions, review added/removed products and versions, cache
   header parsing by file identity plus parser/registry revision, detect removals,
   and publish versioned snapshots atomically. The prototype records size/mtime
   but does not implement incremental builds or publishing.

The `publication_ready=false` report makes these limits explicit. The full
registry scan and seven live value reads establish feasibility; they are not a
scientific validation of all files or all av17 collections.
