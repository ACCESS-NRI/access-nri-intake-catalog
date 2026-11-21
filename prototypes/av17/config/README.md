# Cryosphere registry provenance

`ccd-datasets.yaml` is a snapshot (with trailing whitespace removed) of
[ACCESS-NRI/ccdtools, src/ccdtools/config/datasets.yaml](https://github.com/ACCESS-NRI/ccdtools/blob/main/src/ccdtools/config/datasets.yaml).
The source tree/blob identifiers and original/vendored SHA-256 values are in `../evidence/provenance.json`.
The upstream work is licensed under Apache-2.0; see the repository's `LICENSE`.

The prototype preserves the upstream dataset IDs, versions, subdataset paths,
DOIs, loader names and exclusion rules. It resolves versions from the published
directories, as ccdtools does. It does not evaluate or import upstream loader code.

Additional prototype policies live in `av17_catalog.py`: static product rules,
source-backed cadence rules for the two monthly RACMO products, annual MEaSUREs
velocity and quarterly ITS_LIVE height change, a provisional collection-level
`landIce` realm, and `not_applicable` as the model label for observational products.
Review those mappings with the data owners before publication. The original
coordinate-spacing result is retained separately from the declared cadence.

In production, pin the ccdtools registry revision and regenerate from it. Avoid
maintaining an independently edited second list of CCD products. Require a review
for newly discovered versions, changed DOI/loader rules or removed files.
