CMIP7 search with optional ESGF matches (Gadi prototype)
========================================================

This prototype adds ``esgf=True`` to searches on the ``cmip7_replicated`` and
``cmip7_australian`` source datastores. Existing searches use the NCI catalog
as before. ESGF search retrieves metadata only; data transfer occurs when an
ESGF-only result is opened with ``to_dataset_dict()``.

Install this checkout into a Python 3.12 or newer environment on Gadi, with
access to the NCI catalog paths and outbound access to ESGF indices. Use an
isolated environment rather than modifying ``analysis3``::

   python -m pip install -e '.[esgf]'

CMIP7 requires ``intake-esgf>=2026.9.4``. If an older version is already
installed in the environment, upgrade it there before testing::

   python -m pip install --upgrade 'intake-esgf>=2026.9.4'

Use a cache under your project or scratch allocation. Set ``CACHE_DIR`` to a
writable path such as ``/scratch/<project>/<user>/intake-esgf-cache``. For a
first check, run the included script without ``--download``::

   python scripts/gadi_cmip7_esgf_smoke.py \
       --source cmip7_replicated \
       --source-id <CMIP7-source-id> \
       --variable-id <CMIP7-variable-id> \
       --cache "$CACHE_DIR"

The script prints the local file count, ESGF dataset count, ESGF-only count,
and a dataset-level table. Narrow with ``--experiment-id``,
``--variant-label``, ``--frequency``, and ``--grid-label`` as needed. When the
query yields one ESGF-only dataset, adding ``--download`` calls
``to_dataset_dict()`` and asks for transfer confirmation.

For a regex search from the command line, quote the pattern so the shell does
not expand it::

   python scripts/gadi_cmip7_esgf_smoke.py \
       --source-id '^ACCESS-.*$' \
       --variable-id tas \
       --cache "$CACHE_DIR"

In a notebook the equivalent calls are::

   import intake
   import intake_esgf

   with intake_esgf.conf.set(local_cache=["/scratch/path/intake-esgf"]):
       source = intake.cat.access_nri.search(name="cmip7_replicated").to_source()
       result = source.search(esgf=True, source_id="...", variable_id="...")
       result.df[["source_id", "variable_id", "availability"]]
       # This is the first call that may transfer files:
       datasets = result.to_dataset_dict()

Regexes follow intake-esm search rules. For example, keep an exact facet to
bound the ESGF query and use a compiled pattern for the other facet::

   import re

   result = source.search(
       esgf=True,
       source_id=re.compile(r"^ACCESS-.*$"),
       variable_id="tas",
   )

Only exact facets are sent to the ESGF indices; the remaining regexes are
applied to the returned ESGF metadata before reconciliation. With no exact
``source_id``, ``variable_id``, or ``experiment_id``, request a project-wide
scan explicitly with ``esgf_broad=True``. This can be slow and return many
rows. The smoke script exposes the same option as ``--esgf-broad``. As with
intake-esm, strings containing ``*``, ``?``, ``^``, or ``$`` are treated as
patterns; use ``re.compile`` for expressions such as ``tas|pr`` that do not
contain one of those characters.

``result.df`` is a dataset-level union with ``availability`` values ``local``,
``both``, or ``esgf``. ``result.local_df`` retains NCI file rows,
``result.esgf_df`` retains the ESGF search rows, and
``result.esgf_only_df`` identifies the rows eligible for transfer.
Local dictionary keys remain the intake-esm keys. Remote keys use the full
ESGF dataset identity prefixed with ``esgf:``. Pass intake-esm opening options
directly to ``to_dataset_dict()`` and intake-esgf options in
``esgf_kwargs={...}``.

Current limitations
-------------------

* Regex matches require fetching a superset of ESGF metadata. Search time and
  memory use depend on the exact facets supplied. ``require_all_on`` and
  file-level filters cannot be translated to ESGF.
* Reconciliation requires the shared fields ``source_id``,
  ``experiment_id``, ``variant_label`` (or local ``member_id``),
  ``frequency``, ``variable_id``, and ``grid_label`` when both catalogs have
  matches. It raises an error if NCI's live CMIP7 schema lacks one of these,
  so Gadi validation can identify needed mappings without silently merging
  different datasets. Other CMIP7 identity fields, including ``version``,
  are compared when available in both catalogs.
* The opt-in ``df`` is dataset-level, while the original local-only ``df``
  remains file-level. Use ``local_df`` for local file paths and time ranges.
  Other intake-esm methods remain available on ``result.local``; the combined
  result supports ``search`` and ``to_dataset_dict``.
* This searches one CMIP7 NCI source at a time; it does not reconcile the
  replicated and Australian source entries with one another.
* Network availability, cache permissions, data volume, and transfer speed
  depend on the Gadi environment and chosen ESGF index. A live Gadi test on
  26 October 2026 fetched an ESGF-only 467,541-byte CMIP7 ``orog`` file into
  a scratch cache and opened it through ``to_dataset_dict()``.
