# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file.
# SPDX-License-Identifier: Apache-2.0

"""Exercise the existing ACCESS CatalogManager without modifying its checkout.

Run with PYTHONPATH pointing to access-nri-intake-catalog/src. The optional schema
extension is applied in memory, for this experiment only, and then restored.
"""

import argparse
import copy
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import intake
import jsonschema
from intake_dataframe_catalog.core import DfFileCatalog

from access_nri_intake.catalog import manager as manager_module
from access_nri_intake.catalog.manager import CatalogManager
from access_nri_intake.source import ESM_JSONSCHEMA


def check(descriptor, output):
    descriptor, output = Path(descriptor).resolve(), Path(output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    kwargs = dict(
        name="cryosphere_av17",
        description="Cryosphere Community Datapool NetCDF prototype",
        path=str(descriptor),
        directory=str(output),
    )
    report = {}
    datastore = intake.open_esm_datastore(str(descriptor))
    file_schema = copy.deepcopy(ESM_JSONSCHEMA)
    file_schema["properties"]["frequency"]["oneOf"].append(
        {"enum": ["unknown", "irregular"]}
    )
    # JSON roundtrip makes Parquet numpy list values ordinary JSON arrays.
    records = json.loads(datastore.df.to_json(orient="records"))
    validator = jsonschema.Draft202012Validator(file_schema)
    for row in records:
        validator.validate(row)
    report["file_rows_validated_with_prototype_schema"] = len(records)
    # CatalogManager creates an empty output file before validating. Isolate
    # the expected failing probe so it does not break a subsequent run.
    with TemporaryDirectory() as probe:
        manager = CatalogManager(Path(probe) / "metacatalog.parquet", use_parquet=True)
        try:
            manager.load(**{**kwargs, "directory": probe})
            report["original_schema"] = "accepted"
        except jsonschema.ValidationError as exc:
            report["original_schema"] = "rejected"
            report["original_schema_error"] = exc.message

    original = manager_module.CATALOG_JSONSCHEMA
    extended = copy.deepcopy(original)
    choices = extended["properties"]["frequency"]["items"]["oneOf"]
    choices.append({"enum": ["unknown", "irregular"]})
    manager_module.CATALOG_JSONSCHEMA = extended
    try:
        # This command rebuilds its generated prototype metacatalog on each run.
        (output / "metacatalog.parquet").unlink(missing_ok=True)
        manager = CatalogManager(output / "metacatalog.parquet", use_parquet=True)
        manager.load(**kwargs)
        manager.save()
        report["prototype_schema"] = "accepted"
        report["metacatalog_rows"] = len(manager.dfcat.df)
        loaded = intake.open_esm_datastore(str(output / "cryosphere_av17.json"))
        report["reloaded_asset_rows"] = len(loaded.df)
        report["independent_datasets"] = len(loaded.keys())
        report["bedmachine_v3_bed_matches"] = len(
            loaded.search(
                dataset="measures_bedmachine_antarctica", version="v3", variable="bed"
            ).df
        )
        report["production_modified"] = False
        reopened = DfFileCatalog(
            path=str(output / "metacatalog.parquet"),
            yaml_column="yaml",
            name_column="name",
            mode="r",
        )
        report["metacatalog_roundtrip_asset_rows"] = len(reopened["cryosphere_av17"].df)
    finally:
        manager_module.CATALOG_JSONSCHEMA = original
    (output / "integration-report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("descriptor")
    parser.add_argument("output")
    args = parser.parse_args()
    print(json.dumps(check(args.descriptor, args.output), indent=2))
