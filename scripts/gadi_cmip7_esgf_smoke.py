"""Exercise opt-in CMIP7 ESGF discovery on Gadi.

Run with --download only after inspecting the matches. That mode requires a
single ESGF-only match and asks for confirmation before transfer.
"""

import argparse
import os
from pathlib import Path

import intake
import intake_esgf


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        choices=("cmip7_replicated", "cmip7_australian"),
        default="cmip7_replicated",
    )
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--variable-id", required=True)
    parser.add_argument("--experiment-id")
    parser.add_argument("--variant-label")
    parser.add_argument("--frequency")
    parser.add_argument("--grid-label")
    parser.add_argument("--cache", type=Path)
    parser.add_argument(
        "--esgf-broad",
        action="store_true",
        help="Permit a project-wide ESGF scan when no exact source, variable, or experiment is given",
    )
    parser.add_argument("--download", action="store_true")
    args = parser.parse_args()

    cache = args.cache
    if cache is None:
        scratch = os.environ.get("SCRATCH")
        if not scratch:
            parser.error("Set --cache or SCRATCH to a writable location")
        cache = Path(scratch) / "intake-esgf-cache"

    facets = {"source_id": args.source_id, "variable_id": args.variable_id}
    for key in ("experiment_id", "variant_label", "frequency", "grid_label"):
        value = getattr(args, key)
        if value:
            facets[key] = value

    with intake_esgf.conf.set(local_cache=[str(cache)], confirm_download=True):
        source = intake.cat.access_nri.search(name=args.source).to_source()
        result = source.search(esgf=True, esgf_broad=args.esgf_broad, **facets)
        print(f"ESGF search status: {result.esgf_search_status}")
        print(f"NCI file rows: {len(result.local_df)}")
        print(f"ESGF dataset rows: {len(result.esgf_df)}")
        print(f"ESGF-only datasets: {len(result.esgf_only_df)}")
        display_columns = [
            column
            for column in (
                "source_id",
                "experiment_id",
                "variant_label",
                "frequency",
                "variable_id",
                "grid_label",
                "version",
                "availability",
            )
            if column in result.df
        ]
        print(result.df[display_columns].to_string(index=False, max_rows=30))

        if args.download:
            if len(result.df) != 1 or len(result.esgf_only_df) != 1:
                parser.error(
                    "--download requires exactly one ESGF-only result; "
                    "refine the facets and rerun"
                )
            datasets = result.to_dataset_dict(esgf_kwargs={"quiet": True})
            print(f"Opened dataset keys: {list(datasets)}")
            print(f"Cache: {cache}")


if __name__ == "__main__":
    main()
