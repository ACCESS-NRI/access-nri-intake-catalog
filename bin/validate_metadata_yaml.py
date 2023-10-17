#!/usr/bin/env python

# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import argparse
import glob

from access_nri_intake.catalog import EXP_JSONSCHEMA
from access_nri_intake.utils import load_metadata_yaml


def main():

    parser = argparse.ArgumentParser(
        description="Validate the schema of a metadata.yaml file"
    )
    parser.add_argument(
        "file",
        type=str,
        help="The path to the metadata.yaml file (can include wildcards for multiple metadata.yaml)",
    )

    args = parser.parse_args()
    file = args.file

    for f in glob.glob(file):
        print(f"Validating {f}... ", end="")
        load_metadata_yaml(f, EXP_JSONSCHEMA)
        print("success")


if __name__ == "__main__":
    main()
