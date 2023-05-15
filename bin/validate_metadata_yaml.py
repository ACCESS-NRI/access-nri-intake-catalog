import argparse
import glob

from access_nri_intake.metacat import METADATA_JSONSCHEMA
from access_nri_intake.utils import load_metadata_yaml, validate_against_schema


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
        instance = load_metadata_yaml(f)

        print(f"Validating {f}... ", end="")

        validate_against_schema(instance, METADATA_JSONSCHEMA)

        print("success")


if __name__ == "__main__":
    main()
