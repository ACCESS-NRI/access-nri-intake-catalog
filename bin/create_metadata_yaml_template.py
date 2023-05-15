import argparse

import yaml

from access_nri_intake.metacat import METADATA_JSONSCHEMA


def main():
    argparse.ArgumentParser(
        description="Generate a template for metadata.yaml from the ACCESS-NRI schema"
    )

    template = {}
    for name, descr in METADATA_JSONSCHEMA["properties"].items():
        if name in METADATA_JSONSCHEMA["required"]:
            description = f"<REQUIRED {descr['description']}>"
        else:
            description = f"<{descr['description']}>"

        if descr["type"] == "array":
            description = [description]

        template[name] = description

    with open("../metadata.yaml", "w") as outfile:
        yaml.dump(template, outfile, default_flow_style=False, sort_keys=False)


if __name__ == "__main__":
    main()