# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Generate includes for documentation"""

import re
import warnings
from pathlib import Path

import yaml

STORAGE_FLAG_REGEXP = r"^/g/data/(?P<proj>[a-z]{1,2}[0-9]{1,2})/.*?$"


def storage_includes() -> None:
    here = Path(__file__).parent.absolute()

    project_set = set()
    for source_yaml in (here.parent / "config").glob("*.yaml"):
        print(source_yaml)
        with open(source_yaml) as fobj:
            contents = yaml.safe_load(fobj)

        # Loop over the sources in the YAML, extract all storage flags
        # Will ignore anything that doesn't look like /g/data/<flag>/....
        try:
            for source in contents["sources"]:
                metadata_match = re.match(STORAGE_FLAG_REGEXP, source["metadata_yaml"])
                if metadata_match:
                    project_set.add(metadata_match.group("proj"))
                for data_path in source["path"]:
                    data_path_match = re.match(STORAGE_FLAG_REGEXP, data_path)
                    if data_path_match:
                        project_set.add(data_path_match.group("proj"))
        except KeyError:
            warnings.warn(f"Unable to parse config YAML file {source_yaml} - skipping")
            continue

    project_list = list(project_set)
    project_list.sort()

    with open("project_list.rst", "w") as fobj:
        [fobj.write(f"* :code:`{proj}`\n") for proj in project_list]
    storage_string = "+".join([f"gdata/{proj}" for proj in project_list])
    with open("storage_flags.rst", "w") as fobj:
        fobj.write(f".. code-block::\n\n   {storage_string}")

    return None


if __name__ == "__main__":
    print("Generating documentation includes... ", end="")
    storage_includes()
    print("done")
