# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Generate includes for documentation """

import os

import yaml


def storage_includes():
    here = os.path.abspath(os.path.dirname(__file__))
    with open(
        os.path.join(here, "..", "src", "access_nri_intake", "data", "catalog.yaml")
    ) as fobj:
        contents = yaml.safe_load(fobj)
    storage_flags = contents["sources"]["access_nri"]["metadata"]["storage"]
    project_list = [
        f"* :code:`{proj.removeprefix('gdata/')}`" for proj in storage_flags.split("+")
    ]
    with open("storage_flags.rst", "w") as fobj:
        fobj.write(f".. code-block::\n\n   {storage_flags}")
    with open("project_list.rst", "w") as fobj:
        fobj.write("\n".join(project_list) + "\n")


def generate_storage_flags():
    here = os.path.abspath(os.path.dirname(__file__))
    with open(
        os.path.join(here, "..", "src", "access_nri_intake", "data", "catalog.yaml")
    ) as fobj:
        contents = yaml.safe_load(fobj)
    storage_flags = contents["sources"]["access_nri"]["metadata"]["storage"]
    with open("storage_flags.rst", "w") as fobj:
        fobj.write(f".. code-block::\n\n   {storage_flags}")


if __name__ == "__main__":
    print("Generating documentation includes... ", end="")
    storage_includes()
    print("done")
