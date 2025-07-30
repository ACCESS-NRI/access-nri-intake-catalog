import pytest
import yaml

import access_nri_intake.catalog.translators as translators
from access_nri_intake.cli import build

from . import e2e


@e2e
@pytest.mark.parametrize(
    "translator_name",
    [t for t in translators.__all__],
)
def test_alignment(translator_name, live_config_dir, BASE_DIR, v_num):
    # Now live test the translator. Honestly, might be overkill - it might be easier
    # to just extract the json files, open them, check they match the test data
    filenames = [f for f in live_config_dir.glob("*.yaml")]
    # Now we want to open them & throw away anything where builder != null.
    translator_fnames = []

    for fname in filenames:
        with open(fname) as fo:
            catalog_metadata = yaml.load(fo, yaml.FullLoader)
            if catalog_metadata["translator"] == translator_name:
                translator_fnames.append(str(fname))

    assert len(translator_fnames) == 1

    try:
        build(
            [
                *translator_fnames,
                "--build_base_path",
                str(BASE_DIR),
                "--catalog_base_path",
                str(BASE_DIR),
                "--catalog_file",
                "metacatalog.csv",
                "--version",
                v_num,
                "--no_update",
                "--no_concretize",
            ]
        )
    except Exception as exc:
        assert False, f"Failed to build {translator_name} with exception {exc}"
