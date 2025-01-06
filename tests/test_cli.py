# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import glob
import os
import shutil
from pathlib import Path
from unittest import mock

import intake
import pytest
import yaml

from access_nri_intake.cli import (
    MetadataCheckError,
    _check_build_args,
    build,
    metadata_template,
    metadata_validate,
)


def test_entrypoint():
    """
    Check that entry point works
    """
    exit_status = os.system("catalog-build --help")
    assert exit_status == 0

    exit_status = os.system("metadata-validate --help")
    assert exit_status == 0

    exit_status = os.system("metadata-template --help")
    assert exit_status == 0


@pytest.mark.parametrize(
    "args, raises",
    [
        (
            [
                {
                    "name": "exp0",
                    "metadata": {
                        "experiment_uuid": "214e8e6d-3bc5-4353-98d3-b9e9a5507d4b"
                    },
                },
                {
                    "name": "exp1",
                    "metadata": {
                        "experiment_uuid": "7b0bc2c6-7cbb-4d97-8eb9-b0255c16d910"
                    },
                },
            ],
            False,
        ),
        (
            [
                {
                    "name": "exp0",
                    "metadata": {
                        "experiment_uuid": "214e8e6d-3bc5-4353-98d3-b9e9a5507d4b"
                    },
                },
                {
                    "name": "exp0",
                    "metadata": {
                        "experiment_uuid": "7b0bc2c6-7cbb-4d97-8eb9-b0255c16d910"
                    },
                },
            ],
            True,
        ),
        (
            [
                {
                    "name": "exp0",
                    "metadata": {
                        "experiment_uuid": "214e8e6d-3bc5-4353-98d3-b9e9a5507d4b"
                    },
                },
                {
                    "name": "exp1",
                    "metadata": {
                        "experiment_uuid": "214e8e6d-3bc5-4353-98d3-b9e9a5507d4b"
                    },
                },
            ],
            True,
        ),
    ],
)
def test_check_build_args(args, raises):
    """
    Check that non-unique names and uuids return an error
    """
    if raises:
        with pytest.raises(MetadataCheckError) as excinfo:
            _check_build_args(args)
        assert "exp0" in str(excinfo.value)
    else:
        _check_build_args(args)


@pytest.mark.parametrize(
    "version",
    [
        "v2024-01-01",
        "2024-01-01",
    ],
)
def test_build(version, test_data, tmp_path):
    """Test full catalog build process from config files"""
    # Update the config_yaml paths
    build_base_path = str(tmp_path)

    configs = [
        str(test_data / fname)
        for fname in ["config/access-om2.yaml", "config/cmip5.yaml"]
    ]

    build(
        [
            *configs,
            "--catalog_file",
            "cat.csv",
            "--no_update",
            "--version",
            version,
            "--build_base_path",
            build_base_path,
            "--catalog_base_path",
            build_base_path,
            "--data_base_path",
            str(test_data),
        ]
    )

    # manually fix the version so we can correctly build the test path: build
    # will do this for us so we need to replicate it here
    if not version.startswith("v"):
        version = f"v{version}"

    # Try to open the catalog
    build_path = Path(build_base_path) / version / "cat.csv"
    cat = intake.open_df_catalog(build_path)
    assert len(cat) == 2


@pytest.mark.parametrize(
    "bad_vers",
    [
        # "2024-01-01",  # Forgot the leading v - BUT the build process will fix it
        "v2024_01_01",  # Underscores instead of spaces
        "v1999-02-01",  # Regex only accepts dates > 2000-01-01
        "v2024-01-32",  # Out of bounds day (not month-sensitive)
        "v2024-01-00",  # Out of bounds day
        "v2024-13-02",  # Out of bounds month
        "v2024-00-02",  # Out of bounds month
        "v2024-01-01-243",  # Trailing gumph
        "v2024-01-02-v2",  # Trailing gumph
        "v0.1.2",  # Old-style version numbers
    ],
)
def test_build_bad_version(bad_vers, test_data, tmp_path):
    """Test full catalog build process from config files"""
    # Update the config_yaml paths
    build_base_path = str(tmp_path)

    configs = [
        str(test_data / fname)
        for fname in ["config/access-om2.yaml", "config/cmip5.yaml"]
    ]

    with pytest.raises(ValueError):
        build(
            [
                *configs,
                "--catalog_file",
                "cat.csv",
                "--no_update",
                "--version",
                bad_vers,
                "--build_base_path",
                build_base_path,
                "--catalog_base_path",
                build_base_path,
                "--data_base_path",
                "",
            ]
        )


def test_build_bad_metadata(test_data, tmp_path):
    """
    Test if bad metadata is detected
    """

    configs = [
        str(test_data / "config/access-om2-bad.yaml"),
        str(test_data / "config/cmip5.yaml"),
    ]
    data_base_path = str(test_data)
    build_base_path = str(tmp_path)

    with pytest.raises(MetadataCheckError):
        build(
            [
                *configs,
                "--catalog_file",
                "cat.csv",
                "--data_base_path",
                data_base_path,
                "--build_base_path",
                build_base_path,
                "--catalog_base_path",
                build_base_path,
                "--version",
                "v2024-01-01",
                "--no_update",
            ]
        )


def test_build_repeat_nochange(test_data, tmp_path):
    """
    Test if the intelligent versioning works correctly when there is
    no significant change to the underlying catalogue
    """
    configs = [
        str(test_data / "config/access-om2.yaml"),
        str(test_data / "config/cmip5.yaml"),
    ]
    data_base_path = str(test_data)
    build_base_path = str(tmp_path)

    build(
        [
            *configs,
            "--catalog_file",
            "cat.csv",
            "--data_base_path",
            data_base_path,
            "--build_base_path",
            build_base_path,
            "--catalog_base_path",
            build_base_path,
            "--version",
            "v2024-01-01",
        ]
    )

    # Update the version number and have another crack at building
    NEW_VERSION = "v2024-01-02"
    build(
        [
            *configs,
            "--catalog_file",
            "cat.csv",
            "--data_base_path",
            data_base_path,
            "--build_base_path",
            build_base_path,
            "--catalog_base_path",
            build_base_path,
            "--version",
            NEW_VERSION,
        ]
    )

    # There is no change between catalogs, so we should be able to
    # see just a version number change in the yaml
    with (tmp_path / "catalog.yaml").open(mode="r") as fobj:
        cat_yaml = yaml.safe_load(fobj)

    assert (
        cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("min")
        == "v2024-01-01"
    ), f'Min version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("min")} does not match expected v2024-01-01'
    assert (
        cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("max")
        == "v2024-01-02"
    ), f'Max version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("max")} does not match expected v2024-01-02'
    assert (
        cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("default")
        == "v2024-01-02"
    ), f'Default version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("default")} does not match expected v2024-01-02'


def test_build_repeat_adddata(test_data, tmp_path):
    configs = [
        str(test_data / "config/access-om2.yaml"),
    ]
    data_base_path = str(test_data)
    build_base_path = str(tmp_path)

    # Build the first catalog
    build(
        [
            *configs,
            "--catalog_file",
            "cat.csv",
            "--data_base_path",
            data_base_path,
            "--build_base_path",
            build_base_path,
            "--catalog_base_path",
            build_base_path,
            "--version",
            "v2024-01-01",
        ]
    )

    configs.append(str(test_data / "config/cmip5.yaml"))
    NEW_VERSION = "v2024-01-02"

    build(
        [
            *configs,
            "--catalog_file",
            "cat.csv",
            "--data_base_path",
            data_base_path,
            "--build_base_path",
            build_base_path,
            "--catalog_base_path",
            build_base_path,
            "--version",
            NEW_VERSION,
        ]
    )

    # There is no change between catalogs, so we should be able to
    # see just a version number change in the yaml
    with Path(tmp_path / "catalog.yaml").open(mode="r") as fobj:
        cat_yaml = yaml.safe_load(fobj)

    assert (
        cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("min")
        == "v2024-01-01"
    ), f'Min version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("min")} does not match expected v2024-01-01'
    assert (
        cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("max")
        == "v2024-01-02"
    ), f'Max version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("max")} does not match expected v2024-01-02'
    assert (
        cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("default")
        == "v2024-01-02"
    ), f'Default version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("default")} does not match expected v2024-01-02'
    assert cat_yaml["sources"]["access_nri"]["metadata"]["storage"] == "gdata/al33"


@pytest.mark.parametrize(
    "min_vers,max_vers",
    [
        ("v2001-01-01", "v2099-01-01"),
        (None, "v2099-01-01"),
        ("v2001-01-01", None),
    ],
)
def test_build_existing_data(test_data, min_vers, max_vers, tmp_path):
    """
    Test if the build process can handle min and max catalog
    versions when an original catalog.yaml does not exist
    """
    configs = [
        str(test_data / "config/access-om2.yaml"),
        str(test_data / "config/cmip5.yaml"),
    ]
    data_base_path = str(test_data)
    build_base_path = str(tmp_path)
    VERSION = "v2024-01-01"

    # Put dummy version folders into the tempdir
    if min_vers is not None:
        (tmp_path / min_vers).mkdir(parents=True, exist_ok=False)
    if max_vers is not None:
        (tmp_path / max_vers).mkdir(parents=True, exist_ok=False)

    build(
        [
            *configs,
            "--catalog_file",
            "cat.csv",
            "--data_base_path",
            data_base_path,
            "--build_base_path",
            build_base_path,
            "--catalog_base_path",
            build_base_path,
            "--version",
            VERSION,
        ]
    )

    with (tmp_path / "catalog.yaml").open(mode="r") as fobj:
        cat_yaml = yaml.safe_load(fobj)

    assert cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("min") == (
        min_vers if min_vers is not None else VERSION
    ), f'Min version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("min")} does not match expected {min_vers if min_vers is not None else VERSION}'
    assert cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("max") == (
        max_vers if max_vers is not None else VERSION
    ), f'Max version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("max")} does not match expected {max_vers if max_vers is not None else VERSION}'
    # Default should always be the newly-built version
    assert (
        cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("default")
        == VERSION
    ), f'Default version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("default")} does not match expected {VERSION}'


@pytest.mark.parametrize(
    "min_vers,max_vers",
    [
        ("v2001-01-01", "v2099-01-01"),
        (None, "v2099-01-01"),
        ("v2001-01-01", None),
    ],
)
def test_build_existing_data_existing_old_cat(test_data, min_vers, max_vers, tmp_path):
    """
    Test if the build process can handle min and max catalog
    versions when a old-style catalog.yaml exists
    """
    configs = [
        str(test_data / "config/access-om2.yaml"),
        str(test_data / "config/cmip5.yaml"),
    ]
    data_base_path = str(test_data)
    build_base_path = str(tmp_path)
    VERSION = "v2024-01-01"

    # Put dummy version folders into the tempdir
    if min_vers is not None:
        (tmp_path / min_vers).mkdir(parents=True, exist_ok=False)
    if max_vers is not None:
        (tmp_path / max_vers).mkdir(parents=True, exist_ok=False)

    # Copy the test data old-style catalog yaml to this location
    shutil.copy(test_data / "catalog/catalog-orig.yaml", str(tmp_path / "catalog.yaml"))

    build(
        [
            *configs,
            "--catalog_file",
            "cat.csv",
            "--data_base_path",
            data_base_path,
            "--build_base_path",
            build_base_path,
            "--catalog_base_path",
            build_base_path,
            "--version",
            "v2024-01-01",
        ]
    )

    with (tmp_path / "catalog.yaml").open(mode="r") as fobj:
        cat_yaml = yaml.safe_load(fobj)

    assert cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("min") == (
        min_vers if min_vers is not None else VERSION
    ), f'Min version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("min")} does not match expected {min_vers if min_vers is not None else VERSION}'
    assert cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("max") == (
        max_vers if max_vers is not None else VERSION
    ), f'Max version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("max")} does not match expected {max_vers if max_vers is not None else VERSION}'
    # Default should always be the newly-built version
    assert (
        cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("default")
        == VERSION
    ), f'Default version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("default")} does not match expected {VERSION}'
    # Make sure the catalog storage flags were correctly merged
    assert (
        cat_yaml["sources"]["access_nri"]["metadata"]["storage"]
        == "gdata/al33+gdata/dc19+gdata/fs38+gdata/oi10+gdata/tm70"
    )
    # Make sure the old catalog vanished (i.e. there's only one)
    assert (
        len(glob.glob(build_base_path + "/*.yaml")) == 1
    ), "Found more than one catalog remains!"


@pytest.mark.parametrize(
    "min_vers,max_vers",
    [
        ("v2001-01-01", "v2099-01-01"),
        (None, "v2099-01-01"),
        ("v2001-01-01", None),
    ],
)
def test_build_separation_between_catalog_and_buildbase(
    test_data, min_vers, max_vers, tmp_path
):
    """
    Test if the intelligent versioning works correctly when there is
    no significant change to the underlying catalogue
    """

    configs = [
        str(test_data / "config/access-om2.yaml"),
        str(test_data / "config/cmip5.yaml"),
    ]
    data_base_path = str(test_data)
    build_base_path = str(tmp_path)
    VERSION = "v2024-01-01"

    bbp, catdir = tmp_path / "bbp", tmp_path / "catdir"
    bbp.mkdir(parents=True), catdir.mkdir(parents=True)

    # Write the catalog.yamls to its own directory
    catalog_fp = Path(catdir) / "catalog.yaml"

    # Create dummy version folders in the *catalog* directory
    # (They would normally be in the build directory)
    if min_vers is not None:
        (catdir / min_vers).mkdir(parents=True, exist_ok=False)
    if max_vers is not None:
        (catdir / max_vers).mkdir(parents=True, exist_ok=False)

    build(
        [
            *configs,
            "--catalog_file",
            "cat.csv",
            "--data_base_path",
            data_base_path,
            "--build_base_path",
            build_base_path,
            "--catalog_base_path",
            str(catdir),
            "--version",
            VERSION,
        ]
    )

    # The version folders exist in the catalog directory, not the build
    # directory, hence they shouldn't have been found - therefore,
    # all the version numbers should align with the newly-built catalog
    with catalog_fp.open(mode="r") as fobj:
        cat_yaml = yaml.safe_load(fobj)

    assert (
        cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("min") == VERSION
    ), f'Min version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("min")} does not match expected v2024-01-01'
    assert (
        cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("max") == VERSION
    ), f'Max version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("max")} does not match expected v2024-01-01'
    assert (
        cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("default")
        == VERSION
    ), f'Default version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("default")} does not match expected v2024-01-01'


@mock.patch("access_nri_intake.cli.get_catalog_fp")
@pytest.mark.parametrize(
    "min_vers,max_vers",
    [
        ("v2001-01-01", "v2099-01-01"),
        (None, "v2099-01-01"),
        ("v2001-01-01", None),
    ],
)
def test_build_repeat_renamecatalogyaml(
    get_catalog_fp, test_data, min_vers, max_vers, tmp_path
):
    configs = [
        str(test_data / "config/access-om2.yaml"),
    ]
    data_base_path = str(test_data)
    build_base_path = str(tmp_path)
    VERSION = "v2024-01-01"

    # Write the catalog.yamls to where the catalogs go
    get_catalog_fp.return_value = str(tmp_path / "catalog.yaml")

    # Build the first catalog
    build(
        [
            *configs,
            "--catalog_file",
            "cat.csv",
            "--data_base_path",
            data_base_path,
            "--build_base_path",
            build_base_path,
            "--catalog_base_path",
            build_base_path,
            "--version",
            VERSION,
        ]
    )

    # Update the version number, *and* the catalog name
    NEW_VERSION = "v2025-01-01"
    get_catalog_fp.return_value = str(tmp_path / "metacatalog.yaml")
    # Put dummy version folders into the tempdir
    # The new catalog will consider these, as the catalog.yaml
    # names are no longer consistent
    if min_vers is not None:
        (tmp_path / min_vers).mkdir(parents=True, exist_ok=False)
    if max_vers is not None:
        (tmp_path / max_vers).mkdir(parents=True, exist_ok=False)

    # Build another catalog
    build(
        [
            *configs,
            "--catalog_file",
            "cat.csv",
            "--data_base_path",
            data_base_path,
            "--build_base_path",
            build_base_path,
            "--catalog_base_path",
            build_base_path,
            "--version",
            NEW_VERSION,
        ]
    )

    # There should now be two catalogs - catalog.yaml and metacatalog.yaml
    with (tmp_path / "catalog.yaml").open(mode="r") as fobj:
        cat_first = yaml.safe_load(fobj)
    with (tmp_path / "metacatalog.yaml").open(mode="r") as fobj:
        cat_second = yaml.safe_load(fobj)

    assert (
        cat_first["sources"]["access_nri"]["parameters"]["version"].get("min")
        == "v2024-01-01"
    ), f'Min version {cat_first["sources"]["access_nri"]["parameters"]["version"].get("min")} does not match expected v2024-01-01'
    assert (
        cat_first["sources"]["access_nri"]["parameters"]["version"].get("max")
        == "v2024-01-01"
    ), f'Max version {cat_first["sources"]["access_nri"]["parameters"]["version"].get("max")} does not match expected v2024-01-01'
    assert (
        cat_first["sources"]["access_nri"]["parameters"]["version"].get("default")
        == "v2024-01-01"
    ), f'Default version {cat_first["sources"]["access_nri"]["parameters"]["version"].get("default")} does not match expected v2024-01-01'

    assert (
        cat_second["sources"]["access_nri"]["parameters"]["version"].get("min")
        == min_vers
        if min_vers is not None
        else VERSION
    ), f'Min version {cat_second["sources"]["access_nri"]["parameters"]["version"].get("min")} does not match expected {min_vers if min_vers is not None else VERSION}'
    assert (
        cat_second["sources"]["access_nri"]["parameters"]["version"].get("max")
        == max_vers
        if max_vers is not None
        else VERSION
    ), f'Max version {cat_second["sources"]["access_nri"]["parameters"]["version"].get("max")} does not match expected {max_vers if max_vers is not None else VERSION}'
    assert (
        cat_second["sources"]["access_nri"]["parameters"]["version"].get("default")
        == "v2025-01-01"
    ), f'Default version {cat_second["sources"]["access_nri"]["parameters"]["version"].get("default")} does not match expected v2025-01-01'


@pytest.mark.parametrize(
    "min_vers,max_vers",
    [
        ("v2001-01-01", "v2099-01-01"),
        (None, "v2099-01-01"),
        ("v2001-01-01", None),
    ],
)
def test_build_repeat_altercatalogstruct(test_data, min_vers, max_vers, tmp_path):
    configs = [
        str(test_data / "config/access-om2.yaml"),
    ]
    data_base_path = str(test_data)
    build_base_path = str(tmp_path)

    # Build the first catalog
    build(
        [
            *configs,
            "--catalog_file",
            "cat.csv",
            "--data_base_path",
            data_base_path,
            "--build_base_path",
            build_base_path,
            "--catalog_base_path",
            build_base_path,
            "--version",
            "v2024-01-01",
        ]
    )

    # Update the version number, *and* the catalog name
    NEW_VERSION = "v2025-01-01"
    # Put dummy version folders into the tempdir
    # The new catalog will *not* consider these, as the catalog.yaml
    # names are no longer consistent
    if min_vers is not None:
        (tmp_path / min_vers).mkdir(parents=True, exist_ok=False)
    if max_vers is not None:
        (tmp_path / max_vers).mkdir(parents=True, exist_ok=False)

    # Build another catalog
    build(
        [
            *configs,
            "--catalog_file",
            "new_cat.csv",
            "--data_base_path",
            data_base_path,
            "--build_base_path",
            build_base_path,
            "--catalog_base_path",
            build_base_path,
            "--version",
            NEW_VERSION,
        ]
    )

    # There should now be two catalogs - catalog.yaml and catalog-v2024-01-01.yaml
    with (tmp_path / "catalog-v2024-01-01.yaml").open(mode="r") as fobj:
        cat_first = yaml.safe_load(fobj)
    with (tmp_path / "catalog.yaml").open(mode="r") as fobj:
        cat_second = yaml.safe_load(fobj)

    assert (
        cat_first["sources"]["access_nri"]["parameters"]["version"].get("min")
        == "v2024-01-01"
    ), f'Min version {cat_first["sources"]["access_nri"]["parameters"]["version"].get("min")} does not match expected v2024-01-01'
    assert (
        cat_first["sources"]["access_nri"]["parameters"]["version"].get("max")
        == "v2024-01-01"
    ), f'Max version {cat_first["sources"]["access_nri"]["parameters"]["version"].get("max")} does not match expected v2024-01-01'
    assert (
        cat_first["sources"]["access_nri"]["parameters"]["version"].get("default")
        == "v2024-01-01"
    ), f'Default version {cat_first["sources"]["access_nri"]["parameters"]["version"].get("default")} does not match expected v2024-01-01'

    assert (
        cat_second["sources"]["access_nri"]["parameters"]["version"].get("min")
        == NEW_VERSION
    ), f'Min version {cat_second["sources"]["access_nri"]["parameters"]["version"].get("min")} does not match expected {NEW_VERSION}'
    assert (
        cat_second["sources"]["access_nri"]["parameters"]["version"].get("max")
        == NEW_VERSION
    ), f'Max version {cat_second["sources"]["access_nri"]["parameters"]["version"].get("max")} does not match expected {NEW_VERSION}'
    assert (
        cat_second["sources"]["access_nri"]["parameters"]["version"].get("default")
        == NEW_VERSION
    ), f'Default version {cat_second["sources"]["access_nri"]["parameters"]["version"].get("default")} does not match expected {NEW_VERSION}'


@pytest.mark.parametrize(
    "min_vers,max_vers",
    [
        ("v2001-01-01", "v2099-01-01"),
        (None, "v2099-01-01"),
        ("v2001-01-01", None),
    ],
)
def test_build_repeat_altercatalogstruct_multivers(
    test_data, min_vers, max_vers, tmp_path
):
    configs = [
        str(test_data / "config/cmip5.yaml"),
    ]
    data_base_path = str(test_data)
    build_base_path = str(tmp_path)

    # Put dummy version folders into the tempdir - these will
    # be picked up by the first catalog
    if min_vers is not None:
        (tmp_path / min_vers).mkdir(parents=True, exist_ok=False)
    if max_vers is not None:
        (tmp_path / max_vers).mkdir(parents=True, exist_ok=False)

    # Build the first catalog
    build(
        [
            *configs,
            "--catalog_file",
            "cat.csv",
            "--data_base_path",
            data_base_path,
            "--build_base_path",
            build_base_path,
            "--catalog_base_path",
            build_base_path,
            "--version",
            "v2024-01-01",
        ]
    )

    # Update the version number, *and* the catalog name
    NEW_VERSION = "v2025-01-01"

    # Build another catalog
    build(
        [
            *configs,
            "--catalog_file",
            "new_cat.csv",
            "--data_base_path",
            data_base_path,
            "--build_base_path",
            build_base_path,
            "--catalog_base_path",
            build_base_path,
            "--version",
            NEW_VERSION,
        ]
    )

    # There should now be two catalogs - catalog.yaml and catalog-<min>-<max>.yaml
    cat_first_name = f"catalog-{min_vers if min_vers is not None else 'v2024-01-01'}"
    if max_vers is not None or min_vers is not None:
        cat_first_name += f"-{max_vers if max_vers is not None else 'v2024-01-01'}"
    cat_first_name += ".yaml"
    with (tmp_path / cat_first_name).open(mode="r") as fobj:
        cat_first = yaml.safe_load(fobj)
    with (tmp_path / "catalog.yaml").open(mode="r") as fobj:
        cat_second = yaml.safe_load(fobj)

    assert (
        cat_first["sources"]["access_nri"]["parameters"]["version"].get("min")
        == min_vers
        if min_vers is not None
        else "v2024-01-01"
    ), f'Min version {cat_first["sources"]["access_nri"]["parameters"]["version"].get("min")} does not match expected {min_vers if min_vers is not None else "v2024-01-01"}'
    assert (
        cat_first["sources"]["access_nri"]["parameters"]["version"].get("max")
        == max_vers
        if max_vers is not None
        else "v2024-01-01"
    ), f'Max version {cat_first["sources"]["access_nri"]["parameters"]["version"].get("max")} does not match expected {max_vers if max_vers is not None else "v2024-01-01"}'
    assert (
        cat_first["sources"]["access_nri"]["parameters"]["version"].get("default")
        == "v2024-01-01"
    ), f'Default version {cat_first["sources"]["access_nri"]["parameters"]["version"].get("default")} does not match expected v2024-01-01'

    assert (
        cat_second["sources"]["access_nri"]["parameters"]["version"].get("min")
        == "v2025-01-01"
    ), f'Min version {cat_second["sources"]["access_nri"]["parameters"]["version"].get("min")} does not match expected v2025-01-01'
    assert (
        cat_second["sources"]["access_nri"]["parameters"]["version"].get("max")
        == "v2025-01-01"
    ), f'Max version {cat_second["sources"]["access_nri"]["parameters"]["version"].get("max")} does not match expected v2025-01-01'
    assert (
        cat_second["sources"]["access_nri"]["parameters"]["version"].get("default")
        == "v2025-01-01"
    ), f'Default version {cat_second["sources"]["access_nri"]["parameters"]["version"].get("default")} does not match expected v2025-01-01'


def test_metadata_validate(test_data):
    """Test metadata_validate"""

    file = str(test_data / "access-om2/metadata.yaml")
    metadata_validate([file])


@pytest.mark.parametrize(
    "bad_yaml,e",
    [
        ("bad_metadata/metadata-bad-all-bad.yaml", None),
        ("bad_metadata/doesntexist.yaml", FileNotFoundError),
    ],
)
def test_metadata_validate_bad(test_data, bad_yaml, e):
    bad_yaml = str(test_data / bad_yaml)
    if (
        e is None
    ):  # These are situations where an exception is raised, caught, and printed
        metadata_validate([bad_yaml])
    else:
        with pytest.raises(e):
            metadata_validate([bad_yaml])


def test_metadata_validate_multi(test_data):
    """Test metadata_validate"""
    files = ["access-om2/metadata.yaml", "access-om3/metadata.yaml"]
    files = [str(test_data / f) for f in files]
    metadata_validate(files)


def test_metadata_validate_no_file():
    """Test metadata_validate"""
    with pytest.raises(FileNotFoundError) as excinfo:
        metadata_validate(["./does/not/exist.yaml"])
    assert "No such file(s)" in str(excinfo.value)


def test_metadata_template(tmp_path):
    metadata_template(loc=tmp_path)
    if not (tmp_path / "metadata.yaml").is_file():
        raise RuntimeError("Didn't write template into temp dir")


def test_metadata_template_default_loc():
    metadata_template()
    if (Path.cwd() / "metadata.yaml").is_file():
        (Path.cwd() / "metadata.yaml").unlink()
    else:
        raise RuntimeError("Didn't write template into PWD")
