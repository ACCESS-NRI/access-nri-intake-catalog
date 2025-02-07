# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import glob
import os
import shutil
from pathlib import Path, PosixPath
from unittest import mock

import intake
import pytest
import yaml

import access_nri_intake
from access_nri_intake.catalog.manager import CatalogManager
from access_nri_intake.cli import (
    MetadataCheckError,
    _add_source_to_catalog,
    _check_build_args,
    build,
    metadata_template,
    metadata_validate,
    scaffold_catalog_entry,
    use_esm_datastore,
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

    exit_status = os.system("build-esm-datastore --help")
    assert exit_status == 0

    exit_status = os.system("scaffold-catalog-entry --help")
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
def test_build(version, test_data, tmpdir):
    """Test full catalog build process from config files"""
    # Update the config_yaml paths
    build_base_path = str(tmpdir)

    configs = [
        str(test_data / fname)
        for fname in ["config/access-om2.yaml", "config/cmip5.yaml"]
    ]

    build(
        [
            *configs,
            "--catalog_file",
            "cat.csv",
            # "--no_update",  # commented out to test brand-new-catalog-versioning
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

    # Check that the metacatalog is correct
    metacat = Path(build_base_path) / "catalog.yaml"
    with metacat.open(mode="r") as fobj:
        cat_info = yaml.safe_load(fobj)
    assert (
        cat_info["sources"]["access_nri"]["parameters"]["version"]["default"] == version
    )
    assert cat_info["sources"]["access_nri"]["parameters"]["version"]["min"] == version
    assert cat_info["sources"]["access_nri"]["parameters"]["version"]["max"] == version


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


@mock.patch("access_nri_intake.cli._get_project", return_value=set())
@mock.patch("access_nri_intake.cli._get_project_code", return_value="aa99")
def test_build_project_base_code(
    mock_get_project, mock_get_project_code, test_data, tmp_path
):
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

    # Check that the metacatalog is correct
    metacat = Path(build_base_path) / "catalog.yaml"
    with metacat.open(mode="r") as fobj:
        cat_info = yaml.safe_load(fobj)
    assert "gdata/aa99" in cat_info["sources"]["access_nri"]["metadata"]["storage"]


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


@mock.patch("access_nri_intake.cli._parse_build_directory")
@pytest.mark.parametrize(
    "failure",
    [PermissionError, FileNotFoundError, OSError, RuntimeError, Exception],
)
def test_build_parse_builddir_failure(
    mock_parse_build_directory, failure, test_data, tmp_path
):
    """Test build's response to a failure in _parse_build_directory"""

    configs = [
        str(test_data / "config/access-om2.yaml"),
    ]
    data_base_path = str(test_data)
    build_base_path = str(tmp_path)

    mock_parse_build_directory.side_effect = failure

    expected_failure = (
        failure if failure in [PermissionError, FileNotFoundError] else Exception
    )

    with pytest.raises(expected_failure):
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


@mock.patch("access_nri_intake.cli._get_project")
def test_build_parse_get_project_code_failure(
    mock_get_project_code, test_data, tmp_path
):
    """Test build's response to a failure in _get_project (should just carry on)"""

    configs = [
        str(test_data / "config/access-om2.yaml"),
    ]
    data_base_path = str(test_data)
    build_base_path = str(tmp_path)

    mock_get_project_code.side_effect = KeyError("Simulated key error")

    with pytest.warns(UserWarning, match="Unable to determine storage flags/projects"):
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


@mock.patch("access_nri_intake.cli.Path.mkdir")
def test_build_mkdir_failure(mock_mkdir, test_data, tmp_path):
    """Test build's response to a failure in _get_project (should just carry on)"""

    configs = [
        str(test_data / "config/access-om2.yaml"),
    ]
    data_base_path = str(test_data)
    build_base_path = str(tmp_path)

    mock_mkdir.side_effect = PermissionError("Simulated permission error")

    with pytest.raises(PermissionError, match="You lack the necessary permissions"):
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


class NoInitCatalogManager(CatalogManager):
    def __init__(self):
        pass


@pytest.mark.parametrize("method", ["load", "build_esm"])
def test_add_source_to_catalog_failure(method, tmpdir):
    with mock.patch.object(
        NoInitCatalogManager, method, side_effect=Exception("Dummy Exception injected")
    ):
        cm = NoInitCatalogManager()

        with pytest.warns(UserWarning, match="Unable to add dummy_name"):
            _add_source_to_catalog(cm, method, {"name": "dummy_name"}, "", None)


@mock.patch("access_nri_intake.cli._write_catalog_yaml")
def test_build_write_catalog_yaml_failure(mock_write_catalog_yaml, test_data, tmp_path):
    """Test build's response to a failure in _write_catalog_yaml"""

    configs = [
        str(test_data / "config/access-om2.yaml"),
    ]
    data_base_path = str(test_data)
    build_base_path = str(tmp_path)

    mock_write_catalog_yaml.side_effect = Exception("Simulated Exception")

    with pytest.raises(RuntimeError, match="Simulated Exception"):
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


@pytest.mark.parametrize(
    "builder",
    [
        "not_a_real_builder",  # Complete nonsense, not in the builders module
        "PATTERNS_HELPERS",  # We can get this from the module with getattr but it's not a builder
    ],
)
def test_use_esm_datastore_bad_builder(builder):
    with pytest.raises(ValueError) as excinfo:
        use_esm_datastore(
            [
                "--builder",
                builder,
                "--expt-dir",
                ".",
                "--cat-dir",
                ".",
            ]
        )

        assert f"Builder {builder} is not a valid builder." in str(excinfo.value)


@pytest.mark.parametrize(
    "expt_dir, cat_dir",
    [
        ("/not/a/real/dir", "."),
        (".", "/not/a/real/dir"),
    ],
)
def test_use_esm_datastore_nonexistent_dirs(expt_dir, cat_dir):
    with pytest.raises(FileNotFoundError) as excinfo:
        use_esm_datastore(
            [
                "--builder",
                "AccessOm2Builder",
                "--expt-dir",
                expt_dir,
                "--cat-dir",
                cat_dir,
            ]
        )

        assert "Directory /not/a/real/dir does not exist" in str(excinfo.value)


@mock.patch("access_nri_intake.cli.use_datastore")
@pytest.mark.parametrize(
    "argv, expected_call_args, expected_call_kwargs",
    [
        (
            ["--builder", "AccessOm2Builder"],
            (
                PosixPath("."),
                access_nri_intake.source.builders.AccessOm2Builder,
                PosixPath("."),
            ),
            {
                "builder_kwargs": {},
                "datastore_name": "experiment_datastore",
                "description": None,
                "open_ds": False,
            },
        ),
        (
            ["--builder", "Mom6Builder", "--datastore-name", "VERY_BAD_NAME"],
            (
                PosixPath("."),
                access_nri_intake.source.builders.Mom6Builder,
                PosixPath("."),
            ),
            {
                "builder_kwargs": {},
                "datastore_name": "VERY_BAD_NAME",
                "description": None,
                "open_ds": False,
            },
        ),
        (
            [
                "--builder",
                "AccessOm2Builder",
                "--description",
                "meaningless_description",
            ],
            (
                PosixPath("."),
                access_nri_intake.source.builders.AccessOm2Builder,
                PosixPath("."),
            ),
            {
                "builder_kwargs": {},
                "datastore_name": "experiment_datastore",
                "description": "meaningless_description",
                "open_ds": False,
            },
        ),
    ],
)
def test_use_esm_datastore_valid(
    use_datastore, argv, expected_call_args, expected_call_kwargs
):
    """I'm not using any args here, so we should get defaults. This should return
    zero. I'm going to mock the use_datastore function so it doesn't do anything,
    just returns none"""
    use_datastore.return_value = None
    ret = use_esm_datastore(argv)

    args, kwargs = use_datastore.call_args

    assert args == expected_call_args
    assert kwargs == expected_call_kwargs
    assert ret == 0


def test_use_esm_datastore_no_builder(tmp_path):
    """
    Test use_esm_datastore - no builder specified. This should look for a ESM-datastore
    in the new temporary directory, and try to build a datastore since there won't be
    one in there. Then it'll fail because there's no builder specified.
    """
    with pytest.raises(ValueError) as excinfo:
        use_esm_datastore(["--expt-dir", str(tmp_path)])

        assert "A builder must be provided if no valid datastore is found" in str(
            excinfo.value
        )


def test_scaffold_catalog_entry():
    """Test scaffold_catalog_entry - as of right now, it should just raise"""
    with pytest.raises(
        NotImplementedError, match="not yet implemented for non-interactive mode"
    ):
        scaffold_catalog_entry([])
    with pytest.raises(
        NotImplementedError, match="not yet implemented for interactive mode"
    ):
        scaffold_catalog_entry(["--interactive"])
