# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import argparse
import glob
import os
import shutil
import tempfile
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


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(
        config_yaml=[
            "config/access-om2.yaml",
            "config/cmip5.yaml",
        ],
        build_base_path=None,  # Use pytest fixture here?
        catalog_base_path=None,
        catalog_file="cat.csv",
        version=None,
        no_update=True,
    ),
)
@pytest.mark.parametrize(
    "version",
    [
        "v2024-01-01",
        "2024-01-01",
    ],
)
def test_build(mockargs, version, test_data):
    """Test full catalog build process from config files"""
    # Update the config_yaml paths
    mockargs.return_value.build_base_path = tempfile.TemporaryDirectory().name
    mockargs.return_value.catalog_base_path = mockargs.return_value.build_base_path
    for i, p in enumerate(mockargs.return_value.config_yaml):
        mockargs.return_value.config_yaml[i] = os.path.join(test_data, p)
    mockargs.return_value.version = version

    build()

    # Manually fix the version so we can correctly build the test path
    if not mockargs.return_value.version.startswith("v"):
        mockargs.return_value.version = f"v{mockargs.return_value.version}"

    # Try to open the catalog
    build_path = (
        Path(mockargs.return_value.build_base_path)
        / mockargs.return_value.version
        / mockargs.return_value.catalog_file
    )
    cat = intake.open_df_catalog(build_path)
    assert len(cat) == 2


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(
        config_yaml=[
            "config/access-om2.yaml",
            "config/cmip5.yaml",
        ],
        build_base_path=tempfile.TemporaryDirectory().name,  # Use pytest fixture here?
        catalog_base_path=tempfile.TemporaryDirectory().name,
        catalog_file="cat.csv",
        version="v2024-01-01",
        no_update=True,
    ),
)
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
def test_build_bad_version(mockargs, bad_vers, test_data):
    """Test full catalog build process from config files"""
    # Update the config_yaml paths
    for i, p in enumerate(mockargs.return_value.config_yaml):
        mockargs.return_value.config_yaml[i] = os.path.join(test_data, p)

    mockargs.return_value.version = bad_vers

    with pytest.raises(ValueError):
        build()


@mock.patch("access_nri_intake.cli.get_catalog_fp")
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(
        config_yaml=[
            "config/access-om2-bad.yaml",
            "config/cmip5.yaml",
        ],
        build_base_path=tempfile.TemporaryDirectory().name,  # Use pytest fixture here?
        catalog_base_path=None,  # Not required, get_catalog_fp is mocked
        catalog_file="cat.csv",
        version="v2024-01-01",
        no_update=False,
    ),
)
def test_build_bad_metadata(mockargs, get_catalog_fp, test_data):
    """
    Test if the intelligent versioning works correctly when there is
    no significant change to the underlying catalogue
    """
    # Update the config_yaml paths
    for i, p in enumerate(mockargs.return_value.config_yaml):
        mockargs.return_value.config_yaml[i] = os.path.join(test_data, p)

    # Write the catalog.yamls to where the catalogs go
    get_catalog_fp.return_value = os.path.join(
        mockargs.return_value.build_base_path, "catalog.yaml"
    )

    with pytest.raises(MetadataCheckError):
        build()


@mock.patch("access_nri_intake.cli.get_catalog_fp")
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(
        config_yaml=[
            "config/access-om2.yaml",
            "config/cmip5.yaml",
        ],
        build_base_path=tempfile.TemporaryDirectory().name,  # Use pytest fixture here?
        catalog_base_path=None,  # Not required, get_catalog_fp is mocked
        catalog_file="cat.csv",
        version="v2024-01-01",
        no_update=False,
    ),
)
def test_build_repeat_nochange(mockargs, get_catalog_fp, test_data):
    """
    Test if the intelligent versioning works correctly when there is
    no significant change to the underlying catalogue
    """
    # Update the config_yaml paths
    for i, p in enumerate(mockargs.return_value.config_yaml):
        mockargs.return_value.config_yaml[i] = os.path.join(test_data, p)

    # Write the catalog.yamls to where the catalogs go
    get_catalog_fp.return_value = os.path.join(
        mockargs.return_value.build_base_path, "catalog.yaml"
    )

    build()

    # Update the version number and have another crack at building
    mockargs.return_value.version = "v2024-01-02"
    build()

    # There is no change between catalogs, so we should be able to
    # see just a version number change in the yaml
    with Path(get_catalog_fp.return_value).open(mode="r") as fobj:
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


@mock.patch("access_nri_intake.cli.get_catalog_fp")
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(
        config_yaml=[
            "config/access-om2.yaml",
            # "config/cmip5.yaml",  # Save this for addition
        ],
        build_base_path=tempfile.TemporaryDirectory().name,  # Use pytest fixture here?
        catalog_base_path=None,  # Not required, get_catalog_fp is mocked
        catalog_file="cat.csv",
        version="v2024-01-01",
        no_update=False,
    ),
)
def test_build_repeat_adddata(mockargs, get_catalog_fp, test_data):
    # Update the config_yaml paths
    for i, p in enumerate(mockargs.return_value.config_yaml):
        mockargs.return_value.config_yaml[i] = os.path.join(test_data, p)

    # Write the catalog.yamls to where the catalogs go
    get_catalog_fp.return_value = os.path.join(
        mockargs.return_value.build_base_path, "catalog.yaml"
    )

    # Build the first catalog
    build()

    # Now, add the second data source & rebuild
    mockargs.return_value.config_yaml.append(
        os.path.join(test_data, "config/cmip5.yaml")
    )
    mockargs.return_value.version = "v2024-01-02"
    build()

    # There is no change between catalogs, so we should be able to
    # see just a version number change in the yaml
    with Path(get_catalog_fp.return_value).open(mode="r") as fobj:
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


@mock.patch("access_nri_intake.cli.get_catalog_fp")
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(
        config_yaml=[
            "config/access-om2.yaml",
            "config/cmip5.yaml",
        ],
        build_base_path=None,
        catalog_base_path=None,  # Not required, get_catalog_fp is mocked
        catalog_file="cat.csv",
        version="v2024-01-01",
        no_update=False,
    ),
)
@pytest.mark.parametrize(
    "min_vers,max_vers",
    [
        ("v2001-01-01", "v2099-01-01"),
        (None, "v2099-01-01"),
        ("v2001-01-01", None),
    ],
)
def test_build_existing_data(mockargs, get_catalog_fp, test_data, min_vers, max_vers):
    """
    Test if the build process can handle min and max catalog
    versions when an original catalog.yaml does not exist
    """
    # New temp directory for each test
    mockargs.return_value.build_base_path = (
        tempfile.TemporaryDirectory().name
    )  # Use pytest fixture here?
    # Update the config_yaml paths
    for i, p in enumerate(mockargs.return_value.config_yaml):
        mockargs.return_value.config_yaml[i] = os.path.join(test_data, p)

    # Write the catalog.yamls to where the catalogs go
    get_catalog_fp.return_value = os.path.join(
        mockargs.return_value.build_base_path, "catalog.yaml"
    )

    # Put dummy version folders into the tempdir
    if min_vers is not None:
        os.makedirs(
            os.path.join(mockargs.return_value.build_base_path, min_vers),
            exist_ok=False,
        )
    if max_vers is not None:
        os.makedirs(
            os.path.join(mockargs.return_value.build_base_path, max_vers),
            exist_ok=False,
        )

    build()

    with Path(get_catalog_fp.return_value).open(mode="r") as fobj:
        cat_yaml = yaml.safe_load(fobj)

    assert cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("min") == (
        min_vers if min_vers is not None else mockargs.return_value.version
    ), f'Min version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("min")} does not match expected {min_vers if min_vers is not None else mockargs.return_value.version}'
    assert cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("max") == (
        max_vers if max_vers is not None else mockargs.return_value.version
    ), f'Max version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("max")} does not match expected {max_vers if max_vers is not None else mockargs.return_value.version}'
    # Default should always be the newly-built version
    assert (
        cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("default")
        == mockargs.return_value.version
    ), f'Default version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("default")} does not match expected {mockargs.return_value.version}'


@mock.patch("access_nri_intake.cli.get_catalog_fp")
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(
        config_yaml=[
            "config/access-om2.yaml",
            "config/cmip5.yaml",
        ],
        build_base_path=None,
        catalog_base_path=None,  # Not required, get_catalog_fp is mocked
        catalog_file="cat.csv",
        version="v2024-01-01",
        no_update=False,
    ),
)
@pytest.mark.parametrize(
    "min_vers,max_vers",
    [
        ("v2001-01-01", "v2099-01-01"),
        (None, "v2099-01-01"),
        ("v2001-01-01", None),
    ],
)
def test_build_existing_data_existing_old_cat(
    mockargs, get_catalog_fp, test_data, min_vers, max_vers
):
    """
    Test if the build process can handle min and max catalog
    versions when a old-style catalog.yaml exists
    """
    # New temp directory for each test
    mockargs.return_value.build_base_path = (
        tempfile.TemporaryDirectory().name
    )  # Use pytest fixture here?
    # Update the config_yaml paths
    for i, p in enumerate(mockargs.return_value.config_yaml):
        mockargs.return_value.config_yaml[i] = os.path.join(test_data, p)

    # Write the catalog.yamls to where the catalogs go
    get_catalog_fp.return_value = os.path.join(
        mockargs.return_value.build_base_path, "catalog.yaml"
    )

    # Put dummy version folders into the tempdir
    if min_vers is not None:
        os.makedirs(
            os.path.join(mockargs.return_value.build_base_path, min_vers),
            exist_ok=False,
        )
    if max_vers is not None:
        os.makedirs(
            os.path.join(mockargs.return_value.build_base_path, max_vers),
            exist_ok=False,
        )

    # Copy the test data old-style catalog yaml to this location
    shutil.copy(test_data / "catalog/catalog-orig.yaml", get_catalog_fp.return_value)

    build()

    with Path(get_catalog_fp.return_value).open(mode="r") as fobj:
        cat_yaml = yaml.safe_load(fobj)

    assert cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("min") == (
        min_vers if min_vers is not None else mockargs.return_value.version
    ), f'Min version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("min")} does not match expected {min_vers if min_vers is not None else mockargs.return_value.version}'
    assert cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("max") == (
        max_vers if max_vers is not None else mockargs.return_value.version
    ), f'Max version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("max")} does not match expected {max_vers if max_vers is not None else mockargs.return_value.version}'
    # Default should always be the newly-built version
    assert (
        cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("default")
        == mockargs.return_value.version
    ), f'Default version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("default")} does not match expected {mockargs.return_value.version}'
    # Make sure the catalog storage flags were correctly merged
    assert (
        cat_yaml["sources"]["access_nri"]["metadata"]["storage"]
        == "gdata/al33+gdata/dc19+gdata/fs38+gdata/oi10+gdata/tm70"
    )
    # Make sure the old catalog vanished (i.e. there's only one)
    assert (
        len(glob.glob(mockargs.return_value.build_base_path + "/*.yaml")) == 1
    ), "Found more than one catalog remains!"


@mock.patch("access_nri_intake.cli.get_catalog_fp")
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(
        config_yaml=[
            "config/access-om2.yaml",
            "config/cmip5.yaml",
        ],
        build_base_path=None,  # Use pytest fixture here?
        catalog_base_path=None,  # Not required, get_catalog_fp is mocked
        catalog_file="cat.csv",
        version="v2024-01-01",
        no_update=False,
    ),
)
@pytest.mark.parametrize(
    "min_vers,max_vers",
    [
        ("v2001-01-01", "v2099-01-01"),
        (None, "v2099-01-01"),
        ("v2001-01-01", None),
    ],
)
def test_build_separation_between_catalog_and_buildbase(
    mockargs, get_catalog_fp, test_data, min_vers, max_vers
):
    """
    Test if the intelligent versioning works correctly when there is
    no significant change to the underlying catalogue
    """
    mockargs.return_value.build_base_path = (
        tempfile.TemporaryDirectory().name
    )  # Use pytest fixture here?
    # Update the config_yaml paths
    for i, p in enumerate(mockargs.return_value.config_yaml):
        mockargs.return_value.config_yaml[i] = os.path.join(test_data, p)

    # Write the catalog.yamls to its own directory
    catalog_dir = tempfile.TemporaryDirectory().name
    mockargs.return_value.catalog_base_path = catalog_dir
    get_catalog_fp.return_value = os.path.join(catalog_dir, "catalog.yaml")

    # Create dummy version folders in the *catalog* directory
    # (They would normally be in the build directory)
    if min_vers is not None:
        os.makedirs(
            os.path.join(mockargs.return_value.catalog_base_path, min_vers),
            exist_ok=False,
        )
    if max_vers is not None:
        os.makedirs(
            os.path.join(mockargs.return_value.catalog_base_path, max_vers),
            exist_ok=False,
        )

    # import pdb; pdb.set_trace()

    build()

    # The version folders exist in the catalog directory, not the build
    # directory, hence they shouldn't have been found - therefore,
    # all the version numbers should align with the newly-built catalog
    with Path(get_catalog_fp.return_value).open(mode="r") as fobj:
        cat_yaml = yaml.safe_load(fobj)

    assert (
        cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("min")
        == "v2024-01-01"
    ), f'Min version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("min")} does not match expected v2024-01-01'
    assert (
        cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("max")
        == "v2024-01-01"
    ), f'Max version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("max")} does not match expected v2024-01-01'
    assert (
        cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("default")
        == "v2024-01-01"
    ), f'Default version {cat_yaml["sources"]["access_nri"]["parameters"]["version"].get("default")} does not match expected v2024-01-01'


@mock.patch("access_nri_intake.cli.get_catalog_fp")
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(
        config_yaml=[
            "config/access-om2.yaml",
            # "config/cmip5.yaml",  # Save this for addition
        ],
        build_base_path=None,  # Use pytest fixture here?
        catalog_base_path=None,  # Not required, get_catalog_fp is mocked
        catalog_file="cat.csv",
        version="v2024-01-01",
        no_update=False,
    ),
)
@pytest.mark.parametrize(
    "min_vers,max_vers",
    [
        ("v2001-01-01", "v2099-01-01"),
        (None, "v2099-01-01"),
        ("v2001-01-01", None),
    ],
)
def test_build_repeat_renamecatalogyaml(
    mockargs, get_catalog_fp, test_data, min_vers, max_vers
):
    # Update the config_yaml paths
    for i, p in enumerate(mockargs.return_value.config_yaml):
        mockargs.return_value.config_yaml[i] = os.path.join(test_data, p)

    mockargs.return_value.build_base_path = tempfile.TemporaryDirectory().name
    mockargs.return_value.version = (
        "v2024-01-01"  # May have been overridden in previous parametrize pass
    )

    # Write the catalog.yamls to where the catalogs go
    get_catalog_fp.return_value = os.path.join(
        mockargs.return_value.build_base_path, "catalog.yaml"
    )

    # Build the first catalog
    build()

    # Update the version number, *and* the catalog name
    NEW_VERSION = "v2025-01-01"
    mockargs.return_value.version = NEW_VERSION
    get_catalog_fp.return_value = os.path.join(
        mockargs.return_value.build_base_path, "metacatalog.yaml"
    )
    # Put dummy version folders into the tempdir
    # The new catalog will consider these, as the catalog.yaml
    # names are no longer consistent
    if min_vers is not None:
        os.makedirs(
            os.path.join(mockargs.return_value.build_base_path, min_vers),
            exist_ok=False,
        )
    if max_vers is not None:
        os.makedirs(
            os.path.join(mockargs.return_value.build_base_path, max_vers),
            exist_ok=False,
        )

    # Build another catalog
    build()

    # There should now be two catalogs - catalog.yaml and metacatalog.yaml
    with Path(
        os.path.join(os.path.dirname(get_catalog_fp.return_value), "catalog.yaml")
    ).open(mode="r") as fobj:
        cat_first = yaml.safe_load(fobj)
    with Path(
        os.path.join(os.path.dirname(get_catalog_fp.return_value), "metacatalog.yaml")
    ).open(mode="r") as fobj:
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
        else mockargs.return_value.version
    ), f'Min version {cat_second["sources"]["access_nri"]["parameters"]["version"].get("min")} does not match expected {min_vers if min_vers is not None else mockargs.return_value.version}'
    assert (
        cat_second["sources"]["access_nri"]["parameters"]["version"].get("max")
        == max_vers
        if max_vers is not None
        else mockargs.return_value.version
    ), f'Max version {cat_second["sources"]["access_nri"]["parameters"]["version"].get("max")} does not match expected {max_vers if max_vers is not None else mockargs.return_value.version}'
    assert (
        cat_second["sources"]["access_nri"]["parameters"]["version"].get("default")
        == "v2025-01-01"
    ), f'Default version {cat_second["sources"]["access_nri"]["parameters"]["version"].get("default")} does not match expected v2025-01-01'


@mock.patch("access_nri_intake.cli.get_catalog_fp")
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(
        config_yaml=[
            "config/access-om2.yaml",
            # "config/cmip5.yaml",  # Save this for addition
        ],
        build_base_path=None,  # Use pytest fixture here?
        catalog_base_path=None,  # Not required, get_catalog_fp is mocked
        catalog_file="cat.csv",
        version="v2024-01-01",
        no_update=False,
    ),
)
@pytest.mark.parametrize(
    "min_vers,max_vers",
    [
        ("v2001-01-01", "v2099-01-01"),
        (None, "v2099-01-01"),
        ("v2001-01-01", None),
    ],
)
def test_build_repeat_altercatalogstruct(
    mockargs, get_catalog_fp, test_data, min_vers, max_vers
):
    # Update the config_yaml paths
    for i, p in enumerate(mockargs.return_value.config_yaml):
        mockargs.return_value.config_yaml[i] = os.path.join(test_data, p)

    mockargs.return_value.build_base_path = tempfile.TemporaryDirectory().name
    mockargs.return_value.version = (
        "v2024-01-01"  # May have been overridden in previous parametrize pass
    )
    mockargs.return_value.catalog_file = "cat.csv"

    # Write the catalog.yamls to where the catalogs go
    get_catalog_fp.return_value = os.path.join(
        mockargs.return_value.build_base_path, "catalog.yaml"
    )

    # Build the first catalog
    build()

    # Update the version number, *and* the catalog name
    NEW_VERSION = "v2025-01-01"
    mockargs.return_value.version = NEW_VERSION
    mockargs.return_value.catalog_file = "new_cat.csv"
    # Put dummy version folders into the tempdir
    # The new catalog will *not* consider these, as the catalog.yaml
    # names are no longer consistent
    if min_vers is not None:
        os.makedirs(
            os.path.join(mockargs.return_value.build_base_path, min_vers),
            exist_ok=False,
        )
    if max_vers is not None:
        os.makedirs(
            os.path.join(mockargs.return_value.build_base_path, max_vers),
            exist_ok=False,
        )

    # Build another catalog
    build()

    # There should now be two catalogs - catalog.yaml and catalog-v2024-01-01.yaml
    with Path(
        os.path.join(
            os.path.dirname(get_catalog_fp.return_value), "catalog-v2024-01-01.yaml"
        )
    ).open(mode="r") as fobj:
        cat_first = yaml.safe_load(fobj)
    with Path(
        os.path.join(os.path.dirname(get_catalog_fp.return_value), "catalog.yaml")
    ).open(mode="r") as fobj:
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


@mock.patch("access_nri_intake.cli.get_catalog_fp")
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(
        config_yaml=[
            "config/access-om2.yaml",
            # "config/cmip5.yaml",  # Save this for addition
        ],
        build_base_path=None,  # Use pytest fixture here?
        catalog_base_path=None,  # Not required, get_catalog_fp is mocked
        catalog_file=None,
        version="v2024-01-01",
        no_update=False,
    ),
)
@pytest.mark.parametrize(
    "min_vers,max_vers",
    [
        ("v2001-01-01", "v2099-01-01"),
        (None, "v2099-01-01"),
        ("v2001-01-01", None),
    ],
)
def test_build_repeat_altercatalogstruct_multivers(
    mockargs, get_catalog_fp, test_data, min_vers, max_vers
):
    # Update the config_yaml paths
    for i, p in enumerate(mockargs.return_value.config_yaml):
        mockargs.return_value.config_yaml[i] = os.path.join(test_data, p)

    mockargs.return_value.build_base_path = tempfile.TemporaryDirectory().name
    mockargs.return_value.version = (
        "v2024-01-01"  # May have been overridden in previous parametrize pass
    )
    mockargs.return_value.catalog_file = "cat.csv"

    # Put dummy version folders into the tempdir - these will
    # be picked up by the first catalog
    if min_vers is not None:
        os.makedirs(
            os.path.join(mockargs.return_value.build_base_path, min_vers),
            exist_ok=False,
        )
    if max_vers is not None:
        os.makedirs(
            os.path.join(mockargs.return_value.build_base_path, max_vers),
            exist_ok=False,
        )

    # Write the catalog.yamls to where the catalogs go
    get_catalog_fp.return_value = os.path.join(
        mockargs.return_value.build_base_path, "catalog.yaml"
    )

    # Build the first catalog
    build()

    # Update the version number, *and* the catalog name
    NEW_VERSION = "v2025-01-01"
    mockargs.return_value.version = NEW_VERSION
    mockargs.return_value.catalog_file = "new_cat.csv"

    # Build another catalog
    build()

    # There should now be two catalogs - catalog.yaml and catalog-<min>-<max>.yaml
    cat_first_name = f"catalog-{min_vers if min_vers is not None else 'v2024-01-01'}"
    if max_vers is not None or min_vers is not None:
        cat_first_name += f"-{max_vers if max_vers is not None else 'v2024-01-01'}"
    cat_first_name += ".yaml"
    with Path(
        os.path.join(os.path.dirname(get_catalog_fp.return_value), cat_first_name)
    ).open(mode="r") as fobj:
        cat_first = yaml.safe_load(fobj)
    with Path(
        os.path.join(os.path.dirname(get_catalog_fp.return_value), "catalog.yaml")
    ).open(mode="r") as fobj:
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


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(
        file=["access-om2/metadata.yaml"],
    ),
)
def test_metadata_validate(mockargs, test_data):
    """Test metadata_validate"""
    for i, p in enumerate(mockargs.return_value.file):
        mockargs.return_value.file[i] = os.path.join(test_data, p)
    metadata_validate()


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(
        file=None,
    ),
)
@pytest.mark.parametrize(
    "bad_yaml,e",
    [
        ("bad_metadata/metadata-bad-all-bad.yaml", None),
        ("bad_metadata/doesntexist.yaml", FileNotFoundError),
    ],
)
def test_metadata_validate_bad(mockargs, test_data, bad_yaml, e):
    bad_yaml = os.path.join(test_data, bad_yaml)
    mockargs.return_value.file = [bad_yaml]
    if (
        e is None
    ):  # These are situations where an exception is raised, caught, and printed
        metadata_validate()
    else:
        with pytest.raises(e):
            metadata_validate()


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(
        file=[
            "access-om2/metadata.yaml",
            "access-om3/metadata.yaml",
        ],
    ),
)
def test_metadata_validate_multi(mockargs, test_data):
    """Test metadata_validate"""
    # Update the config_yaml paths
    for i, p in enumerate(mockargs.return_value.file):
        mockargs.return_value.file[i] = os.path.join(test_data, p)
    metadata_validate()


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(
        file="./does/not/exist.yaml",
    ),
)
def test_metadata_validate_no_file(mockargs):
    """Test metadata_validate"""
    with pytest.raises(FileNotFoundError) as excinfo:
        metadata_validate()
    assert "No such file(s)" in str(excinfo.value)


def test_metadata_template():
    metadata_template()
