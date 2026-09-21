from os import remove, mkdir
from pathlib import Path
from shutil import copyfile, copytree
from time import sleep
import pytest
import pyarrow.parquet as pq

from access_nri_intake.cli import build
from access_nri_intake.catalog.manager import CatalogManager
from access_nri_intake.source import builders

from test_cli import fake_project_access


@pytest.mark.parametrize(
    "build_datastore, file_list_changes, need_to_rebuild",
    [
        # If there's no datastore always rebuild
        (False, {}, True),
        (False, {}, True),
        # Rebuild if files have been touched
        (True, {}, False),
        (True, {"touch": ["woa13_ts_01_mom01.nc"]}, True),
        (True, {"touch": ["woa13_ts_02_mom01.nc"]}, True),
        (True, {"touch": ["woa13_ts_01_mom01.nc", "woa13_ts_02_mom01.nc"]}, True),
        # Rebuild if files have been added
        (True, {"add": ["woa13_ts_03_mom01.nc"]}, True),
        (True, {"add": ["woa13_ts_03_mom01.nc", "woa13_ts_03_mom01.nc"]}, True),
        # Rebuild if files have been deleted
        (True, {"remove": ["woa13_ts_01_mom01.nc"]}, True),
        (True, {"remove": ["woa13_ts_02_mom01.nc"]}, True),
        #        (True, {"remove": ["woa13_ts_01_mom01.nc", "woa13_ts_02_mom01.nc"]}, True),
        # Rebuild if files are rename - do an add+remove to simulate this
        (
            True,
            {"add": ["woa13_ts_03_mom01.nc"], "remove": ["woa13_ts_01_mom01.nc"]},
            True,
        ),
    ],
)
def test__need_to_redo_build(
    tmp_path, test_data, build_datastore, file_list_changes, need_to_rebuild
):
    """
    _need_to_redo_build has a few paths to test
    - datastore is None
    - files in datastore have not changed since last build
    - files in datastore have been modified
    - list of files have changed (more files, less files, different files)

    Here file_list_changes is a dict as follows:
        {
            "add": [list of files to add to the datastore],
            "remove": [list of files to remove from the datastore],
            "touch": [list of files to touch],
        }
    """
    src_dataset = test_data / "woa/KDS50"
    dataset_path = tmp_path / "dataset"
    mkdir(dataset_path)
    datastore_path = tmp_path / "datastore"
    mkdir(datastore_path)

    # Copy the sample dataset
    # This portion of the test woa data set has two nc files:
    #   woa13_ts_01_mom01.nc & woa13_ts_02_mom01.nc
    copytree(src_dataset, dataset_path, dirs_exist_ok=True)

    # Build the original datastore
    Builder = getattr(builders, "WoaBuilder")
    b = Builder(str(dataset_path))
    if build_datastore:
        b.build()
        b.save(name="test", description="test", directory=str(datastore_path))
    else:
        datastore_path = None

    if "add" in file_list_changes:
        for f in file_list_changes["add"]:
            # copy the first existing file to the supplied name
            copyfile(dataset_path / "woa13_ts_01_mom01.nc", dataset_path / f)

    if "remove" in file_list_changes:
        for f in file_list_changes["remove"]:
            # remove the file
            remove(dataset_path / f)

    if "touch" in file_list_changes:
        # Sleep for a second to ensure mtime changes
        sleep(1)
        for f in file_list_changes["touch"]:
            # touch the file
            (dataset_path / f).touch()

    # Check _need_to_redo_build
    # Need a fresh builder
    b2 = Builder(str(dataset_path))
    d_path = datastore_path / "test.csv" if datastore_path else None
    do_rebuild = CatalogManager._need_to_redo_build(d_path, b2)
    assert do_rebuild == need_to_rebuild


@pytest.mark.parametrize(
    "version",
    [
        "v2024-01-01",
    ],
)
@pytest.mark.parametrize(
    "input_list",
    [
        ["config/access-om2.yaml", "config/cmip5.yaml"],
        # FIXME: The following test has invalid assets - currently fails
        ["config/access-om2-patterns.yaml", "config/cmip5.yaml"],
    ],
)
@pytest.mark.filterwarnings("ignore:Unable to determine project for base path")
@pytest.mark.filterwarnings("ignore:Unable to parse 32 assets.")
def test_skipped_build_identical(
    version,
    input_list,
    test_data,
    tmpdir,
    fake_project_access,
    capfd,
):
    """
    Confirm that if a build is skipped the resulting datastore is identical
    to the previous one

    This test is mostly copied from test_cli.test_build
    """
    # Build the datastore
    build_base_path = str(tmpdir)

    configs = [str(test_data / fname) for fname in input_list]

    cat_name = "access_nri_pq"
    catfile = "cat.parquet"

    argv = [
        *configs,
        "--catalog_file",
        catfile,
        "--version",
        version,
        "--build_base_path",
        build_base_path,
        "--catalog_base_path",
        build_base_path,
        "--data_base_path",
        str(test_data),
        "--use_parquet",
    ]

    build(argv)

    # Build the datastore again
    new_version = "v2024-01-02"
    argv[5] = new_version

    build(argv)

    # Confirm that old datastore was reused
    reuse_str = "Reusing previous datastore"
    stdout, _stderr = capfd.readouterr()
    assert reuse_str in stdout

    # Now compare the datastore files from each build
    old_source = Path(tmpdir) / version / "source"
    new_source = Path(tmpdir) / new_version / "source"
    for old_datastore_path in old_source.glob("*.parquet"):
        new_datastore_path = new_source / old_datastore_path.name

        old_pq = pq.read_table(old_datastore_path)
        new_pq = pq.read_table(new_datastore_path)
        assert old_pq.equals(new_pq)


@pytest.mark.parametrize(
    "datastore_file, expected_error",
    [
        # These two should pass the filetype check and return a FileNotFoundError
        ("file.parquet", FileNotFoundError),
        ("file.csv", FileNotFoundError),
        # These should fail the filetype check and return a ValueError
        ("file.somethingelse", ValueError),
        ("file_with_no_extension", ValueError),
    ],
)
def test__need_to_redo_build_invalid_filetype(datastore_file, expected_error):
    """
    This test explores the file type check for the datastore file in _need_to_redo_build
    """
    with pytest.raises(expected_error):
        CatalogManager._need_to_redo_build(
            Path(datastore_file), builders.BaseBuilder(".")
        )


@pytest.mark.parametrize(
    "version",
    [
        "v2024-01-01",
    ],
)
@pytest.mark.parametrize(
    "input_list",
    [
        ["config/access-om2.yaml", "config/cmip5.yaml"],
    ],
)
@pytest.mark.parametrize("use_parquet", [True])
@pytest.mark.filterwarnings("ignore:Unable to determine project for base path")
def test__build_datastore_missing_file(
    version,
    input_list,
    test_data,
    tmpdir,
    use_parquet,
    fake_project_access,
):
    """
    This test checks that the FileNotFound exception is raised correctly

    This test is mostly copied from test_skipped_build_identical
    """
    # Build the datastore
    build_base_path = str(tmpdir)

    configs = [str(test_data / fname) for fname in input_list]

    if use_parquet:
        cat_name = "access_nri_pq"
        catfile = "cat.parquet"
    else:
        cat_name = "access_nri"
        catfile = "cat.csv"

    argv = [
        *configs,
        "--catalog_file",
        catfile,
        "--version",
        version,
        "--build_base_path",
        build_base_path,
        "--catalog_base_path",
        build_base_path,
        "--data_base_path",
        str(test_data),
    ]

    if use_parquet:
        argv.append("--use_parquet")

    build(argv)

    # Delete the datastore files
    for f in (Path(tmpdir) / version / "source").glob("*.parquet"):
        remove(f)

    # Build the datastore again
    new_version = "v2024-01-02"
    argv[5] = new_version

    with pytest.warns(
        UserWarning, match=".*Error: Unable to fild an existing datastore file"
    ):
        build(argv)
