from filecmp import cmp
from os import remove, mkdir
from pathlib import Path
from shutil import copyfile, copytree
from time import sleep
import pytest

from access_nri_intake.catalog.manager import CatalogManager
from access_nri_intake.source import builders

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
        (True, {"add": ["woa13_ts_03_mom01.nc"], "remove": ["woa13_ts_01_mom01.nc"]}, True),
    ]
)
def test__need_to_redo_build(tmp_path, test_data, build_datastore,
                             file_list_changes, need_to_rebuild):
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
    "basedirs, builder, kwargs",
    [
        # This parametrization was taken from test_builders.test_builder_build
        (["access-om2"], "AccessOm2Builder", {}),
        (
            ["access-cm2/by578", "access-cm2/by578a"],
            "AccessCm2Builder",
            {"ensemble": True},
        ),
        (
            ["access-cm2/by578", "access-cm2/by578a"],
            "AccessCm2Builder",
            {"ensemble": False},
        ),
        (["access-esm1-5"], "AccessEsm15Builder", {"ensemble": False}),
        (["access-cm3"], "AccessCm3Builder", {}),
        (["access-om3"], "AccessOm3Builder", {}),
        (["mom6"], "Mom6Builder", {}),
        (["roms"], "ROMSBuilder", {}),
        (
            ["access-esm1-6"],
            "AccessEsm16Builder",
            {"depth": 5, "ensemble": False},
        ),
        (
            ["access-esm1-6"],
            "AccessEsm16Builder",
            {"depth": 5, "ensemble": True},
        ),
        (["woa"], "WoaBuilder", {}),
        (["cmip6"], "Cmip6Builder", {"ensemble": False},),
        (["cmip6"], "Cmip6Builder", {"ensemble": True},),
        (["access-am3"], "AccessAm3Builder", {},),
    ]
)
def test_skipped_build_identical(tmp_path, test_data, basedirs, builder, kwargs):
    """
    Confirm that if a build is skipped the resulting datastore is identical
    to the previous one
    """
    # Build the original datastore
    Builder = getattr(builders, builder)
    path = [str(test_data / Path(basedir)) for basedir in basedirs]
    builder = Builder(path, **kwargs)
    builder.build()
    builder.save(name="test", description="test datastore", directory=str(tmp_path))

    # Rebuild the datastore
    builder = Builder(path, **kwargs)
    builder.build()
    builder.save(name="test2", description="test datastore", directory=str(tmp_path))

    # Check that the two datastores are identical
    assert cmp(tmp_path / "test.csv", tmp_path / "test2.csv")
