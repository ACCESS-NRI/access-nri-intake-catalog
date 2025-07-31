"""Tests for the AST module"""

import pytest

from access_nri_intake.ipython_magic import (
    MissingStorageError,
    MissingStorageWarning,
    check_storage_enabled,
)


@pytest.mark.parametrize(
    "func",
    [
        "to_dataset_dict",
        "to_dask",
        "to_datatree",
    ],
)
def test_to_datasets_raises(ipython, test_data, func):
    raw_cell = f"""
%%check_storage_enabled
import intake
ds = intake.open_esm_datastore("{test_data}/esm_datastore/cmip-forcing-qv56.json")
ds.{func}()
"""

    assert ipython.find_magic("check_storage_enabled", "cell") is not None

    with pytest.raises(MissingStorageError):
        check_storage_enabled("", raw_cell)


def test_to_datasets_warns(ipython, test_data):
    raw_cell = f"""
%%check_storage_enabled
import intake
from contextlib import suppress
from intake_esm.source import ESMDataSourceError
ds = intake.open_esm_datastore("{test_data}/esm_datastore/cmip-forcing-qv56.json")
with suppress(ESMDataSourceError):
    ds.to_dataset_dict()
"""

    assert ipython.find_magic("check_storage_enabled", "cell") is not None

    with pytest.warns(MissingStorageWarning):
        check_storage_enabled("", raw_cell)


def test_to_dataset_dict_syntax_err(ipython, test_data):
    raw_cell = f"""
%%check_storage_enabled
import intake
for (int i = 0; i< 10; i++):
    nope
ds = intake.open_esm_datastore("{test_data}/esm_datastore/cmip-forcing-qv56.json")
ds.to_dask()
"""

    assert ipython.find_magic("check_storage_enabled", "cell") is not None

    check_storage_enabled("", raw_cell)


def test_to_dataset_dict_no_code(ipython, test_data):
    raw_cell = """
%%check_storage_enabled
"""

    assert ipython.find_magic("check_storage_enabled", "cell") is not None

    check_storage_enabled("", raw_cell)


@pytest.mark.parametrize(
    "func",
    [
        "to_dataset_dict",
        "to_dask",
        "to_datatree",
    ],
)
def test_to_datasets_not_esm_datastore(ipython, test_data, func):
    raw_cell = f"""
%%check_storage_enabled
import intake
{func}()
"""

    assert ipython.find_magic("check_storage_enabled", "cell") is not None

    check_storage_enabled("", raw_cell)


@pytest.mark.parametrize(
    "func",
    [
        "to_dataset_dict",
        "to_dask",
        "to_datatree",
    ],
)
def test_to_datasets_undefined_esm_datastore(ipython, test_data, func):
    raw_cell = f"""
%%check_storage_enabled
import intake
undefined_datastore.{func}()
"""

    assert ipython.find_magic("check_storage_enabled", "cell") is not None

    check_storage_enabled("", raw_cell)


@pytest.mark.parametrize(
    "func",
    [
        "to_dataset_dict",
        "to_dask",
        "to_datatree",
    ],
)
def test_chained_to_datasets_raises(ipython, test_data, func):
    raw_cell = f"""
%%check_storage_enabled
import intake
ds = intake.open_esm_datastore("{test_data}/esm_datastore/cmip-forcing-qv56.json")
ds.search(source_id='.*').{func}()
"""

    assert ipython.find_magic("check_storage_enabled", "cell") is not None

    with pytest.raises(MissingStorageError):
        check_storage_enabled("", raw_cell)
