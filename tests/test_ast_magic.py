"""Tests for the AST module"""

from unittest import mock

import intake
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


@pytest.mark.parametrize(
    "func",
    [
        "not_a_function",
    ],
)
def test_to_datasets_no_raise(ipython, test_data, func):
    raw_cell = f"""
%%check_storage_enabled
import intake
ds = intake.open_esm_datastore("{test_data}/esm_datastore/cmip-forcing-qv56.json")
ds.{func}()
"""

    assert ipython.find_magic("check_storage_enabled", "cell") is not None

    check_storage_enabled("", raw_cell)
    assert True


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
ds.search(source_id='.*').search(source_id='.*').{func}()
"""

    assert ipython.find_magic("check_storage_enabled", "cell") is not None

    with pytest.raises(MissingStorageError):
        check_storage_enabled("", raw_cell)


@pytest.mark.parametrize(
    "func",
    [
        "to_dataset_dict",
        "to_dask",
        "to_datatree",
    ],
)
@mock.patch("access_nri_intake.ipython_magic.ast.eval")
def test_index_and_load(mock_getitem, ipython, test_data, func):
    mock_getitem.return_value = intake.open_esm_datastore(
        f"{test_data}/esm_datastore/cmip-forcing-qv56.json"
    )
    raw_cell = f"""
%%check_storage_enabled
import intake
cat = intake.cat.access_nri
cat['01deg_jra55_ryf_Control'].search(source_id='.*').search(source_id='.*').{func}()
"""

    assert ipython.find_magic("check_storage_enabled", "cell") is not None

    with pytest.raises(MissingStorageError):
        check_storage_enabled("", raw_cell)


def test_indexing_something_ele(
    ipython,
):
    """
    Test that we don't intercept indexing of non-catalog items.
    """
    raw_cell = """
%%check_storage_enabled
import intake

l = ["A","B","C"]
l[1].lower()
"""

    assert ipython.find_magic("check_storage_enabled", "cell") is not None

    check_storage_enabled("", raw_cell)

    assert True
