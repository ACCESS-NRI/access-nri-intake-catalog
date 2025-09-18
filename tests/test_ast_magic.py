"""Tests for the AST module"""

import re
from unittest import mock

import intake
import pytest

from access_nri_intake.ipython_magic import (
    MissingStorageError,
    MissingStorageWarning,
    TooManyDatasetsError,
    check_dataset_number,
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
    check_dataset_number("", raw_cell)


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


@pytest.mark.parametrize(
    "func",
    [
        "to_dataset_dict",
        "to_dask",
        "to_datatree",
    ],
)
@mock.patch("access_nri_intake.ipython_magic.ast.eval")
def test_index_and_load_too_many_ds(mock_getitem, ipython, test_data, func):
    mock_getitem.return_value = intake.open_esm_datastore(
        f"{test_data}/esm_datastore/cmip-forcing-qv56.json"
    )
    raw_cell = f"""
%%check_dataset_number
import intake
cat = intake.cat.access_nri
cat['01deg_jra55_ryf_Control'].search(source_id='.*').search(source_id='.*').{func}()
"""

    assert ipython.find_magic("check_dataset_number", "cell") is not None

    if func == "to_dask":
        with pytest.raises(
            TooManyDatasetsError,
            match=re.escape(
                "Found >1 dataset: distinguished on ['project_id (2 values), ', 'source_id (2 values), ', 'frequency (3 values), ', 'table_id (2 values), ', 'mip_era (2 values), ', 'target_mip (2 values), ', 'variable_id (4 values), ', 'version (2 values), ']. Please refine search further, use `.to_dataset_dict()`/`.to_datatree, or change aggregation controls: see https://github.com/COSIMA/cosima-recipes/issues/543#issuecomment-3086429836"
            ),
        ):
            check_dataset_number("", raw_cell)
    else:
        check_dataset_number("", raw_cell)


@pytest.mark.parametrize(
    "func",
    [
        "to_dataset_dict",
        "to_dask",
        "to_datatree",
    ],
)
def test_too_many_ds_no_errors(ipython, test_data, func):
    """
    This should pass, because the .search(path=path) call takes it down to one dataset.
    """
    raw_cell = f"""
%%check_dataset_number
import intake
cat = intake.cat.access_nri
esm_ds = intake.open_esm_datastore("{test_data}/esm_datastore/cmip-forcing-qv56.json")

path = esm_ds.unique().path[0]

esm_ds.search(path=path).{func}()
"""

    assert ipython.find_magic("check_dataset_number", "cell") is not None

    check_dataset_number("", raw_cell)
