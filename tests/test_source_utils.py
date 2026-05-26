# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0


from pathlib import Path
from unittest import mock

import cftime
import pytest
import xarray as xr

from access_nri_intake.source.utils import (
    EmptyFileError,
    HashableIndexes,
    _guess_start_end_dates,
    _parse_variable_cell_methods,
    get_timeinfo,
    open_dataset_cached,
)


@pytest.mark.parametrize(
    "times, bounds, ffreq, expected",
    [
        (
            [365 / 2],
            False,
            (1, "yr"),
            ("1900-01-01, 00:00:00", "1901-01-01, 00:00:00", "1yr"),
        ),
        (
            [31 / 2],
            False,
            (1, "mon"),
            ("1900-01-01, 00:00:00", "1900-02-01, 00:00:00", "1mon"),
        ),
        (
            [1.5 / 24],
            False,
            (3, "hr"),
            ("1900-01-01, 00:00:00", "1900-01-01, 03:00:00", "3hr"),
        ),
        (
            [0.0, 9 / 60 / 24],
            True,
            None,
            ("1900-01-01, 00:00:00", "1900-01-01, 00:09:00", "subhr"),
        ),
        (
            [0.0, 3 / 24],
            True,
            None,
            ("1900-01-01, 00:00:00", "1900-01-01, 03:00:00", "3hr"),
        ),
        (
            [0.0, 6 / 24],
            True,
            None,
            ("1900-01-01, 00:00:00", "1900-01-01, 06:00:00", "6hr"),
        ),
        (
            [0.0, 1.0],
            True,
            None,
            ("1900-01-01, 00:00:00", "1900-01-02, 00:00:00", "1day"),
        ),
        (
            [0.0, 31.0],
            True,
            None,
            ("1900-01-01, 00:00:00", "1900-02-01, 00:00:00", "1mon"),
        ),
        (
            [0.0, 90.0],
            True,
            None,
            ("1900-01-01, 00:00:00", "1900-04-01, 00:00:00", "3mon"),
        ),
        (
            [0.0, 365.0],
            True,
            None,
            ("1900-01-01, 00:00:00", "1901-01-01, 00:00:00", "1yr"),
        ),
        (
            [0.0, 730.0],
            True,
            None,
            ("1900-01-01, 00:00:00", "1902-01-01, 00:00:00", "2yr"),
        ),
        (
            [1.5 / 24, 4.5 / 24],
            False,
            None,
            ("1900-01-01, 00:00:00", "1900-01-01, 06:00:00", "3hr"),
        ),
        (
            [3 / 24, 9 / 24],
            False,
            None,
            ("1900-01-01, 00:00:00", "1900-01-01, 12:00:00", "6hr"),
        ),
        (
            [0.5, 1.5],
            False,
            None,
            ("1900-01-01, 00:00:00", "1900-01-03, 00:00:00", "1day"),
        ),
        (
            [31 / 2, 45],
            False,
            None,
            ("1900-01-01, 00:00:00", "1900-03-01, 00:00:00", "1mon"),
        ),
        (
            [45, 135.5],
            False,
            None,
            ("1900-01-01, 00:00:00", "1900-07-01, 00:00:00", "3mon"),
        ),
        (
            [365 / 2, 365 + 365 / 2],
            False,
            None,
            ("1900-01-01, 00:00:00", "1902-01-01, 00:00:00", "1yr"),
        ),
        (
            [365, 3 * 365],
            False,
            None,
            ("1900-01-01, 00:00:00", "1904-01-01, 00:00:00", "2yr"),
        ),
    ],
)
@pytest.mark.filterwarnings(
    "ignore:The frequency '\\(\\d, '(hr|mon|yr)'\\)' determined from filename"
)
@pytest.mark.filterwarnings("ignore:Time coordinate does not include bounds info")
def test_get_timeinfo(times, bounds, ffreq, expected):
    if bounds:
        time = (times[0] + times[1]) / 2
        ds = xr.Dataset(
            data_vars={
                "dummy": ("time", [0]),
                "time_bounds": (("time", "nv"), [(times[0], times[1])]),
            },
            coords={"time": [time]},
        )
        ds["time"].attrs = dict(bounds="time_bounds")
    else:
        ds = xr.Dataset(
            data_vars={"dummy": ("time", [0] * len(times))},
            coords={"time": times},
        )

    ds["time"].attrs |= dict(
        units="days since 1900-01-01 00:00:00", calendar="GREGORIAN"
    )

    assert get_timeinfo(ds, filename_frequency=ffreq, time_dim="time") == expected


@pytest.mark.filterwarnings("ignore:Time coordinate does not include bounds")
def test_guess_start_end_dates_warning():
    with pytest.warns(UserWarning, match="Cannot infer start and end times"):
        _guess_start_end_dates(
            ts=xr.date_range("1900-01-01", periods=1, freq="6h")[0],
            te=xr.date_range("1900-01-01", periods=1, freq="6h")[0],
            frequency=(10, "min"),
        )


def test_empty_file_error():
    times = []

    ds = xr.Dataset(
        data_vars={"dummy": ("time", [])},
        coords={"time": times},
    )

    ds["time"].attrs |= dict(
        units="days since 1900-01-01 00:00:00", calendar="GREGORIAN"
    )

    with pytest.raises(EmptyFileError):
        get_timeinfo(ds, filename_frequency=None, time_dim="time")


def test_hashable_indexes(test_data):
    ds = xr.open_dataset(
        Path(test_data) / "access-om2/output000/ocean/ocean_grid.nc",
    )

    h1 = HashableIndexes(ds=ds)

    h2 = HashableIndexes(_indexes=ds._indexes)

    ds2 = xr.open_dataset(
        Path(test_data) / "access-om2/output000/ocean/ocean_month.nc",
    )

    h3 = HashableIndexes(ds=ds2)

    assert h1.xxh == "7f9556036b3d01ba"  # Example hash value

    h1_repr = repr(h1)

    assert h1_repr == h1.xxh

    assert h1 == h2

    assert h1 ^ h2 == set()

    assert h1 & h2 == set(h1.keys())

    with pytest.raises(
        TypeError,
        match=r"Can only initialise HashableIndexes with either an xarray dataset",
    ):
        HashableIndexes(_indexes=ds._indexes, ds=ds)

    h3 = HashableIndexes(ds=ds, drop_indices=["xt_ocean"])

    assert h1 != [1, 2, 3]

    assert h1 != h3

    with pytest.raises(
        TypeError, match="Cannot compare HashableIndexes with type list"
    ):
        h1 ^ [1, 2, 3]

    with pytest.raises(
        TypeError, match="Cannot compare HashableIndexes with type list"
    ):
        h1 & [1, 2, 3]

    # Test the __hash__ of HashableIndexes with a set
    # h1 & h2 result in the same hash
    s = set([h1, h1, h2, h3])
    assert len(s) == 2


def test_hashable_indexes_cftime():
    ds = xr.Dataset(
        {
            "foo": ("time", [0.0]),
            "time_bnds": (
                ("time", "bnds"),
                [
                    [
                        cftime.Datetime360Day(2005, 12, 1, 0, 0, 0, 0),
                        cftime.Datetime360Day(2005, 12, 2, 0, 0, 0, 0),
                    ]
                ],
            ),
        },
        {"time": [cftime.Datetime360Day(2005, 12, 1, 12, 0, 0, 0)]},
    )

    h1 = HashableIndexes(ds=ds)

    h2 = HashableIndexes(_indexes=ds._indexes)

    assert h1.xxh == "53dfde4d62872e35"  # Example hash value

    h1_repr = repr(h1)

    assert h1_repr == h1.xxh

    assert h1 == h2

    assert h1 ^ h2 == set()

    assert h1 & h2 == set(h1.keys())


@mock.patch(
    "access_nri_intake.source.utils.xr.open_dataset",
)
@pytest.mark.parametrize(
    "paths, num_calls",
    [
        (["path1.nc", "path2.nc", "path1.nc"], 2),
        (["path1.nc", "path1.nc", "path1.nc"], 1),
        (["path1.nc", "path2.nc", "path3.nc"], 3),
        ([f"path_{i}.nc" for i in range(6)] * 2, 6),
        ([f"path_{i}.nc" for i in range(100)], 100),
    ],
)
def test_open_dataset_caching(mock_open, paths, num_calls):
    mock_open.reset_mock()
    open_dataset_cached.cache_clear()
    mock_open.side_effect = lambda *args, **kwargs: xr.Dataset(
        {"data": ("x", [1, 2, 3])}
    )

    for p in paths:
        open_dataset_cached(p)

    assert mock_open.call_count == num_calls

    path = "dummy_path.nc"
    ds1 = open_dataset_cached(path)
    ds2 = open_dataset_cached(path)

    N_MAX = 11
    assert open_dataset_cached.cache_info().hits >= 1
    assert open_dataset_cached.cache_info().currsize < N_MAX

    assert ds1 is ds2  # Should be the same object due to caching


@pytest.mark.parametrize(
    "cell_methods, expected",
    [
        # Empty list
        ([], "unknown"),
        # Single method
        (["time: mean"], "mean"),
        # Single method and an empty string
        (["time: mean", ""], "mean"),
        # Multiple different methods
        (["time: mean", "time: sum"], "mean,sum"),
        # Duplicate methods (should be deduplicated)
        (["time: mean", "time: mean", "time: mean"], "mean"),
        # Mix of empty and non-empty
        (["", "time: mean", "", "time: sum"], "mean,sum"),
        # Methods with area operations
        (["area: mean time: mean", "time: sum"], "mean,sum"),
        # Complex cell methods
        (
            ["area: mean time: maximum", "time: mean", "area: sum time: minimum"],
            "maximum,mean,minimum",
        ),
        # Only empty strings
        (["", "", ""], "unknown"),
        # Methods without time (should return unknown)
        (["area: mean", "lat: sum"], "unknown"),
        # Mix of time and non-time methods
        (["area: mean", "time: point"], "point"),
        (["", "time: mean", "time: mean_pow(02)"], "mean,mean_pow(02)"),
    ],
)
def test_parse_variable_cell_methods(cell_methods, expected):
    result = _parse_variable_cell_methods(cell_methods)
    assert result == expected
