# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0


import pytest
import xarray as xr

from access_nri_intake.source.utils import (
    FILENAME_TO_FREQ,
    AccessTimeParser,
    EmptyFileError,
    GenericTimeParser,
    GfdlTimeParser,
)


@pytest.mark.parametrize(
    "times, bounds, ffreq, expected",
    [
        (
            [365 / 2],
            False,
            "1year",
            ("1900-01-01, 00:00:00", "1901-01-01, 00:00:00", "1yr"),
        ),
        (
            [31 / 2],
            False,
            "monthly",
            ("1900-01-01, 00:00:00", "1900-02-01, 00:00:00", "1mon"),
        ),
        (
            [1.5 / 24],
            False,
            "3hour",
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
def test_genericparser_get_timeinfo(times, bounds, ffreq, expected, tmp_path):
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

    # import pdb; pdb.set_trace()

    if ffreq is not None:
        ds.to_netcdf(path=tmp_path / ffreq)
        ds = xr.open_dataset(tmp_path / ffreq, decode_cf=False)

    assert GenericTimeParser(ds, time_dim="time")() == expected


@pytest.mark.parametrize(
    "times, bounds, ffreq, expected",
    [
        (
            [365 / 2],
            False,
            "yearly",
            ("1900-01-01, 00:00:00", "1901-01-01, 00:00:00", "1yr"),
        ),
        (
            [31 / 2],
            False,
            "monthly",
            ("1900-01-01, 00:00:00", "1900-02-01, 00:00:00", "1mon"),
        ),
        (
            [1.5 / 24],
            False,
            "3hr",
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
@pytest.mark.parametrize(
    "parser",
    [AccessTimeParser, GenericTimeParser],
)
def test_generic_time_parser(times, bounds, ffreq, expected, parser, tmp_path):
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

    if ffreq is not None:
        ds.to_netcdf(path=tmp_path / ffreq)
        ds = xr.open_dataset(tmp_path / ffreq, decode_cf=False)

    assert parser(ds, time_dim="time")() == expected


@pytest.mark.parametrize(
    "parser",
    [AccessTimeParser, GenericTimeParser],
)
def test_generic_time_parser_warnings(parser):
    times = [1.5 / 24 / 60]

    ds = xr.Dataset(
        data_vars={"dummy": ("time", [0] * len(times))},
        coords={"time": times},
    )

    ds["time"].attrs |= dict(
        units="days since 1900-01-01 00:00:00", calendar="GREGORIAN"
    )

    with pytest.warns(
        match="Cannot infer start and end times for subhourly frequencies."
    ):
        parser(ds, time_dim="time")._guess_start_end_dates(0, 1, (1, "s"))


@pytest.mark.parametrize(
    "parser",
    [AccessTimeParser, GenericTimeParser, GfdlTimeParser],
)
def test_generic_empty_file_error(parser):
    times = []

    ds = xr.Dataset(
        data_vars={"dummy": ("time", [])},
        coords={"time": times},
    )

    ds["time"].attrs |= dict(
        units="days since 1900-01-01 00:00:00", calendar="GREGORIAN"
    )

    with pytest.raises(EmptyFileError):
        parser(ds, time_dim="time")()


@pytest.mark.parametrize(
    "parser",
    [AccessTimeParser, GenericTimeParser, GfdlTimeParser],
)
@pytest.mark.parametrize("clue,freq", list(FILENAME_TO_FREQ.items()))
@pytest.mark.parametrize("no", [1, 2, 4, 6, 12])
def test_generic__guess_freq_from_fn(parser, clue, freq, no, tmp_path):
    times = [1.5 / 24 / 60]

    ds = xr.Dataset(
        data_vars={"dummy": ("time", [0] * len(times))},
        coords={"time": times},
    )

    ds["time"].attrs |= dict(
        units="days since 1900-01-01 00:00:00", calendar="GREGORIAN"
    )

    fn = tmp_path / f"{no if no != 1 else ''}{clue}.nc"

    ds.to_netcdf(path=fn)
    # ds.close()
    ds = xr.open_dataset(fn)

    p = parser(ds, time_dim="time")
    assert p._guess_freq_from_fn() == (
        no,
        freq,
    ), f"_guess_freq_from_fn ({p._guess_freq_from_fn()}) could not deduce '{no}, {freq}' from '{clue}' ({fn})"


@pytest.mark.parametrize(
    "parser",
    [AccessTimeParser, GenericTimeParser, GfdlTimeParser],
)
def test_generic__guess_freq_from_fn_no_saved_ds(parser):
    times = [1.5 / 24 / 60]

    ds = xr.Dataset(
        data_vars={"dummy": ("time", [0] * len(times))},
        coords={"time": times},
    )

    ds["time"].attrs |= dict(
        units="days since 1900-01-01 00:00:00", calendar="GREGORIAN"
    )

    p = parser(ds, time_dim="time")
    with pytest.raises(RuntimeError, match="not attached.*filepath"):
        _ = p._guess_freq_from_fn()


@pytest.mark.parametrize(
    "times, ffreq, expected",
    [
        # #378 - temporary deprecation while working out how to handle these cases
        # (These tests basically check if filename frequency + snapshot data works properly)
        # (
        #     [365 / 2],
        #     (1, "yr"),
        #     ("1900-01-01, 00:00:00", "1901-01-01, 00:00:00", "fx"),
        # ),
        # (
        #     [31 / 2],
        #     (1, "mon"),
        #     ("1900-01-01, 00:00:00", "1900-02-01, 00:00:00", "fx"),
        # ),
        # (
        #     [1.5 / 24],
        #     (3, "hr"),
        #     ("1900-01-01, 00:00:00", "1900-01-01, 03:00:00", "fx"),
        # ),
        (
            [1.5 / 24, 4.5 / 24],
            None,
            ("1900-01-01, 00:00:00", "1900-01-01, 06:00:00", "3hr"),
        ),
        (
            [3 / 24, 9 / 24],
            None,
            ("1900-01-01, 00:00:00", "1900-01-01, 12:00:00", "6hr"),
        ),
        (
            [0.5, 1.5],
            None,
            ("1900-01-01, 00:00:00", "1900-01-03, 00:00:00", "1day"),
        ),
        (
            [31 / 2, 45],
            None,
            ("1900-01-01, 00:00:00", "1900-03-01, 00:00:00", "1mon"),
        ),
        (
            [45, 135.5],
            None,
            ("1900-01-01, 00:00:00", "1900-07-01, 00:00:00", "3mon"),
        ),
        (
            [365 / 2, 365 + 365 / 2],
            None,
            ("1900-01-01, 00:00:00", "1902-01-01, 00:00:00", "1yr"),
        ),
        (
            [365, 3 * 365],
            None,
            ("1900-01-01, 00:00:00", "1904-01-01, 00:00:00", "2yr"),
        ),
        (
            [365 / 86400 / 720, 365 / 86400 / 360],  # 1/2 second, 1 second
            None,
            ("1900-01-01, 00:00:00", "1900-01-01, 00:00:01", "subhr"),
        ),
    ],
)
def test_gfdl_time_parser(times, ffreq, expected):
    ds = xr.Dataset(
        data_vars={"dummy": ("time", [0] * len(times))},
        coords={"time": times},
    )

    ds["time"].attrs |= dict(
        units="days since 1900-01-01 00:00:00", calendar="GREGORIAN"
    )

    assert GfdlTimeParser(ds, time_dim="time")() == expected


def test_gfdl_parser_notime(tmp_path):
    ds = xr.Dataset(
        data_vars={"dummy": ("latitude", [0])},
        coords={"latitude": [0]},
    )

    ds.to_netcdf(path=tmp_path / "notime.nc")
    ds = xr.open_dataset(tmp_path / "notime.nc", decode_cf=False)

    assert GfdlTimeParser(ds, time_dim="time")() == (
        "none",
        "none",
        "fx",
    )
