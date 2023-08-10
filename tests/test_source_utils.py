# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import pytest
import xarray as xr

from access_nri_intake.source.utils import (
    get_timeinfo,
    parse_access_filename,
    parse_access_ncfile,
)


@pytest.mark.parametrize(
    "filename, expected",
    [
        # Example ACCESS-CM2 filenames
        ("bz687a.pm107912_mon", ("bz687a_pmXXXXXX_mon", "107912", "1mon")),
        ("bz687a.p7107912_mon", ("bz687a_p7XXXXXX_mon", "107912", "1mon")),
        ("bz687a.p7107912_dai", ("bz687a_p7XXXXXX_dai", "107912", "1day")),
        ("iceh_m.2014-06", ("iceh_m_XXXX_XX", "2014-06", None)),
        ("iceh.1917-05-daily", ("iceh_XXXX_XX_daily", "1917-05", "1day")),
        ("ocean_bgc_ann", ("ocean_bgc_ann", None, "1yr")),
        ("ocean_daily", ("ocean_daily", None, "1day")),
        # Example ACCESS-ESM1.5 filenames
        (
            "PI-GWL-B2035.pe-109904_dai",
            ("PI_GWL_B2035_pe_XXXXXX_dai", "109904", "1day"),
        ),
        (
            "PI-GWL-B2035.pa-109904_mon",
            ("PI_GWL_B2035_pa_XXXXXX_mon", "109904", "1mon"),
        ),
        (
            "PI-1pct-02.pe-011802_dai.nc_dai",
            ("PI_1pct_02_pe_XXXXXX_dai_nc_dai", "011802", "1day"),
        ),
        ("iceh.1917-05", ("iceh_XXXX_XX", "1917-05", None)),
        # Example ACCESS-OM2 filenames
        ("iceh.057-daily", ("iceh_XXX_daily", "057", "1day")),
        ("ocean", ("ocean", None, None)),
        ("ocean_month", ("ocean_month", None, "1mon")),
        ("ocean_daily_3d_vhrho_nt_07", ("ocean_daily_3d_vhrho_nt_XX", "07", "1day")),
        (
            "oceanbgc-3d-caco3-1-yearly-mean-y_2015",
            ("oceanbgc_3d_caco3_1_yearly_mean_y_XXXX", "2015", "1yr"),
        ),
        (
            "oceanbgc-2d-wdet100-1-daily-mean-y_2015",
            ("oceanbgc_2d_wdet100_1_daily_mean_y_XXXX", "2015", "1day"),
        ),
        (
            "ocean-3d-v-1-monthly-pow02-ym_1958_04",
            ("ocean_3d_v_1_monthly_pow02_ym_XXXX_XX", "1958_04", "1mon"),
        ),
        (
            "ocean-2d-sfc_salt_flux_restore-1-monthly-mean-ym_1958_04",
            (
                "ocean_2d_sfc_salt_flux_restore_1_monthly_mean_ym_XXXX_XX",
                "1958_04",
                "1mon",
            ),
        ),
        (
            "oceanbgc-3d-phy-1-daily-mean-3-sigfig-5-daily-ymd_2020_12_01",
            (
                "oceanbgc_3d_phy_1_daily_mean_3_sigfig_5_daily_ymd_XXXX_XX_XX",
                "2020_12_01",
                "1day",
            ),
        ),
        ("iceh.1985-08-31", ("iceh_XXXX_XX_XX", "1985-08-31", None)),
    ],
)
def test_parse_access_filename(filename, expected):
    assert parse_access_filename(filename) == expected


@pytest.mark.parametrize(
    "filename, expected",
    [
        (
            "access-om2/output000/ocean/ocean_grid.nc",
            (
                "ocean_grid.nc",
                "ocean_grid",
                None,
                "fx",
                "none",
                "none",
                ["geolat_t", "geolon_t"],
                ["tracer latitude", "tracer longitude"],
                [],
                ["time: point", "time: point"],
            ),
        ),
        (
            "access-om2/output000/ocean/ocean.nc",
            (
                "ocean.nc",
                "ocean",
                None,
                "1yr",
                "1900-01-01, 00:00:00",
                "1910-01-01, 00:00:00",
                ["temp", "time_bounds"],
                ["Conservative temperature", "time axis boundaries"],
                ["sea_water_conservative_temperature"],
                ["time: mean"],
            ),
        ),
        (
            "access-om2/output000/ocean/ocean_month.nc",
            (
                "ocean_month.nc",
                "ocean_month",
                None,
                "1mon",
                "1900-01-01, 00:00:00",
                "1910-01-01, 00:00:00",
                ["mld", "time_bounds"],
                [
                    "mixed layer depth determined by density criteria",
                    "time axis boundaries",
                ],
                ["ocean_mixed_layer_thickness_defined_by_sigma_t"],
                ["time: mean"],
            ),
        ),
        (
            "access-om2/output000/ocean/ocean_month_inst_nobounds.nc",
            (
                "ocean_month_inst_nobounds.nc",
                "ocean_month_inst_nobounds",
                None,
                "1mon",
                "1900-01-16, 12:00:00",
                "1900-01-16, 12:00:00",
                ["mld"],
                ["mixed layer depth determined by density criteria"],
                ["ocean_mixed_layer_thickness_defined_by_sigma_t"],
                ["time: mean"],
            ),
        ),
        (
            "access-om2/output000/ice/OUTPUT/iceh.1900-01.nc",
            (
                "iceh.1900-01.nc",
                "iceh_XXXX_XX",
                "1900-01",
                "1mon",
                "1900-01-01, 00:00:00",
                "1900-02-01, 00:00:00",
                ["TLAT", "TLON", "aice_m", "tarea", "time_bounds"],
                [
                    "T grid center latitude",
                    "T grid center longitude",
                    "ice area  (aggregate)",
                    "area of T grid cells",
                    "boundaries for time-averaging interval",
                ],
                [],
                ["time: mean"],
            ),
        ),
        (
            "access-cm2/by578/history/atm/netCDF/by578a.pd201501_dai.nc",
            (
                "by578a.pd201501_dai.nc",
                "by578a_pdXXXXXX_dai",
                "201501",
                "1day",
                "2015-01-01, 00:00:00",
                "2015-02-01, 00:00:00",
                ["fld_s03i236"],
                ["TEMPERATURE AT 1.5M"],
                ["air_temperature"],
                ["time: mean"],
            ),
        ),
        (
            "access-cm2/by578/history/ice/iceh_d.2015-01.nc",
            (
                "iceh_d.2015-01.nc",
                "iceh_d_XXXX_XX",
                "2015-01",
                "1day",
                "2015-01-01, 00:00:00",
                "2015-02-01, 00:00:00",
                ["TLAT", "TLON", "aice", "tarea", "time_bounds"],
                [
                    "T grid center latitude",
                    "T grid center longitude",
                    "ice area  (aggregate)",
                    "area of T grid cells",
                    "boundaries for time-averaging interval",
                ],
                [],
                ["time: mean"],
            ),
        ),
        (
            "access-cm2/by578/history/ocn/ocean_daily.nc-20150630",
            (
                "ocean_daily.nc-20150630",
                "ocean_daily",
                None,
                "1day",
                "2015-01-01, 00:00:00",
                "2015-07-01, 00:00:00",
                ["sst", "time_bounds"],
                ["Potential temperature", "time axis boundaries"],
                ["sea_surface_temperature"],
                ["time: mean"],
            ),
        ),
        (
            "access-cm2/by578/history/ocn/ocean_scalar.nc-20150630",
            (
                "ocean_scalar.nc-20150630",
                "ocean_scalar",
                None,
                "1mon",
                "2015-01-01, 00:00:00",
                "2015-07-01, 00:00:00",
                ["temp_global_ave", "time_bounds"],
                ["Global mean temp in liquid seawater", "time axis boundaries"],
                ["sea_water_potential_temperature"],
                ["time: mean"],
            ),
        ),
        (
            "access-esm1-5/history/atm/netCDF/HI-C-05-r1.pa-185001_mon.nc",
            (
                "HI-C-05-r1.pa-185001_mon.nc",
                "HI_C_05_r1_pa_XXXXXX_mon",
                "185001",
                "1mon",
                "1850-01-01, 00:00:00",
                "1850-02-01, 00:00:00",
                ["fld_s03i236"],
                ["TEMPERATURE AT 1.5M"],
                ["air_temperature"],
                ["time: mean"],
            ),
        ),
        (
            "access-esm1-5/history/ice/iceh.1850-01.nc",
            (
                "iceh.1850-01.nc",
                "iceh_XXXX_XX",
                "1850-01",
                "1mon",
                "1850-01-01, 00:00:00",
                "1850-02-01, 00:00:00",
                ["TLAT", "TLON", "aice", "tarea", "time_bounds"],
                [
                    "T grid center latitude",
                    "T grid center longitude",
                    "ice area  (aggregate)",
                    "area of T grid cells",
                    "boundaries for time-averaging interval",
                ],
                [],
                ["time: mean"],
            ),
        ),
        (
            "access-esm1-5/history/ocn/ocean_bgc_ann.nc-18501231",
            (
                "ocean_bgc_ann.nc-18501231",
                "ocean_bgc_ann",
                None,
                "1yr",
                "1849-12-30, 00:00:00",
                "1850-12-30, 00:00:00",
                ["fgco2_raw", "time_bounds"],
                ["Flux into ocean - DIC, inc. anth.", "time axis boundaries"],
                [],
                ["time: mean"],
            ),
        ),
        (
            "access-esm1-5/history/ocn/ocean_bgc.nc-18501231",
            (
                "ocean_bgc.nc-18501231",
                "ocean_bgc",
                None,
                "1mon",
                "1849-12-30, 00:00:00",
                "1850-12-30, 00:00:00",
                ["o2", "time_bounds"],
                ["o2", "time axis boundaries"],
                [],
                ["time: mean"],
            ),
        ),
    ],
)
def test_parse_access_ncfile(test_data, filename, expected):
    file = str(test_data / Path(filename))

    assert parse_access_ncfile(file) == expected


@pytest.mark.parametrize(
    "start_end, expected",
    [
        ([0.0, 0.00625], ("1900-01-01, 00:00:00", "1900-01-01, 00:09:00", "subhr")),
        ([0.0, 0.125], ("1900-01-01, 00:00:00", "1900-01-01, 03:00:00", "3hr")),
        ([0.0, 0.25], ("1900-01-01, 00:00:00", "1900-01-01, 06:00:00", "6hr")),
        ([0.0, 1.0], ("1900-01-01, 00:00:00", "1900-01-02, 00:00:00", "1day")),
        ([0.0, 31.0], ("1900-01-01, 00:00:00", "1900-02-01, 00:00:00", "1mon")),
        ([0.0, 90.0], ("1900-01-01, 00:00:00", "1900-04-01, 00:00:00", "3mon")),
        ([0.0, 365.0], ("1900-01-01, 00:00:00", "1901-01-01, 00:00:00", "1yr")),
        ([0.0, 730.0], ("1900-01-01, 00:00:00", "1902-01-01, 00:00:00", "2yr")),
    ],
)
@pytest.mark.parametrize("bounds", [True, False])
def test_get_timeinfo(start_end, expected, bounds):
    if bounds:
        time = (start_end[0] + start_end[1]) / 2
        ds = xr.Dataset(
            data_vars={
                "dummy": ("time", [0]),
                "time_bounds": (("time", "nv"), [start_end]),
            },
            coords={"time": [time]},
        )
        ds["time"].attrs = dict(bounds="time_bounds")
    else:
        ds = xr.Dataset(
            data_vars={"dummy": ("time", [0, 0])},
            coords={"time": start_end},
        )

    ds["time"].attrs |= dict(
        units="days since 1900-01-01 00:00:00", calendar="GREGORIAN"
    )

    assert get_timeinfo(ds) == expected
