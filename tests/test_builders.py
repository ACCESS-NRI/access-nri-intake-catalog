# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import intake
import pandas as pd
import pytest

from access_nri_intake.source import CORE_COLUMNS, builders


@pytest.mark.parametrize(
    "basedirs, builder, kwargs, num_assets, num_valid_assets, num_datasets",
    [
        (["access-om2"], "AccessOm2Builder", {}, 12, 12, 6),
        (
            ["access-cm2/by578", "access-cm2/by578a"],
            "AccessCm2Builder",
            {"ensemble": True},
            18,
            14,
            7,
        ),
        (["access-esm1-5"], "AccessEsm15Builder", {"ensemble": False}, 11, 11, 11),
        (["access-om3"], "AccessOm3Builder", {}, 12, 12, 6),
    ],
)
def test_builder_build(
    tmp_path,
    test_data,
    basedirs,
    builder,
    kwargs,
    num_assets,
    num_valid_assets,
    num_datasets,
):
    """
    Test the various steps of the build process
    """
    Builder = getattr(builders, builder)
    path = [str(test_data / Path(basedir)) for basedir in basedirs]
    builder = Builder(path, **kwargs)

    builder.get_assets()
    assert isinstance(builder.assets, list)
    assert len(builder.assets) == num_assets

    builder.build()
    assert isinstance(builder.df, pd.DataFrame)
    assert len(builder.df) == num_valid_assets
    assert all([col in builder.df.columns for col in CORE_COLUMNS])

    builder.save(name="test", description="test datastore", directory=str(tmp_path))

    cat = intake.open_esm_datastore(
        str(tmp_path / "test.json"),
        columns_with_iterables=builder.columns_with_iterables,
    )
    assert len(cat.df) == num_valid_assets
    assert len(cat) == num_datasets


@pytest.mark.parametrize(
    "filename, builder, realm, member, file_id",
    [
        (
            "access-om2/output000/ocean/ocean.nc",
            "AccessOm2Builder",
            "ocean",
            None,
            "ocean",
        ),
        (
            "access-om2/output000/ice/OUTPUT/iceh.1900-01.nc",
            "AccessOm2Builder",
            "seaIce",
            None,
            "iceh_XXXX_XX",
        ),
        (
            "access-cm2/by578/history/atm/netCDF/by578a.pd201501_dai.nc",
            "AccessCm2Builder",
            "atmos",
            "by578",
            "a_pdXXXXXX_dai",
        ),
        (
            "access-cm2/by578a/history/atm/netCDF/by578aa.pd201501_dai.nc",
            "AccessCm2Builder",
            "atmos",
            "by578a",
            "a_pdXXXXXX_dai",
        ),
        (
            "access-cm2/by578/history/ice/iceh_d.2015-01.nc",
            "AccessCm2Builder",
            "seaIce",
            "by578",
            "iceh_d_XXXX_XX",
        ),
        (
            "access-cm2/by578/history/ocn/ocean_daily.nc-20150630",
            "AccessCm2Builder",
            "ocean",
            "by578",
            "ocean_daily",
        ),
        (
            "access-om3/output000/GMOM_JRA_WD.mom6.h.sfc_1900_01_02.nc",
            "AccessOm3Builder",
            "ocean",
            None,
            "GMOM_JRA_WD_mom6_h_sfc_XXXX_XX_XX",
        ),
        (
            "access-om3/output000/GMOM_JRA_WD.cice.h.1900-01-01.nc",
            "AccessOm3Builder",
            "seaIce",
            None,
            "GMOM_JRA_WD_cice_h_XXXX_XX_XX",
        ),
        (
            "access-om3/output000/GMOM_JRA_WD.ww3.hi.1900-01-02-00000.nc",
            "AccessOm3Builder",
            "wave",
            None,
            "GMOM_JRA_WD_ww3_hi_XXXX_XX_XX_XXXXX",
        ),
    ],
)
def test_builder_parser(test_data, filename, builder, realm, member, file_id):
    Builder = getattr(builders, builder)
    info = Builder.parser(str(test_data / filename))
    assert info["realm"] == realm
    if member:
        assert info["member"] == member
    assert info["file_id"] == file_id


def test_builder_columns_with_iterables(test_data):
    builder = builders.AccessOm2Builder(str(test_data / "access-om2"))
    assert not builder.columns_with_iterables
    builder.build()
    assert sorted(list(builder.columns_with_iterables)) == sorted(
        [
            col
            for col, val in builder.df.map(type).isin([list, tuple, set]).any().items()
            if val
        ]
    )


@pytest.mark.parametrize(
    "builder, filename, expected",
    [
        # Example ACCESS-CM2 filenames
        (
            builders.AccessCm2Builder,
            "bz687a.pm107912_mon",
            ("bz687a_pmXXXXXX_mon", "107912", (1, "mon")),
        ),
        (
            builders.AccessCm2Builder,
            "bz687a.p7107912_mon",
            ("bz687a_p7XXXXXX_mon", "107912", (1, "mon")),
        ),
        (
            builders.AccessCm2Builder,
            "bz687a.p7107912_dai",
            ("bz687a_p7XXXXXX_dai", "107912", (1, "day")),
        ),
        (
            builders.AccessCm2Builder,
            "iceh_m.2014-06",
            ("iceh_m_XXXX_XX", "2014-06", None),
        ),
        (
            builders.AccessCm2Builder,
            "iceh.1917-05-daily",
            ("iceh_XXXX_XX_daily", "1917-05", (1, "day")),
        ),
        (
            builders.AccessCm2Builder,
            "iceh_03h.2016-01-3hourly",
            ("iceh_03h_XXXX_XX_3hourly", "2016-01", None),
        ),
        (
            builders.AccessCm2Builder,
            "ocean_bgc_ann",
            ("ocean_bgc_ann", None, (1, "yr")),
        ),
        (builders.AccessCm2Builder, "ocean_daily", ("ocean_daily", None, (1, "day"))),
        # Example ACCESS-ESM1.5 filenames
        (
            builders.AccessEsm15Builder,
            "PI-GWL-B2035.pe-109904_dai",
            ("PI_GWL_B2035_pe_XXXXXX_dai", "109904", (1, "day")),
        ),
        (
            builders.AccessEsm15Builder,
            "PI-GWL-B2035.pa-109904_mon",
            ("PI_GWL_B2035_pa_XXXXXX_mon", "109904", (1, "mon")),
        ),
        (
            builders.AccessEsm15Builder,
            "PI-1pct-02.pe-011802_dai.nc_dai",
            ("PI_1pct_02_pe_XXXXXX_dai_nc_dai", "011802", (1, "day")),
        ),
        (
            builders.AccessEsm15Builder,
            "iceh.1917-05",
            ("iceh_XXXX_XX", "1917-05", None),
        ),
        # Example ACCESS-OM2 filenames
        (
            builders.AccessOm2Builder,
            "iceh.057-daily",
            ("iceh_XXX_daily", "057", (1, "day")),
        ),
        (
            builders.AccessOm2Builder,
            "iceh.1985-08-31",
            ("iceh_XXXX_XX_XX", "1985-08-31", None),
        ),
        (builders.AccessOm2Builder, "ocean", ("ocean", None, None)),
        (builders.AccessOm2Builder, "ocean_month", ("ocean_month", None, (1, "mon"))),
        (
            builders.AccessOm2Builder,
            "ocean-2d-area_t",
            ("ocean_2d_area_t", None, None),
        ),
        (
            builders.AccessOm2Builder,
            "ocean_daily_3d_pot_rho_1",
            ("ocean_daily_3d_pot_rho_1", None, (1, "day")),
        ),
        (
            builders.AccessOm2Builder,
            "ocean_daily_3d_vhrho_nt_07",
            ("ocean_daily_3d_vhrho_nt_XX", "07", (1, "day")),
        ),
        (
            builders.AccessOm2Builder,
            "ocean-3d-v-1-monthly-pow02-ym_1958_04",
            ("ocean_3d_v_1_monthly_pow02_ym_XXXX_XX", "1958_04", (1, "mon")),
        ),
        (
            builders.AccessOm2Builder,
            "ocean-2d-sfc_salt_flux_restore-1-monthly-mean-ym_1958_04",
            (
                "ocean_2d_sfc_salt_flux_restore_1_monthly_mean_ym_XXXX_XX",
                "1958_04",
                (1, "mon"),
            ),
        ),
        (
            builders.AccessOm2Builder,
            "ocean-2d-sea_level-540-seconds-snap-ym_2022_04_01",
            (
                "ocean_2d_sea_level_540_seconds_snap_ym_XXXX_XX_XX",
                "2022_04_01",
                None,
            ),
        ),
        (
            builders.AccessOm2Builder,
            "ocean-3d-salt-1-daily-mean-ym_2018_10_jmax511_sigfig4",
            (
                "ocean_3d_salt_1_daily_mean_ym_XXXX_XX_jmax511_sigfig4",
                "2018_10",
                (1, "day"),
            ),
        ),
        (
            builders.AccessOm2Builder,
            "oceanbgc-3d-caco3-1-yearly-mean-y_2015",
            ("oceanbgc_3d_caco3_1_yearly_mean_y_XXXX", "2015", (1, "yr")),
        ),
        (
            builders.AccessOm2Builder,
            "oceanbgc-2d-wdet100-1-daily-mean-y_2015",
            ("oceanbgc_2d_wdet100_1_daily_mean_y_XXXX", "2015", (1, "day")),
        ),
        (
            builders.AccessOm2Builder,
            "oceanbgc-3d-phy-1-daily-mean-3-sigfig-5-daily-ymd_2020_12_01",
            (
                "oceanbgc_3d_phy_1_daily_mean_3_sigfig_5_daily_ymd_XXXX_XX_XX",
                "2020_12_01",
                (1, "day"),
            ),
        ),
        (
            builders.AccessOm2Builder,
            "rregionPrydz_temp_xflux_adv",
            ("rregionPrydz_temp_xflux_adv", None, None),
        ),
        # Example ACCESS-OM3 filenames
        (
            builders.AccessOm3Builder,
            "GMOM_JRA_WD.ww3.hi.1958-01-02-00000",
            (
                "GMOM_JRA_WD_ww3_hi_XXXX_XX_XX_XXXXX",
                "1958-01-02-00000",
                None,
            ),
        ),
        (
            builders.AccessOm3Builder,
            "GMOM_JRA.cice.h.1900-01-01",
            (
                "GMOM_JRA_cice_h_XXXX_XX_XX",
                "1900-01-01",
                None,
            ),
        ),
        (
            builders.AccessOm3Builder,
            "GMOM_JRA.mom6.ocean_sfc_1900_01_01",
            (
                "GMOM_JRA_mom6_ocean_sfc_XXXX_XX_XX",
                "1900_01_01",
                None,
            ),
        ),
        (
            builders.AccessOm3Builder,
            "GMOM_JRA.mom6.sfc_1900_01_01",
            (
                "GMOM_JRA_mom6_sfc_XXXX_XX_XX",
                "1900_01_01",
                None,
            ),
        ),
        (
            builders.AccessOm3Builder,
            "GMOM_JRA.mom6.sfc_1900_01",
            (
                "GMOM_JRA_mom6_sfc_XXXX_XX",
                "1900_01",
                None,
            ),
        ),
        (
            builders.AccessOm3Builder,
            "GMOM_JRA.mom6.static",
            (
                "GMOM_JRA_mom6_static",
                None,
                None,
            ),
        ),
    ],
)
def test_parse_access_filename(builder, filename, expected):
    assert builder.parse_access_filename(filename) == expected


@pytest.mark.parametrize(
    "builder, filename, expected",
    [
        (
            builders.AccessOm2Builder,
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
                ["", ""],
                ["time: point", "time: point"],
                ["degrees_N", "degrees_E"],
            ),
        ),
        (
            builders.AccessOm2Builder,
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
                ["sea_water_conservative_temperature", ""],
                ["time: mean", ""],
                ["K", "days"],
            ),
        ),
        (
            builders.AccessOm2Builder,
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
                ["ocean_mixed_layer_thickness_defined_by_sigma_t", ""],
                ["time: mean", ""],
                ["m", "days"],
            ),
        ),
        (
            builders.AccessOm2Builder,
            "access-om2/output000/ocean/ocean_month_inst_nobounds.nc",
            (
                "ocean_month_inst_nobounds.nc",
                "ocean_month_inst_nobounds",
                None,
                "1mon",
                "1900-01-01, 00:00:00",
                "1900-02-01, 00:00:00",
                ["mld"],
                ["mixed layer depth determined by density criteria"],
                ["ocean_mixed_layer_thickness_defined_by_sigma_t"],
                ["time: mean"],
                ["m"],
            ),
        ),
        (
            builders.AccessOm2Builder,
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
                ["", "", "", "", ""],
                ["", "", "time: mean", "", ""],
                [
                    "degrees_north",
                    "degrees_east",
                    "1",
                    "m^2",
                    "days since 1900-01-01 00:00:00",
                ],
            ),
        ),
        (
            builders.AccessCm2Builder,
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
                ["K"],
            ),
        ),
        (
            builders.AccessCm2Builder,
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
                ["", "", "", "", ""],
                ["", "", "time: mean", "", ""],
                [
                    "degrees_north",
                    "degrees_east",
                    "1",
                    "m^2",
                    "days since 1850-01-01 00:00:00",
                ],
            ),
        ),
        (
            builders.AccessCm2Builder,
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
                ["sea_surface_temperature", ""],
                ["time: mean", ""],
                ["K", "days"],
            ),
        ),
        (
            builders.AccessCm2Builder,
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
                ["sea_water_potential_temperature", ""],
                ["time: mean", ""],
                ["deg_C", "days"],
            ),
        ),
        (
            builders.AccessEsm15Builder,
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
                ["K"],
            ),
        ),
        (
            builders.AccessEsm15Builder,
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
                ["", "", "", "", ""],
                ["", "", "time: mean", "", ""],
                [
                    "degrees_north",
                    "degrees_east",
                    "1",
                    "m^2",
                    "days since 0001-01-01 00:00:00",
                ],
            ),
        ),
        (
            builders.AccessEsm15Builder,
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
                ["", ""],
                ["time: mean", ""],
                ["mmol/m^2/s", "days"],
            ),
        ),
        (
            builders.AccessEsm15Builder,
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
                ["", ""],
                ["time: mean", ""],
                ["mmol/m^3", "days"],
            ),
        ),
        (
            builders.AccessOm3Builder,
            "access-om3/output000/GMOM_JRA_WD.mom6.h.native_1900_01.nc",
            (
                "GMOM_JRA_WD.mom6.h.native_1900_01.nc",
                "GMOM_JRA_WD_mom6_h_native_XXXX_XX",
                "1900_01",
                "1mon",
                "1900-01-01, 00:00:00",
                "1900-02-01, 00:00:00",
                ["average_DT", "average_T1", "average_T2", "thetao", "time_bnds"],
                [
                    "Length of average period",
                    "Start time for average period",
                    "End time for average period",
                    "Sea Water Potential Temperature",
                    "time axis boundaries",
                ],
                ["", "", "", "sea_water_potential_temperature", ""],
                ["", "", "", "area:mean zl:mean yh:mean xh:mean time: mean", ""],
                [
                    "days",
                    "days since 0001-01-01 00:00:00",
                    "days since 0001-01-01 00:00:00",
                    "degC",
                    "days since 0001-01-01 00:00:00",
                ],
            ),
        ),
        (
            builders.AccessOm3Builder,
            "access-om3/output000/GMOM_JRA_WD.mom6.h.sfc_1900_01_02.nc",
            (
                "GMOM_JRA_WD.mom6.h.sfc_1900_01_02.nc",
                "GMOM_JRA_WD_mom6_h_sfc_XXXX_XX_XX",
                "1900_01_02",
                "1day",
                "1900-01-01, 00:00:00",
                "1900-01-02, 00:00:00",
                ["average_DT", "average_T1", "average_T2", "time_bnds", "tos"],
                [
                    "Length of average period",
                    "Start time for average period",
                    "End time for average period",
                    "time axis boundaries",
                    "Sea Surface Temperature",
                ],
                ["", "", "", "", "sea_surface_temperature"],
                ["", "", "", "", "area:mean yh:mean xh:mean time: mean"],
                [
                    "days",
                    "days since 0001-01-01 00:00:00",
                    "days since 0001-01-01 00:00:00",
                    "days since 0001-01-01 00:00:00",
                    "degC",
                ],
            ),
        ),
        (
            builders.AccessOm3Builder,
            "access-om3/output000/GMOM_JRA_WD.mom6.h.static.nc",
            (
                "GMOM_JRA_WD.mom6.h.static.nc",
                "GMOM_JRA_WD_mom6_h_static",
                None,
                "fx",
                "none",
                "none",
                ["geolat", "geolon"],
                ["Latitude of tracer (T) points", "Longitude of tracer (T) points"],
                ["", ""],
                ["time: point", "time: point"],
                ["degrees_north", "degrees_east"],
            ),
        ),
        (
            builders.AccessOm3Builder,
            "access-om3/output000/GMOM_JRA_WD.mom6.h.z_1900_01.nc",
            (
                "GMOM_JRA_WD.mom6.h.z_1900_01.nc",
                "GMOM_JRA_WD_mom6_h_z_XXXX_XX",
                "1900_01",
                "1mon",
                "1900-01-01, 00:00:00",
                "1900-02-01, 00:00:00",
                ["average_DT", "average_T1", "average_T2", "thetao", "time_bnds"],
                [
                    "Length of average period",
                    "Start time for average period",
                    "End time for average period",
                    "Sea Water Potential Temperature",
                    "time axis boundaries",
                ],
                ["", "", "", "sea_water_potential_temperature", ""],
                ["", "", "", "area:mean z_l:mean yh:mean xh:mean time: mean", ""],
                [
                    "days",
                    "days since 0001-01-01 00:00:00",
                    "days since 0001-01-01 00:00:00",
                    "degC",
                    "days since 0001-01-01 00:00:00",
                ],
            ),
        ),
        (
            builders.AccessOm3Builder,
            "access-om3/output000/GMOM_JRA_WD.cice.h.1900-01-01.nc",
            (
                "GMOM_JRA_WD.cice.h.1900-01-01.nc",
                "GMOM_JRA_WD_cice_h_XXXX_XX_XX",
                "1900-01-01",
                "1day",
                "1900-01-01, 00:00:00",
                "1900-01-02, 00:00:00",
                ["TLAT", "TLON", "aice", "tarea", "time_bounds"],
                [
                    "T grid center latitude",
                    "T grid center longitude",
                    "ice area  (aggregate)",
                    "area of T grid cells",
                    "time interval endpoints",
                ],
                ["", "", "", "", ""],
                ["", "", "time: mean", "", ""],
                [
                    "degrees_north",
                    "degrees_east",
                    "1",
                    "m^2",
                    "days since 0000-01-01 00:00:00",
                ],
            ),
        ),
        (
            builders.AccessOm3Builder,
            "access-om3/output000/GMOM_JRA_WD.ww3.hi.1900-01-02-00000.nc",
            (
                "GMOM_JRA_WD.ww3.hi.1900-01-02-00000.nc",
                "GMOM_JRA_WD_ww3_hi_XXXX_XX_XX_XXXXX",
                "1900-01-02-00000",
                "fx",  # WW3 provides no time bounds
                "1900-01-02, 00:00:00",
                "1900-01-02, 00:00:00",
                ["EF", "mapsta"],
                ["1D spectral density", "map status"],
                ["", ""],
                ["", ""],
                ["m2 s", "unitless"],
            ),
        ),
    ],
)
def test_parse_access_ncfile(test_data, builder, filename, expected):
    file = str(test_data / Path(filename))

    assert builder.parse_access_ncfile(file) == expected
