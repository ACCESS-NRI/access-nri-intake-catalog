# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import intake
import pandas as pd
import pytest
import xarray as xr
from intake_esm.source import _get_xarray_open_kwargs, _open_dataset
from intake_esm.utils import OPTIONS

from access_nri_intake.source import CORE_COLUMNS, builders
from access_nri_intake.source.utils import _AccessNCFileInfo


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
            "access-om3.ww3.hi.1958-01-02-00000",
            (
                "access_om3_ww3_hi_XXXX_XX_XX_XXXXX",
                "1958-01-02-00000",
                None,
            ),
        ),
        (
            builders.AccessOm3Builder,
            "access-om3.cice.h.1900-01-01",
            (
                "access_om3_cice_h_XXXX_XX_XX",
                "1900-01-01",
                None,
            ),
        ),
        (
            builders.AccessOm3Builder,
            "access-om3.cice.h.1900-01",
            (
                "access_om3_cice_h_XXXX_XX",
                "1900-01",
                None,
            ),
        ),
        (
            builders.AccessOm3Builder,
            "access-om3.cice.h.1900-01-daily",
            (
                "access_om3_cice_h_XXXX_XX_daily",
                "1900-01",
                (1, "day"),
            ),
        ),
        (
            builders.AccessOm3Builder,
            "access-om3.mom6.ocean_sfc_1900_01_01",
            (
                "access_om3_mom6_ocean_sfc_XXXX_XX_XX",
                "1900_01_01",
                None,
            ),
        ),
        (
            builders.AccessOm3Builder,
            "access-om3.mom6.sfc_1900_01",
            (
                "access_om3_mom6_sfc_XXXX_XX",
                "1900_01",
                None,
            ),
        ),
        (
            builders.AccessOm3Builder,
            "access-om3.mom6.sfc_1900",
            (
                "access_om3_mom6_sfc_XXXX",
                "1900",
                None,
            ),
        ),
        (
            builders.AccessOm3Builder,
            "access-om3.mom6.static",
            (
                "access_om3_mom6_static",
                None,
                None,
            ),
        ),
        (
            builders.AccessOm3Builder,
            "access-om3.mom6.3d.uh.1mon.mean.1900",
            (
                "access_om3_mom6_3d_uh_1mon_mean_XXXX",
                "1900",
                (1, "mon"),
            ),
        ),
        (
            builders.AccessOm3Builder,
            "access-om3.mom6.3d.uh.1mon.mean.1900-01-01-00000",
            (
                "access_om3_mom6_3d_uh_1mon_mean_XXXX_XX_XX_XXXXX",
                "1900-01-01-00000",
                (1, "mon"),
            ),
        ),
        (
            builders.AccessOm3Builder,
            "access-om3.mom6.3d.uh.1mon.mean.1900-01",
            (
                "access_om3_mom6_3d_uh_1mon_mean_XXXX_XX",
                "1900-01",
                (1, "mon"),
            ),
        ),
        (
            builders.AccessOm3Builder,
            "GMOM_JRA_WD.ww3.hi.1900-01-03-00000",
            (
                "GMOM_JRA_WD_ww3_hi_XXXX_XX_XX_XXXXX",
                "1900-01-03-00000",
                None,
            ),
        ),
        (
            builders.AccessOm3Builder,
            "GMOM_JRA_WD.ww3.hi.1900",
            (
                "GMOM_JRA_WD_ww3_hi_XXXX",
                "1900",
                None,
            ),
        ),
        (
            builders.AccessOm3Builder,
            "GMOM_JRA_WD.ww3.hi.1900-01",
            (
                "GMOM_JRA_WD_ww3_hi_XXXX_XX",
                "1900-01",
                None,
            ),
        ),
    ],
)
def test_parse_access_filename(builder, filename, expected):
    assert builder.parse_access_filename(filename) == expected


@pytest.mark.parametrize(
    "compare_files",
    [
        (True),
        (False),
    ],
)
@pytest.mark.parametrize(
    "builder, filename, expected",
    [
        (
            builders.AccessOm2Builder,
            "access-om2/output000/ocean/ocean_grid.nc",
            _AccessNCFileInfo(
                path=None,  # type: ignore
                filename="ocean_grid.nc",
                file_id="ocean_grid",
                filename_timestamp=None,
                frequency="fx",
                start_date="none",
                end_date="none",
                variable=["geolat_t", "geolon_t", "xt_ocean", "yt_ocean"],
                variable_long_name=[
                    "tracer latitude",
                    "tracer longitude",
                    "tcell longitude",
                    "tcell latitude",
                ],
                variable_standard_name=["", "", "", ""],
                variable_cell_methods=["time: point", "time: point", "", ""],
                variable_units=["degrees_N", "degrees_E", "degrees_E", "degrees_N"],
            ),
        ),
        (
            builders.AccessOm2Builder,
            "access-om2/output000/ocean/ocean.nc",
            _AccessNCFileInfo(
                path=None,  # type: ignore
                filename="ocean.nc",
                file_id="ocean",
                filename_timestamp=None,
                frequency="1yr",
                start_date="1900-01-01, 00:00:00",
                end_date="1910-01-01, 00:00:00",
                variable=[
                    "nv",
                    "st_ocean",
                    "temp",
                    "time",
                    "time_bounds",
                    "xt_ocean",
                    "yt_ocean",
                ],
                variable_long_name=[
                    "vertex number",
                    "tcell zstar depth",
                    "Conservative temperature",
                    "time",
                    "time axis boundaries",
                    "tcell longitude",
                    "tcell latitude",
                ],
                variable_standard_name=[
                    "",
                    "",
                    "sea_water_conservative_temperature",
                    "",
                    "",
                    "",
                    "",
                ],
                variable_cell_methods=["", "", "time: mean", "", "", "", ""],
                variable_units=[
                    "none",
                    "meters",
                    "K",
                    "days since 1900-01-01 00:00:00",
                    "days",
                    "degrees_E",
                    "degrees_N",
                ],
            ),
        ),
        (
            builders.AccessOm2Builder,
            "access-om2/output000/ocean/ocean_month.nc",
            _AccessNCFileInfo(
                path=None,  # type: ignore
                filename="ocean_month.nc",
                file_id="ocean_month",
                filename_timestamp=None,
                frequency="1mon",
                start_date="1900-01-01, 00:00:00",
                end_date="1910-01-01, 00:00:00",
                variable=["mld", "nv", "time", "time_bounds", "xt_ocean", "yt_ocean"],
                variable_long_name=[
                    "mixed layer depth determined by density criteria",
                    "vertex number",
                    "time",
                    "time axis boundaries",
                    "tcell longitude",
                    "tcell latitude",
                ],
                variable_standard_name=[
                    "ocean_mixed_layer_thickness_defined_by_sigma_t",
                    "",
                    "",
                    "",
                    "",
                    "",
                ],
                variable_cell_methods=["time: mean", "", "", "", "", ""],
                variable_units=[
                    "m",
                    "none",
                    "days since 1900-01-01 00:00:00",
                    "days",
                    "degrees_E",
                    "degrees_N",
                ],
            ),
        ),
        (
            builders.AccessOm2Builder,
            "access-om2/output000/ocean/ocean_month_inst_nobounds.nc",
            _AccessNCFileInfo(
                path=None,  # type: ignore
                filename="ocean_month_inst_nobounds.nc",
                file_id="ocean_month_inst_nobounds",
                filename_timestamp=None,
                frequency="1mon",
                start_date="1900-01-01, 00:00:00",
                end_date="1900-02-01, 00:00:00",
                variable=["mld", "time", "xt_ocean", "yt_ocean"],
                variable_long_name=[
                    "mixed layer depth determined by density criteria",
                    "time",
                    "tcell longitude",
                    "tcell latitude",
                ],
                variable_standard_name=[
                    "ocean_mixed_layer_thickness_defined_by_sigma_t",
                    "",
                    "",
                    "",
                ],
                variable_cell_methods=["time: mean", "", "", ""],
                variable_units=[
                    "m",
                    "days since 1900-01-01 00:00:00",
                    "degrees_E",
                    "degrees_N",
                ],
            ),
        ),
        (
            builders.AccessOm2Builder,
            "access-om2/output000/ice/OUTPUT/iceh.1900-01.nc",
            _AccessNCFileInfo(
                path=None,  # type: ignore
                filename="iceh.1900-01.nc",
                file_id="iceh_XXXX_XX",
                filename_timestamp="1900-01",
                frequency="1mon",
                start_date="1900-01-01, 00:00:00",
                end_date="1900-02-01, 00:00:00",
                variable=["TLAT", "TLON", "aice_m", "tarea", "time", "time_bounds"],
                variable_long_name=[
                    "T grid center latitude",
                    "T grid center longitude",
                    "ice area  (aggregate)",
                    "area of T grid cells",
                    "model time",
                    "boundaries for time-averaging interval",
                ],
                variable_standard_name=["", "", "", "", "", ""],
                variable_cell_methods=["", "", "time: mean", "", "", ""],
                variable_units=[
                    "degrees_north",
                    "degrees_east",
                    "1",
                    "m^2",
                    "days since 1900-01-01 00:00:00",
                    "days since 1900-01-01 00:00:00",
                ],
            ),
        ),
        (
            builders.AccessCm2Builder,
            "access-cm2/by578/history/atm/netCDF/by578a.pd201501_dai.nc",
            _AccessNCFileInfo(
                path=None,  # type: ignore
                filename="by578a.pd201501_dai.nc",
                file_id="by578a_pdXXXXXX_dai",
                filename_timestamp="201501",
                frequency="1day",
                start_date="2015-01-01, 00:00:00",
                end_date="2015-02-01, 00:00:00",
                variable=["fld_s03i236"],
                variable_long_name=["TEMPERATURE AT 1.5M"],
                variable_standard_name=["air_temperature"],
                variable_cell_methods=["time: mean"],
                variable_units=["K"],
            ),
        ),
        (
            builders.AccessCm2Builder,
            "access-cm2/by578/history/ice/iceh_d.2015-01.nc",
            _AccessNCFileInfo(
                path=None,  # type: ignore
                filename="iceh_d.2015-01.nc",
                file_id="iceh_d_XXXX_XX",
                filename_timestamp="2015-01",
                frequency="1day",
                start_date="2015-01-01, 00:00:00",
                end_date="2015-02-01, 00:00:00",
                variable=["TLAT", "TLON", "aice", "tarea", "time", "time_bounds"],
                variable_long_name=[
                    "T grid center latitude",
                    "T grid center longitude",
                    "ice area  (aggregate)",
                    "area of T grid cells",
                    "model time",
                    "boundaries for time-averaging interval",
                ],
                variable_standard_name=["", "", "", "", "", ""],
                variable_cell_methods=["", "", "time: mean", "", "", ""],
                variable_units=[
                    "degrees_north",
                    "degrees_east",
                    "1",
                    "m^2",
                    "days since 1850-01-01 00:00:00",
                    "days since 1850-01-01 00:00:00",
                ],
            ),
        ),
        (
            builders.AccessCm2Builder,
            "access-cm2/by578/history/ocn/ocean_daily.nc-20150630",
            _AccessNCFileInfo(
                path=None,  # type: ignore
                filename="ocean_daily.nc-20150630",
                file_id="ocean_daily",
                filename_timestamp=None,
                frequency="1day",
                start_date="2015-01-01, 00:00:00",
                end_date="2015-07-01, 00:00:00",
                variable=["nv", "sst", "time", "time_bounds", "xt_ocean", "yt_ocean"],
                variable_long_name=[
                    "vertex number",
                    "Potential temperature",
                    "time",
                    "time axis boundaries",
                    "tcell longitude",
                    "tcell latitude",
                ],
                variable_standard_name=["", "sea_surface_temperature", "", "", "", ""],
                variable_cell_methods=["", "time: mean", "", "", "", ""],
                variable_units=[
                    "none",
                    "K",
                    "days since 1850-01-01 00:00:00",
                    "days",
                    "degrees_E",
                    "degrees_N",
                ],
            ),
        ),
        (
            builders.AccessCm2Builder,
            "access-cm2/by578/history/ocn/ocean_scalar.nc-20150630",
            _AccessNCFileInfo(
                path=None,  # type: ignore
                filename="ocean_scalar.nc-20150630",
                file_id="ocean_scalar",
                filename_timestamp=None,
                frequency="1mon",
                start_date="2015-01-01, 00:00:00",
                end_date="2015-07-01, 00:00:00",
                variable=[
                    "nv",
                    "scalar_axis",
                    "temp_global_ave",
                    "time",
                    "time_bounds",
                ],
                variable_long_name=[
                    "vertex number",
                    "none",
                    "Global mean temp in liquid seawater",
                    "time",
                    "time axis boundaries",
                ],
                variable_standard_name=[
                    "",
                    "",
                    "sea_water_potential_temperature",
                    "",
                    "",
                ],
                variable_cell_methods=["", "", "time: mean", "", ""],
                variable_units=[
                    "none",
                    "none",
                    "deg_C",
                    "days since 1850-01-01 00:00:00",
                    "days",
                ],
            ),
        ),
        (
            builders.AccessEsm15Builder,
            "access-esm1-5/history/atm/netCDF/HI-C-05-r1.pa-185001_mon.nc",
            _AccessNCFileInfo(
                path=None,  # type: ignore
                filename="HI-C-05-r1.pa-185001_mon.nc",
                file_id="HI_C_05_r1_pa_XXXXXX_mon",
                filename_timestamp="185001",
                frequency="1mon",
                start_date="1850-01-01, 00:00:00",
                end_date="1850-02-01, 00:00:00",
                variable=["fld_s03i236"],
                variable_long_name=["TEMPERATURE AT 1.5M"],
                variable_standard_name=["air_temperature"],
                variable_cell_methods=["time: mean"],
                variable_units=["K"],
            ),
        ),
        (
            builders.AccessEsm15Builder,
            "access-esm1-5/history/ice/iceh.1850-01.nc",
            _AccessNCFileInfo(
                path=None,  # type: ignore
                filename="iceh.1850-01.nc",
                file_id="iceh_XXXX_XX",
                filename_timestamp="1850-01",
                frequency="1mon",
                start_date="1850-01-01, 00:00:00",
                end_date="1850-02-01, 00:00:00",
                variable=["TLAT", "TLON", "aice", "tarea", "time", "time_bounds"],
                variable_long_name=[
                    "T grid center latitude",
                    "T grid center longitude",
                    "ice area  (aggregate)",
                    "area of T grid cells",
                    "model time",
                    "boundaries for time-averaging interval",
                ],
                variable_standard_name=["", "", "", "", "", ""],
                variable_cell_methods=["", "", "time: mean", "", "", ""],
                variable_units=[
                    "degrees_north",
                    "degrees_east",
                    "1",
                    "m^2",
                    "days since 0001-01-01 00:00:00",
                    "days since 0001-01-01 00:00:00",
                ],
            ),
        ),
        (
            builders.AccessEsm15Builder,
            "access-esm1-5/history/ocn/ocean_bgc_ann.nc-18501231",
            _AccessNCFileInfo(
                path=None,  # type: ignore
                filename="ocean_bgc_ann.nc-18501231",
                file_id="ocean_bgc_ann",
                filename_timestamp=None,
                frequency="1yr",
                start_date="1849-12-30, 00:00:00",
                end_date="1850-12-30, 00:00:00",
                variable=[
                    "fgco2_raw",
                    "nv",
                    "time",
                    "time_bounds",
                    "xt_ocean",
                    "yt_ocean",
                ],
                variable_long_name=[
                    "Flux into ocean - DIC, inc. anth.",
                    "vertex number",
                    "time",
                    "time axis boundaries",
                    "tcell longitude",
                    "tcell latitude",
                ],
                variable_standard_name=["", "", "", "", "", ""],
                variable_cell_methods=["time: mean", "", "", "", "", ""],
                variable_units=[
                    "mmol/m^2/s",
                    "none",
                    "days since 0001-01-01 00:00:00",
                    "days",
                    "degrees_E",
                    "degrees_N",
                ],
            ),
        ),
        (
            builders.AccessEsm15Builder,
            "access-esm1-5/history/ocn/ocean_bgc.nc-18501231",
            _AccessNCFileInfo(
                path=None,  # type: ignore
                filename="ocean_bgc.nc-18501231",
                file_id="ocean_bgc",
                filename_timestamp=None,
                frequency="1mon",
                start_date="1849-12-30, 00:00:00",
                end_date="1850-12-30, 00:00:00",
                variable=[
                    "nv",
                    "o2",
                    "st_ocean",
                    "time",
                    "time_bounds",
                    "xt_ocean",
                    "yt_ocean",
                ],
                variable_long_name=[
                    "vertex number",
                    "o2",
                    "tcell zstar depth",
                    "time",
                    "time axis boundaries",
                    "tcell longitude",
                    "tcell latitude",
                ],
                variable_standard_name=["", "", "", "", "", "", ""],
                variable_cell_methods=["", "time: mean", "", "", "", "", ""],
                variable_units=[
                    "none",
                    "mmol/m^3",
                    "meters",
                    "days since 0001-01-01 00:00:00",
                    "days",
                    "degrees_E",
                    "degrees_N",
                ],
            ),
        ),
        (
            builders.AccessOm3Builder,
            "access-om3/output000/GMOM_JRA_WD.mom6.h.native_1900_01.nc",
            _AccessNCFileInfo(
                path=None,  # type: ignore
                filename="GMOM_JRA_WD.mom6.h.native_1900_01.nc",
                file_id="GMOM_JRA_WD_mom6_h_native_XXXX_XX",
                filename_timestamp="1900_01",
                frequency="1mon",
                start_date="1900-01-01, 00:00:00",
                end_date="1900-02-01, 00:00:00",
                variable=[
                    "average_DT",
                    "average_T1",
                    "average_T2",
                    "nv",
                    "thetao",
                    "time",
                    "time_bnds",
                    "xh",
                    "yh",
                    "zl",
                ],
                variable_long_name=[
                    "Length of average period",
                    "Start time for average period",
                    "End time for average period",
                    "vertex number",
                    "Sea Water Potential Temperature",
                    "time",
                    "time axis boundaries",
                    "h point nominal longitude",
                    "h point nominal latitude",
                    "Layer pseudo-depth, -z*",
                ],
                variable_standard_name=[
                    "",
                    "",
                    "",
                    "",
                    "sea_water_potential_temperature",
                    "",
                    "",
                    "",
                    "",
                    "",
                ],
                variable_cell_methods=[
                    "",
                    "",
                    "",
                    "",
                    "area:mean zl:mean yh:mean xh:mean time: mean",
                    "",
                    "",
                    "",
                    "",
                    "",
                ],
                variable_units=[
                    "days",
                    "days since 0001-01-01 00:00:00",
                    "days since 0001-01-01 00:00:00",
                    "",
                    "degC",
                    "days since 0001-01-01 00:00:00",
                    "days since 0001-01-01 00:00:00",
                    "degrees_east",
                    "degrees_north",
                    "meter",
                ],
            ),
        ),
        (
            builders.AccessOm3Builder,
            "access-om3/output000/GMOM_JRA_WD.mom6.h.sfc_1900_01_02.nc",
            _AccessNCFileInfo(
                path=None,  # type: ignore
                filename="GMOM_JRA_WD.mom6.h.sfc_1900_01_02.nc",
                file_id="GMOM_JRA_WD_mom6_h_sfc_XXXX_XX_XX",
                filename_timestamp="1900_01_02",
                frequency="1day",
                start_date="1900-01-01, 00:00:00",
                end_date="1900-01-02, 00:00:00",
                variable=[
                    "average_DT",
                    "average_T1",
                    "average_T2",
                    "nv",
                    "time",
                    "time_bnds",
                    "tos",
                    "xh",
                    "yh",
                ],
                variable_long_name=[
                    "Length of average period",
                    "Start time for average period",
                    "End time for average period",
                    "vertex number",
                    "time",
                    "time axis boundaries",
                    "Sea Surface Temperature",
                    "h point nominal longitude",
                    "h point nominal latitude",
                ],
                variable_standard_name=[
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "sea_surface_temperature",
                    "",
                    "",
                ],
                variable_cell_methods=[
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "area:mean yh:mean xh:mean time: mean",
                    "",
                    "",
                ],
                variable_units=[
                    "days",
                    "days since 0001-01-01 00:00:00",
                    "days since 0001-01-01 00:00:00",
                    "",
                    "days since 0001-01-01 00:00:00",
                    "days since 0001-01-01 00:00:00",
                    "degC",
                    "degrees_east",
                    "degrees_north",
                ],
            ),
        ),
        (
            builders.AccessOm3Builder,
            "access-om3/output000/GMOM_JRA_WD.mom6.h.static.nc",
            _AccessNCFileInfo(
                path=None,  # type: ignore
                filename="GMOM_JRA_WD.mom6.h.static.nc",
                file_id="GMOM_JRA_WD_mom6_h_static",
                filename_timestamp=None,
                frequency="fx",
                start_date="none",
                end_date="none",
                variable=["geolat", "geolon", "xh", "yh"],
                variable_long_name=[
                    "Latitude of tracer (T) points",
                    "Longitude of tracer (T) points",
                    "h point nominal longitude",
                    "h point nominal latitude",
                ],
                variable_standard_name=["", "", "", ""],
                variable_cell_methods=["time: point", "time: point", "", ""],
                variable_units=[
                    "degrees_north",
                    "degrees_east",
                    "degrees_east",
                    "degrees_north",
                ],
            ),
        ),
        (
            builders.AccessOm3Builder,
            "access-om3/output000/GMOM_JRA_WD.mom6.h.z_1900_01.nc",
            _AccessNCFileInfo(
                path=None,  # type: ignore
                filename="GMOM_JRA_WD.mom6.h.z_1900_01.nc",
                file_id="GMOM_JRA_WD_mom6_h_z_XXXX_XX",
                filename_timestamp="1900_01",
                frequency="1mon",
                start_date="1900-01-01, 00:00:00",
                end_date="1900-02-01, 00:00:00",
                variable=[
                    "average_DT",
                    "average_T1",
                    "average_T2",
                    "nv",
                    "thetao",
                    "time",
                    "time_bnds",
                    "xh",
                    "yh",
                    "z_l",
                ],
                variable_long_name=[
                    "Length of average period",
                    "Start time for average period",
                    "End time for average period",
                    "vertex number",
                    "Sea Water Potential Temperature",
                    "time",
                    "time axis boundaries",
                    "h point nominal longitude",
                    "h point nominal latitude",
                    "Depth at cell center",
                ],
                variable_standard_name=[
                    "",
                    "",
                    "",
                    "",
                    "sea_water_potential_temperature",
                    "",
                    "",
                    "",
                    "",
                    "",
                ],
                variable_cell_methods=[
                    "",
                    "",
                    "",
                    "",
                    "area:mean z_l:mean yh:mean xh:mean time: mean",
                    "",
                    "",
                    "",
                    "",
                    "",
                ],
                variable_units=[
                    "days",
                    "days since 0001-01-01 00:00:00",
                    "days since 0001-01-01 00:00:00",
                    "",
                    "degC",
                    "days since 0001-01-01 00:00:00",
                    "days since 0001-01-01 00:00:00",
                    "degrees_east",
                    "degrees_north",
                    "meters",
                ],
            ),
        ),
        (
            builders.AccessOm3Builder,
            "access-om3/output000/GMOM_JRA_WD.cice.h.1900-01-01.nc",
            _AccessNCFileInfo(
                path=None,  # type: ignore
                filename="GMOM_JRA_WD.cice.h.1900-01-01.nc",
                file_id="GMOM_JRA_WD_cice_h_XXXX_XX_XX",
                filename_timestamp="1900-01-01",
                frequency="1day",
                start_date="1900-01-01, 00:00:00",
                end_date="1900-01-02, 00:00:00",
                variable=["TLAT", "TLON", "aice", "tarea", "time", "time_bounds"],
                variable_long_name=[
                    "T grid center latitude",
                    "T grid center longitude",
                    "ice area  (aggregate)",
                    "area of T grid cells",
                    "time",
                    "time interval endpoints",
                ],
                variable_standard_name=["", "", "", "", "", ""],
                variable_cell_methods=["", "", "time: mean", "", "", ""],
                variable_units=[
                    "degrees_north",
                    "degrees_east",
                    "1",
                    "m^2",
                    "days since 0000-01-01 00:00:00",
                    "days since 0000-01-01 00:00:00",
                ],
            ),
        ),
        (
            builders.AccessOm3Builder,
            "access-om3/output000/GMOM_JRA_WD.ww3.hi.1900-01-02-00000.nc",
            _AccessNCFileInfo(
                path=None,  # type: ignore
                filename="GMOM_JRA_WD.ww3.hi.1900-01-02-00000.nc",
                file_id="GMOM_JRA_WD_ww3_hi_XXXX_XX_XX_XXXXX",
                filename_timestamp="1900-01-02-00000",
                frequency="fx",  # WW3 provides no time bounds
                start_date="1900-01-02, 00:00:00",
                end_date="1900-01-02, 00:00:00",
                variable=["EF", "mapsta"],
                variable_long_name=["1D spectral density", "map status"],
                variable_standard_name=["", ""],
                variable_cell_methods=["", ""],
                variable_units=["m2 s", "unitless"],
            ),
        ),
    ],
)
def test_parse_access_ncfile(test_data, builder, filename, expected, compare_files):
    file = str(test_data / Path(filename))

    # Set the path to the test data directory
    expected.path = file

    assert builder.parse_access_ncfile(file) == expected

    if not compare_files:
        return None

    """
    In the rest of this test, we refer to the dataset loaded using intake-esm
    as ie_ds and the dataset loaded directly with xarray as xr_ds.

    We also need to perform some additional logic that intake-esm does to avoid
    xr.testing.assert_equal from failing due to preprocessing differences.
    """
    xarray_open_kwargs = _get_xarray_open_kwargs("netcdf")

    ie_ds = _open_dataset(
        urlpath=expected.path,
        varname=expected.variable,
        xarray_open_kwargs=xarray_open_kwargs,
        requested_variables=expected.variable,
    ).compute()
    ie_ds.set_coords(set(ie_ds.variables) - set(ie_ds.attrs[OPTIONS["vars_key"]]))

    xr_ds = xr.open_dataset(file, **xarray_open_kwargs)

    scalar_variables = [v for v in xr_ds.data_vars if len(xr_ds[v].dims) == 0]
    xr_ds = xr_ds.set_coords(scalar_variables)

    xr_ds = xr_ds[expected.variable]

    xr.testing.assert_equal(ie_ds, xr_ds)
