# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import pytest

from access_nri_intake.source.utils import parse_access_filename


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
