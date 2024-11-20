import os
import numpy as np
from pathlib import Path

import intake
import pytest

from access_nri_intake.cli import build

from .conftest import here

"""
args=Namespace(
    config_yaml=[
        '/scratch/tm70/ct1163/configs/cmip5.yaml', 
        '/scratch/tm70/ct1163/configs/access-om2.yaml'],
    build_base_path='/scratch/tm70/ct1163/test_cat/',
    catalog_base_path='./',
    catalog_file='metacatalog.csv',
    version='v2024-11-18',
    no_update=False
    )
"""


def print_directory_tree(root, indent=""):
    """
    Pretty print a directory tree - code from chatgpt.
    """
    for item in os.listdir(root):
        path = os.path.join(root, item)
        if os.path.isdir(path):
            print(f"{indent}├── {item}/")
            print_directory_tree(path, indent + "│   ")
        else:
            print(f"{indent}├── {item}")

@pytest.fixture(scope="session")
def current_catalog():
    """
    Return the current catalog as an intake catalog.
    """
    metacat = intake.cat.access_nri
    yield metacat

@pytest.fixture(scope="session")
def metacat(BASE_DIR, v_num):
    # Build our subset of the catalog. This should take ~2 minutes with the PBS
    # flags in build_subset.sh
    print(f"Building the catalog subset & writing to {BASE_DIR}")
    print(f"Version number: {v_num}")
    build(
        [
            f"{here}/configs/cmip5.yaml",
            f"{here}/configs/access-om2.yaml",
            "--build_base_path",
            str(BASE_DIR),
            "--catalog_base_path",
            "./",
            "--catalog_file",
            "metacatalog.csv",
            "--version",
            v_num,
            "--no_update",
        ]
    )
    cat_path = os.path.join(BASE_DIR, v_num, "metacatalog.csv")
    metacat = intake.open_df_catalog(cat_path)
    yield metacat


def test_catalog_subset_exists(BASE_DIR, v_num, metacat):
    assert os.path.exists(os.path.join(BASE_DIR, v_num, "metacatalog.csv"))


def test_open_dataframe_catalog(metacat):
    assert metacat
    print("Catalog opened successfully.")


@pytest.mark.parametrize(
        "name",
        [
            "cmip5_al33",
            "1deg_jra55_ryf9091_gadi",
        ]
)
def test_datastore_found(metacat, name):
    breakpoint()
    assert metacat[name] == metacat.search(name=name).to_source()



@pytest.mark.parametrize(
    "colname, expected",
    [
        ("path", 3700255),
        ("file_type", 2),
        ("project", 3),
        ("institute", 62),
        ("model", 74),
        ("experiment", 94),
        ("frequency", 9),
        ("realm", 9),
        ("table", 20),
        ("ensemble", 240),
        ("version", 610),
        ("variable", 584),
        ("time_range", 31152),
        ("derived_variable", -999),
    ],
)
def test_cmip5_datastore_nunique(metacat, colname, expected):

    cat = metacat["cmip5_al33"]

    if colname != "derived_variable":
        assert len(cat.df[colname].unique()) == expected
    else:
        with pytest.raises(KeyError):
            assert len(cat.df[colname].unique()) == expected


@pytest.mark.parametrize(
    "colname, expected",
    [
        ('filename',9367),
		('file_id',8),
		('path',9677),
		('filename_timestamp',9361),
		('frequency',3),
		('start_date',9361),
		('end_date',9360),
		('variable',15),           
		('variable_long_name',15),
		('variable_standard_name',15),
		('variable_cell_methods',15),
		('variable_units',15),
		('realm',2)
    ]
)
def test_om2_datastore_nunique(metacat, colname, expected):

    cat = metacat["1deg_jra55_ryf9091_gadi"]


    if colname not in ['variable','variable_long_name','variable_standard_name','variable_cell_methods','variable_units']:
        assert len(cat.df[colname].unique()) == expected
    else:
        # These should fail because they contains lists (unhashable)
        with pytest.raises(TypeError):
            assert len(cat.df[colname].unique()) == expected
        # cast to tuple to make them hashable, then check the length
        tuplified = cat.df[colname].apply(lambda x : tuple(x)).unique()
        assert len(tuplified) == expected

@pytest.mark.parametrize(
    "colname, expected",
    [
        ("file_type", {"l", "f"}),
        ("project", {"CMIP5", "CORDEX", "isimip2b"}),
        (
            "institute",
            {
                "BNU",
                "CMCC",
                "IPSL",
                "LASG-CESS",
                "LASG-IAP",
                "MPI-M",
                "MRI",
                "NASA-GISS",
                "NASA-GMAO",
                "NIMR-KMA",
            },
        ),
        (
            "model",
            {
                "BNU-ESM",
                "FGOALS-g2",
                "FGOALS-s2",
                "GISS-E2-H",
                "GISS-E2-H-CC",
                "GISS-E2-R",
                "GISS-E2-R-CC",
                "IPSL-CM5A-LR",
                "IPSL-CM5A-MR",
                "IPSL-CM5B-LR",
            },
        ),
        (
            "realm",
            {
                "atmos",
                "ocean",
                "seaIce",
                "land",
                "aerosol",
                "ocnBgchem",
                "landIce",
                "na",
                "landonly",
            },
        ),
        (
            "experiment",
            {
                "amip",
                "esmFdbk2",
                "historical",
                "midHolocene",
                "piControl",
                "rcp45",
                "rcp85",
                "sstClim",
                "sstClimAerosol",
                "sstClimSulfate",
            },
        ),
        ("frequency", {'3hr', '6hr', 'daily', 'day', 'fx', 'mon', 'monClim', 'subhr', 'yr'}),
        (
            "table",
            {
                "6hrLev",
                "6hrPlev",
                "Amon",
                "Lmon",
                "OImon",
                "Omon",
                "cfDay",
                "cfMon",
                "day",
                "fx",
            },
        ),
        (
            "ensemble",
            {
                "r0i0p0",
                "r11i1p1",
                "r1i1p1",
                "r1i1p2",
                "r2i1p1",
                "r3i1p1",
                "r4i1p1",
                "r5i1p1",
                "r6i1p1",
                "r8i1p1",
            },
        ),
        (
            "version",
            {
                "v1",
                "v2",
                "v20110726",
                "v20111119",
                "v20111219",
                "v20120430",
                "v20120526",
                "v20120804",
                "v20130506",
                "v20161204",
            },
        ),
        (
            "variable",
            {"ccb", "pr", "psl", "tas", "tasmax", "tasmin", "tro3", "ua", "va", "wmo"},
        ),
        (
            "time_range",
            {
                "000101-010012",
                "185001-234912",
                "18520101-18521231",
                "18780101-18781231",
                "19170101-19171231",
                "195001-200512",
                "198201010000-198212311800",
                "19910101-19911231",
                "199201010000-199212311800",
                "20010101-20011231",
            },
        ),
    ],
)
def test_cmip5_metacat_vals_found(metacat, colname, expected):
    # Test that the unique values in the column are as expected. I've truncated
    # the unique values to the first 10 for brevity because I'm not typing out
    # 3700255 unique values.
    cat = metacat["cmip5_al33"]
    found = set(cat.df[colname].unique()[:10])

    assert found == expected

@pytest.mark.parametrize(
    "colname, expected",
    [
        ('filename',{'iceh.1900-07.nc', 'iceh.1900-05.nc', 'iceh.1900-04.nc', 'iceh.1900-03.nc', 'iceh.1900-08.nc', 'iceh.1900-06.nc', 'iceh.1900-02.nc', 'iceh.1900-09.nc', 'iceh.1900-10.nc', 'iceh.1900-01.nc'}),
		('file_id',{'iceh_XXXX_XX', 'ocean_scalar', 'ocean_snap', 'ocean_grid', 'ocean', 'ocean_wmass', 'ocean_heat', 'ocean_month'}),
		('path',{'/g/data/ik11/outputs/access-om2/1deg_jra55_ryf9091_gadi/output000/ice/OUTPUT/iceh.1900-01.nc', '/g/data/ik11/outputs/access-om2/1deg_jra55_ryf9091_gadi/output000/ice/OUTPUT/iceh.1900-07.nc', '/g/data/ik11/outputs/access-om2/1deg_jra55_ryf9091_gadi/output000/ice/OUTPUT/iceh.1900-08.nc', '/g/data/ik11/outputs/access-om2/1deg_jra55_ryf9091_gadi/output000/ice/OUTPUT/iceh.1900-03.nc', '/g/data/ik11/outputs/access-om2/1deg_jra55_ryf9091_gadi/output000/ice/OUTPUT/iceh.1900-06.nc', '/g/data/ik11/outputs/access-om2/1deg_jra55_ryf9091_gadi/output000/ice/OUTPUT/iceh.1900-10.nc', '/g/data/ik11/outputs/access-om2/1deg_jra55_ryf9091_gadi/output000/ice/OUTPUT/iceh.1900-05.nc', '/g/data/ik11/outputs/access-om2/1deg_jra55_ryf9091_gadi/output000/ice/OUTPUT/iceh.1900-04.nc', '/g/data/ik11/outputs/access-om2/1deg_jra55_ryf9091_gadi/output000/ice/OUTPUT/iceh.1900-09.nc', '/g/data/ik11/outputs/access-om2/1deg_jra55_ryf9091_gadi/output000/ice/OUTPUT/iceh.1900-02.nc'}),
		('filename_timestamp',{'1900-04', '1900-05', '1900-06', '1900-01', '1900-08', '1900-09', '1900-07', '1900-03', '1900-10', '1900-02'}),
		('frequency',{'fx', '1yr', '1mon'}),
		('start_date',{'1900-02-01, 00:00:00', '1900-03-01, 00:00:00', '1900-06-01, 00:00:00', '1900-10-01, 00:00:00', '1900-07-01, 00:00:00', '1900-09-01, 00:00:00', '1900-05-01, 00:00:00', '1900-01-01, 00:00:00', '1900-04-01, 00:00:00', '1900-08-01, 00:00:00'}),
		('end_date',{'1900-02-01, 00:00:00', '1900-03-01, 00:00:00', '1900-06-01, 00:00:00', '1900-10-01, 00:00:00', '1900-07-01, 00:00:00', '1900-09-01, 00:00:00', '1900-11-01, 00:00:00', '1900-05-01, 00:00:00', '1900-04-01, 00:00:00', '1900-08-01, 00:00:00'}),
		('variable',{('scalar_axis', 'time', 'nv', 'ke_tot', 'pe_tot', 'temp_global_ave', 'salt_global_ave', 'rhoave', 'temp_surface_ave', 'salt_surface_ave', 'total_ocean_salt', 'total_ocean_heat', 'eta_global', 'total_ocean_sfc_salt_flux_coupler', 'total_ocean_pme_river', 'total_ocean_river', 'total_ocean_runoff', 'total_ocean_calving', 'total_ocean_melt', 'total_ocean_evap', 'total_ocean_lprec', 'total_ocean_fprec', 'total_ocean_runoff_heat', 'total_ocean_calving_heat', 'total_ocean_river_heat', 'total_ocean_hflux_prec', 'total_ocean_hflux_evap', 'total_ocean_hflux_coupler', 'total_ocean_swflx', 'total_ocean_swflx_vis', 'total_ocean_lw_heat', 'total_ocean_evap_heat', 'total_ocean_fprec_melt_heat', 'total_ocean_calving_melt_heat', 'total_ocean_sens_heat', 'average_T1', 'average_T2', 'average_DT', 'time_bounds'), ('xt_ocean', 'yt_ocean', 'st_ocean', 'st_edges_ocean', 'time', 'nv', 'xu_ocean', 'yu_ocean', 'sw_ocean', 'sw_edges_ocean', 'temp', 'salt', 'age_global', 'u', 'v', 'wt', 'dzt', 'pot_rho_0', 'tx_trans', 'ty_trans', 'tx_trans_gm', 'ty_trans_gm', 'average_T1', 'average_T2', 'average_DT', 'time_bounds'), ('grid_xu_ocean', 'grid_yt_ocean', 'neutral', 'neutralrho_edges', 'time', 'nv', 'grid_xt_ocean', 'grid_yu_ocean', 'tx_trans_nrho', 'ty_trans_nrho', 'tx_trans_nrho_gm', 'ty_trans_nrho_gm', 'tx_trans_nrho_submeso', 'ty_trans_nrho_submeso', 'mass_pmepr_on_nrho', 'average_T1', 'average_T2', 'average_DT', 'time_bounds'), ('xt_ocean', 'yt_ocean', 'st_ocean', 'st_edges_ocean', 'time', 'nv', 'xu_ocean', 'yu_ocean', 'sw_ocean', 'sw_edges_ocean', 'temp', 'salt', 'age_global', 'u', 'v', 'wt', 'dzt', 'pot_rho_0', 'tx_trans', 'ty_trans', 'tx_trans_gm', 'ty_trans_gm', 'tx_trans_submeso', 'ty_trans_submeso', 'temp_xflux_adv', 'temp_yflux_adv', 'temp_xflux_gm', 'temp_yflux_gm', 'temp_xflux_submeso', 'temp_yflux_submeso', 'temp_xflux_ndiffuse', 'temp_yflux_ndiffuse', 'diff_cbt_t', 'average_T1', 'average_T2', 'average_DT', 'time_bounds'), ('xt_ocean', 'yt_ocean', 'time', 'nv', 'xu_ocean', 'yu_ocean', 'sea_level', 'eta_t', 'sea_levelsq', 'mld', 'pme_river', 'river', 'runoff', 'ice_calving', 'evap', 'melt', 'sfc_salt_flux_restore', 'sfc_salt_flux_ice', 'sfc_salt_flux_coupler', 'net_sfc_heating', 'frazil_3d_int_z', 'tau_x', 'tau_y', 'bmf_u', 'bmf_v', 'tx_trans_int_z', 'ty_trans_int_z', 'pbot_t', 'average_T1', 'average_T2', 'average_DT', 'time_bounds'), ('grid_xu_ocean', 'grid_yt_ocean', 'neutral', 'neutralrho_edges', 'time', 'nv', 'grid_xt_ocean', 'grid_yu_ocean', 'tx_trans_nrho', 'ty_trans_nrho', 'tx_trans_nrho_gm', 'ty_trans_nrho_gm', 'tx_trans_nrho_submeso', 'ty_trans_nrho_submeso', 'temp_xflux_adv_on_nrho', 'temp_yflux_adv_on_nrho', 'temp_xflux_submeso_on_nrho', 'temp_yflux_submeso_on_nrho', 'temp_xflux_gm_on_nrho', 'temp_yflux_gm_on_nrho', 'temp_xflux_ndiffuse_on_nrho', 'temp_yflux_ndiffuse_on_nrho', 'mass_pmepr_on_nrho', 'average_T1', 'average_T2', 'average_DT', 'time_bounds'), ('xt_ocean', 'yt_ocean', 'st_ocean', 'st_edges_ocean', 'time', 'nv', 'xu_ocean', 'yu_ocean', 'temp', 'salt', 'age_global', 'u', 'v', 'average_T1', 'average_T2', 'average_DT', 'time_bounds'), ('xt_ocean', 'yt_ocean', 'time', 'xu_ocean', 'yu_ocean', 'geolon_t', 'geolat_t', 'geolon_c', 'geolat_c', 'ht', 'hu', 'dxt', 'dyt', 'dxu', 'dyu', 'area_t', 'area_u', 'kmt', 'kmu', 'drag_coeff'), ('time', 'time_bounds', 'TLON', 'TLAT', 'ULON', 'ULAT', 'NCAT', 'tmask', 'blkmask', 'tarea', 'uarea', 'dxt', 'dyt', 'dxu', 'dyu', 'HTN', 'HTE', 'ANGLE', 'ANGLET', 'hi_m', 'hs_m', 'Tsfc_m', 'aice_m', 'uvel_m', 'vvel_m', 'uatm_m', 'vatm_m', 'sice_m', 'fswdn_m', 'fswup_m', 'flwdn_m', 'snow_ai_m', 'rain_ai_m', 'sst_m', 'sss_m', 'uocn_m', 'vocn_m', 'frzmlt_m', 'fswfac_m', 'fswabs_ai_m', 'albsni_m', 'alvdr_ai_m', 'alidr_ai_m', 'alvdf_ai_m', 'alidf_ai_m', 'albice_m', 'albsno_m', 'flat_ai_m', 'fsens_ai_m', 'flwup_ai_m', 'evap_ai_m', 'Tair_m', 'congel_m', 'frazil_m', 'snoice_m', 'meltt_m', 'melts_m', 'meltb_m', 'meltl_m', 'fresh_ai_m', 'fsalt_ai_m', 'fhocn_ai_m', 'fswthru_ai_m', 'strairx_m', 'strairy_m', 'strtltx_m', 'strtlty_m', 'strcorx_m', 'strcory_m', 'strocnx_m', 'strocny_m', 'strintx_m', 'strinty_m', 'strength_m', 'divu_m', 'shear_m', 'dvidtt_m', 'dvidtd_m', 'daidtt_m', 'daidtd_m', 'mlt_onset_m', 'frz_onset_m', 'trsig_m', 'ice_present_m', 'fcondtop_ai_m', 'aicen_m', 'vicen_m', 'fsurfn_ai_m', 'fcondtopn_ai_m', 'fmelttn_ai_m', 'flatn_ai_m'), ('xt_ocean', 'yt_ocean', 'st_ocean', 'st_edges_ocean', 'time', 'nv', 'xu_ocean', 'yu_ocean', 'sw_ocean', 'sw_edges_ocean', 'grid_xt_ocean', 'grid_yu_ocean', 'potrho', 'potrho_edges', 'temp', 'salt', 'age_global', 'u', 'v', 'wt', 'pot_rho_0', 'ty_trans_rho', 'ty_trans_rho_gm', 'average_T1', 'average_T2', 'average_DT', 'time_bounds')}),           
		('variable_long_name',{('tcell longitude', 'tcell latitude', 'time', 'vertex number', 'ucell longitude', 'ucell latitude', 'effective sea level (eta_t + patm/(rho0*g)) on T cells', 'surface height on T cells [Boussinesq (volume conserving) model]', 'square of effective sea level (eta_t + patm/(rho0*g)) on T cells', 'mixed layer depth determined by density criteria', 'mass flux of precip-evap+river via sbc (liquid, frozen, evaporation)', 'mass flux of river (runoff + calving) entering ocean', 'mass flux of liquid river runoff entering ocean', 'mass flux of land ice calving into ocean', 'mass flux from evaporation/condensation (>0 enters ocean)', 'water flux transferred with sea ice form/melt (>0 enters ocean)', 'sfc_salt_flux_restore: flux from restoring term', 'sfc_salt_flux_ice', 'sfc_salt_flux_coupler: flux from the coupler', 'surface ocean heat flux coming through coupler and mass transfer', 'Vertical sum of ocn frazil heat flux over time step', 'i-directed wind stress forcing u-velocity', 'j-directed wind stress forcing v-velocity', 'Bottom u-stress via bottom drag', 'Bottom v-stress via bottom drag', 'T-cell i-mass transport vertically summed', 'T-cell j-mass transport vertically summed', 'bottom pressure on T cells [Boussinesq (volume conserving) model]', 'Start time for average period', 'End time for average period', 'Length of average period', 'time axis boundaries'), ('none', 'time', 'vertex number', 'Globally integrated ocean kinetic energy', 'Globally integrated ocean potential energy', 'Global mean temp in liquid seawater', 'Global mean salt in liquid seawater', 'global mean ocean in-situ density from ocean_density_mod', 'Global mass weighted mean surface temp in liquid seawater', 'Global mass weighted mean surface salt in liquid seawater', 'total mass of salt in liquid seawater', 'Total heat in the liquid ocean referenced to 0degC', 'global ave eta_t plus patm_t/(g*rho0)', 'total_ocean_sfc_salt_flux_coupler', 'total ocean precip-evap+river via sbc (liquid, frozen, evaporation)', 'total liquid river water and calving ice entering ocean', 'total liquid river runoff (>0 water enters ocean)', 'total water entering ocean from calving land ice', 'total liquid water melted from sea ice (>0 enters ocean)', 'total evaporative ocean mass flux (>0 enters ocean)', 'total liquid precip into ocean (>0 enters ocean)', 'total snow falling onto ocean (>0 enters ocean)', 'total ocean heat flux from liquid river runoff', 'total ocean heat flux from calving land ice', 'total heat flux into ocean from liquid+solid runoff (<0 cools ocean)', 'total ocean heat flux from precip transferring water across surface', 'total ocean heat flux from evap transferring water across surface', 'total surface heat flux passed through coupler', 'total shortwave flux into ocean (>0 heats ocean)', 'total visible shortwave into ocean (>0 heats ocean)', 'total longwave flux into ocean (<0 cools ocean)', 'total latent heat flux into ocean (<0 cools ocean)', 'total heat flux to melt frozen precip (<0 cools ocean)', 'total heat flux to melt frozen land ice (<0 cools ocean)', 'total sensible heat into ocean (<0 cools ocean)', 'Start time for average period', 'End time for average period', 'Length of average period', 'time axis boundaries'), ('tcell longitude', 'tcell latitude', 'time', 'ucell longitude', 'ucell latitude', 'tracer longitude', 'tracer latitude', 'uv longitude', 'uv latitude', 'ocean depth on t-cells', 'ocean depth on u-cells', 'ocean dxt on t-cells', 'ocean dyt on t-cells', 'ocean dxu on u-cells', 'ocean dyu on u-cells', 'tracer cell area', 'velocity cell area', 'number of depth levels on t-grid', 'number of depth levels on u-grid', 'Dimensionless bottom drag coefficient'), ('tcell longitude', 'tcell latitude', 'tcell zstar depth', 'tcell zstar depth edges', 'time', 'vertex number', 'ucell longitude', 'ucell latitude', 'Conservative temperature', 'Practical Salinity', 'Age (global)', 'i-current', 'j-current', 'Start time for average period', 'End time for average period', 'Length of average period', 'time axis boundaries'), ('tcell longitude', 'tcell latitude', 'tcell zstar depth', 'tcell zstar depth edges', 'time', 'vertex number', 'ucell longitude', 'ucell latitude', 'ucell zstar depth', 'ucell zstar depth edges', 'Conservative temperature', 'Practical Salinity', 'Age (global)', 'i-current', 'j-current', 'dia-surface velocity T-points', 't-cell thickness', 'potential density referenced to 0 dbar', 'T-cell i-mass transport', 'T-cell j-mass transport', 'T-cell mass i-transport from GM', 'T-cell mass j-transport from GM', 'T-cell mass i-transport from submesoscale param', 'T-cell mass j-transport from submesoscale param', 'cp*rho*dzt*dyt*u*temp', 'cp*rho*dzt*dxt*v*temp', 'cp*gm_xflux*dyt*rho_dzt*temp', 'cp*gm_yflux*dxt*rho_dzt*temp', 'cp*submeso_xflux*dyt*rho_dzt*temp', 'cp*submeso_yflux*dxt*rho_dzt*temp', 'cp*ndiffuse_xflux*dyt*rho_dzt*temp', 'cp*ndiffuse_yflux*dxt*rho_dzt*temp', 'total vert diff_cbt(temp) (w/o neutral included)', 'Start time for average period', 'End time for average period', 'Length of average period', 'time axis boundaries'), ('tcell longitude', 'tcell latitude', 'tcell zstar depth', 'tcell zstar depth edges', 'time', 'vertex number', 'ucell longitude', 'ucell latitude', 'ucell zstar depth', 'ucell zstar depth edges', 'Conservative temperature', 'Practical Salinity', 'Age (global)', 'i-current', 'j-current', 'dia-surface velocity T-points', 't-cell thickness', 'potential density referenced to 0 dbar', 'T-cell i-mass transport', 'T-cell j-mass transport', 'T-cell mass i-transport from GM', 'T-cell mass j-transport from GM', 'Start time for average period', 'End time for average period', 'Length of average period', 'time axis boundaries'), ('ucell longitude', 'tcell latitude', 'neutral density', 'neutral density edges', 'time', 'vertex number', 'tcell longitude', 'ucell latitude', 'T-cell i-mass transport on neutral rho', 'T-cell j-mass transport on neutral rho', 'T-cell i-mass transport from GM on neutral rho', 'T-cell j-mass transport from GM on neutral rho', 'T-cell i-mass transport from submesoscale param on neutral rho', 'T-cell j-mass transport from submesoscale param on neutral rho', 'mass transport from liquid+frozen mass and seaice melt+form (>0 enters ocean) binned to neutral density classes', 'Start time for average period', 'End time for average period', 'Length of average period', 'time axis boundaries'), ('tcell longitude', 'tcell latitude', 'tcell zstar depth', 'tcell zstar depth edges', 'time', 'vertex number', 'ucell longitude', 'ucell latitude', 'ucell zstar depth', 'ucell zstar depth edges', 'tcell longitude', 'ucell latitude', 'potential density', 'potential density edges', 'Conservative temperature', 'Practical Salinity', 'Age (global)', 'i-current', 'j-current', 'dia-surface velocity T-points', 'potential density referenced to 0 dbar', 'T-cell j-mass transport on pot_rho', 'T-cell j-mass transport from GM on pot_rho', 'Start time for average period', 'End time for average period', 'Length of average period', 'time axis boundaries'), ('model time', 'boundaries for time-averaging interval', 'T grid center longitude', 'T grid center latitude', 'U grid center longitude', 'U grid center latitude', 'category maximum thickness', 'ocean grid mask', 'ice grid block mask', 'area of T grid cells', 'area of U grid cells', 'T cell width through middle', 'T cell height through middle', 'U cell width through middle', 'U cell height through middle', 'T cell width on North side', 'T cell width on East side', 'angle grid makes with latitude line on U grid', 'angle grid makes with latitude line on T grid', 'grid cell mean ice thickness', 'grid cell mean snow thickness', 'snow/ice surface temperature', 'ice area  (aggregate)', 'ice velocity (x)', 'ice velocity (y)', 'atm velocity (x)', 'atm velocity (y)', 'bulk ice salinity', 'down solar flux', 'upward solar flux', 'down longwave flux', 'snowfall rate', 'rainfall rate', 'sea surface temperature', 'sea surface salinity', 'ocean current (x)', 'ocean current (y)', 'freeze/melt potential', 'shortwave scaling factor', 'snow/ice/ocn absorbed solar flux', 'snow/ice broad band albedo', 'visible direct albedo', 'near IR direct albedo', 'visible diffuse albedo', 'near IR diffuse albedo', 'bare ice albedo', 'snow albedo', 'latent heat flux', 'sensible heat flux', 'upward longwave flux', 'evaporative water flux', 'air temperature', 'congelation ice growth', 'frazil ice growth', 'snow-ice formation', 'top ice melt', 'top snow melt', 'basal ice melt', 'lateral ice melt', 'freshwtr flx ice to ocn', 'salt flux ice to ocean', 'heat flux ice to ocean', 'SW flux thru ice to ocean', 'atm/ice stress (x)', 'atm/ice stress (y)', 'sea sfc tilt stress (x)', 'sea sfc tilt stress (y)', 'coriolis stress (x)', 'coriolis stress (y)', 'ocean/ice stress (x)', 'ocean/ice stress (y)', 'internal ice stress (x)', 'internal ice stress (y)', 'compressive ice strength', 'strain rate (divergence)', 'strain rate (shear)', 'volume tendency thermo', 'volume tendency dynamics', 'area tendency thermo', 'area tendency dynamics', 'melt onset date', 'freeze onset date', 'internal stress tensor trace', 'fraction of time-avg interval that ice is present', 'top surface conductive heat flux', 'ice area, categories', 'ice volume, categories', 'net surface heat flux, categories', 'top sfc conductive heat flux, cat', 'net sfc heat flux causing melt, cat', 'latent heat flux, category'), ('ucell longitude', 'tcell latitude', 'neutral density', 'neutral density edges', 'time', 'vertex number', 'tcell longitude', 'ucell latitude', 'T-cell i-mass transport on neutral rho', 'T-cell j-mass transport on neutral rho', 'T-cell i-mass transport from GM on neutral rho', 'T-cell j-mass transport from GM on neutral rho', 'T-cell i-mass transport from submesoscale param on neutral rho', 'T-cell j-mass transport from submesoscale param on neutral rho', 'cp*rho*dzt*dyt*u*temp binned to neutral density', 'cp*rho*dzt*dxt*v*temp binned to neutral density', 'cp*submeso_xflux*dyt*rho_dzt*temp binned to neutral density', 'cp*submeso_yflux*dxt*rho_dzt*temp binned to neutral density', 'cp*gm_xflux*dyt*rho_dzt*temp binned to neutral density', 'cp*gm_yflux*dxt*rho_dzt*temp binned to neutral density', 'cp*ndiffuse_xflux*dyt*rho_dzt*temp binned to neutral density', 'cp*ndiffuse_yflux*dxt*rho_dzt*temp binned to neutral density', 'mass transport from liquid+frozen mass and seaice melt+form (>0 enters ocean) binned to neutral density classes', 'Start time for average period', 'End time for average period', 'Length of average period', 'time axis boundaries')}),
		('variable_standard_name',{('', '', '', '', '', '', '', '', '', 'sea_floor_depth_below_geoid', '', '', '', '', '', '', '', '', '', ''), ('', '', '', '', '', 'sea_water_potential_temperature', 'sea_water_salinity', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''), ('', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''), ('', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''), ('', '', '', '', '', '', '', '', '', '', 'sea_water_conservative_temperature', 'sea_water_salinity', 'sea_water_age_since_surface_contact', 'sea_water_x_velocity', 'sea_water_y_velocity', '', 'cell_thickness', 'sea_water_potential_density', 'ocean_mass_x_transport', 'ocean_mass_y_transport', '', '', '', '', '', '', '', '', '', '', '', '', 'ocean_vertical_heat_diffusivity', '', '', '', ''), ('', '', '', '', '', '', 'sea_surface_height_above_geoid', '', 'square_of_sea_surface_height_above_geoid', 'ocean_mixed_layer_thickness_defined_by_sigma_t', 'water_flux_into_sea_water', '', 'water_flux_into_sea_water_from_rivers', 'water_flux_into_sea_water_from_icebergs', 'water_evaporation_flux', 'water_flux_into_sea_water_due_to_sea_ice_thermodynamics', '', 'downward_sea_ice_basal_salt_flux', '', '', '', 'surface_downward_x_stress', 'surface_downward_y_stress', '', '', '', '', 'sea_water_pressure_at_sea_floor', '', '', '', ''), ('', '', '', '', '', '', '', '', '', '', '', '', '', '', 'sea_water_conservative_temperature', 'sea_water_salinity', 'sea_water_age_since_surface_contact', 'sea_water_x_velocity', 'sea_water_y_velocity', '', 'sea_water_potential_density', '', '', '', '', '', ''), ('', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''), ('', '', '', '', '', '', '', '', 'sea_water_conservative_temperature', 'sea_water_salinity', 'sea_water_age_since_surface_contact', 'sea_water_x_velocity', 'sea_water_y_velocity', '', '', '', ''), ('', '', '', '', '', '', '', '', '', '', 'sea_water_conservative_temperature', 'sea_water_salinity', 'sea_water_age_since_surface_contact', 'sea_water_x_velocity', 'sea_water_y_velocity', '', 'cell_thickness', 'sea_water_potential_density', 'ocean_mass_x_transport', 'ocean_mass_y_transport', '', '', '', '', '', '')}),
		('variable_cell_methods',{('', '', '', '', '', 'time: point', 'time: point', 'time: point', 'time: point', 'time: point', 'time: point', 'time: point', 'time: point', 'time: point', 'time: point', 'time: point', 'time: point', 'time: point', 'time: point', 'time: point'), ('', '', '', '', '', '', '', '', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', '', '', '', ''), ('', '', '', '', '', '', '', '', '', '', '', '', '', '', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', '', '', '', ''), ('', '', '', '', '', '', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', '', '', '', ''), ('', '', '', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', '', '', '', ''), ('', '', '', '', '', '', '', '', '', '', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', '', '', '', ''), ('', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean'), ('', '', '', '', '', '', '', '', '', '', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', '', '', '', ''), ('', '', '', '', '', '', '', '', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', '', '', '', ''), ('', '', '', '', '', '', '', '', 'time: mean', 'time: mean', 'time: mean', 'time: mean', 'time: mean', '', '', '', '')}),
		('variable_units',{('degrees_E', 'degrees_N', 'days since 1900-01-01 00:00:00', 'degrees_E', 'degrees_N', 'degrees_E', 'degrees_N', 'degrees_E', 'degrees_N', 'm', 'm', 'm', 'm', 'm', 'm', 'm^2', 'm^2', 'dimensionless', 'dimensionless', 'dimensionless'), ('none', 'days since 1900-01-01 00:00:00', 'none', '10^15 Joules', '10^15 Joules', 'deg_C', 'psu', 'kg/m^3', 'deg_C', 'psu', 'kg/1e18', 'Joule/1e25', 'meter', 'kg/sec (*1e-15)', '(kg/sec)/1e15', 'kg/sec/1e15', '(kg/sec)/1e15', '(kg/sec)/1e15', '(kg/sec)/1e15', '(kg/sec)/1e15', '(kg/sec)/1e15', '(kg/sec)/1e15', 'Watts/1e15', 'Watts/1e15', 'Watts/1e15', 'Watts/1e15', 'Watts/1e15', 'Watts/1e15', 'Watts/1e15', 'Watts/1e15', 'Watts/1e15', 'Watts/1e15', 'Watts/1e15', 'Watts/1e15', 'Watts/1e15', 'days since 1900-01-01 00:00:00', 'days since 1900-01-01 00:00:00', 'days', 'days'), ('degrees_E', 'degrees_N', 'meters', 'meters', 'days since 1900-01-01 00:00:00', 'none', 'degrees_E', 'degrees_N', 'meters', 'meters', 'K', 'psu', 'yr', 'm/sec', 'm/sec', 'm/sec', 'm', 'kg/m^3', 'kg/s', 'kg/s', 'kg/s', 'kg/s', 'kg/s', 'kg/s', 'Watts', 'Watts', 'Watt', 'Watt', 'Watt', 'Watt', 'Watt', 'Watt', 'm^2/s', 'days since 1900-01-01 00:00:00', 'days since 1900-01-01 00:00:00', 'days', 'days'), ('degrees_E', 'degrees_N', 'kg/m^3', 'kg/m^3', 'days since 1900-01-01 00:00:00', 'none', 'degrees_E', 'degrees_N', 'kg/s', 'kg/s', 'kg/s', 'kg/s', 'kg/s', 'kg/s', 'Watts', 'Watts', 'Watt', 'Watt', 'Watt', 'Watt', 'Watt', 'Watt', 'kg/sec', 'days since 1900-01-01 00:00:00', 'days since 1900-01-01 00:00:00', 'days', 'days'), ('degrees_E', 'degrees_N', 'meters', 'meters', 'days since 1900-01-01 00:00:00', 'none', 'degrees_E', 'degrees_N', 'meters', 'meters', 'K', 'psu', 'yr', 'm/sec', 'm/sec', 'm/sec', 'm', 'kg/m^3', 'kg/s', 'kg/s', 'kg/s', 'kg/s', 'days since 1900-01-01 00:00:00', 'days since 1900-01-01 00:00:00', 'days', 'days'), ('degrees_E', 'degrees_N', 'days since 1900-01-01 00:00:00', 'none', 'degrees_E', 'degrees_N', 'meter', 'meter', 'm^2', 'm', '(kg/m^3)*(m/sec)', '(kg/m^3)*(m/sec)', '(kg/m^3)*(m/sec)', '(kg/m^3)*(m/sec)', '(kg/m^3)*(m/sec)', '(kg/m^3)*(m/sec)', 'kg/(m^2*sec)', 'kg/(m^2*sec)', 'kg/(m^2*sec)', 'Watts/m^2', 'W/m^2', 'N/m^2', 'N/m^2', 'N/m^2', 'N/m^2', 'kg/s', 'kg/s', 'dbar', 'days since 1900-01-01 00:00:00', 'days since 1900-01-01 00:00:00', 'days', 'days'), ('degrees_E', 'degrees_N', 'meters', 'meters', 'days since 1900-01-01 00:00:00', 'none', 'degrees_E', 'degrees_N', 'meters', 'meters', 'degrees_E', 'degrees_N', 'kg/m^3', 'kg/m^3', 'K', 'psu', 'yr', 'm/sec', 'm/sec', 'm/sec', 'kg/m^3', 'kg/s', 'kg/s', 'days since 1900-01-01 00:00:00', 'days since 1900-01-01 00:00:00', 'days', 'days'), ('degrees_E', 'degrees_N', 'kg/m^3', 'kg/m^3', 'days since 1900-01-01 00:00:00', 'none', 'degrees_E', 'degrees_N', 'kg/s', 'kg/s', 'kg/s', 'kg/s', 'kg/s', 'kg/s', 'kg/sec', 'days since 1900-01-01 00:00:00', 'days since 1900-01-01 00:00:00', 'days', 'days'), ('days since 1900-01-01 00:00:00', 'days since 1900-01-01 00:00:00', 'degrees_east', 'degrees_north', 'degrees_east', 'degrees_north', 'm', '', '', 'm^2', 'm^2', 'm', 'm', 'm', 'm', 'm', 'm', 'radians', 'radians', 'm', 'm', 'C', '1', 'm/s', 'm/s', 'm/s', 'm/s', 'ppt', 'W/m^2', 'W/m^2', 'W/m^2', 'cm/day', 'cm/day', 'C', 'ppt', 'm/s', 'm/s', 'W/m^2', '1', 'W/m^2', '%', '%', '%', '%', '%', '%', '%', 'W/m^2', 'W/m^2', 'W/m^2', 'cm/day', 'C', 'cm/day', 'cm/day', 'cm/day', 'cm/day', 'cm/day', 'cm/day', 'cm/day', 'cm/day', 'kg/m^2/s', 'W/m^2', 'W/m^2', 'N/m^2', 'N/m^2', 'N/m^2', 'N/m^2', 'N/m^2', 'N/m^2', 'N/m^2', 'N/m^2', 'N/m^2', 'N/m^2', 'N/m', '%/day', '%/day', 'cm/day', 'cm/day', '%/day', '%/day', 'day of year', 'day of year', 'N/m^2', '1', 'W/m^2', '1', 'm', 'W/m^2', 'W/m^2', 'W/m^2', 'W/m^2'), ('degrees_E', 'degrees_N', 'meters', 'meters', 'days since 1900-01-01 00:00:00', 'none', 'degrees_E', 'degrees_N', 'K', 'psu', 'yr', 'm/sec', 'm/sec', 'days since 1900-01-01 00:00:00', 'days since 1900-01-01 00:00:00', 'days', 'days')}),
		('realm',{'ocean', 'seaIce'} )
    ],
)
def test_om2_metacat_vals_found(metacat, colname, expected, current_catalog):
    # Test that the unique values in the column are as expected. I've truncated
    # the unique values to the first 10 for brevity because I'm not typing out
    # 3700255 unique values.
    breakpoint()
    cat = metacat["1deg_jra55_ryf9091_gadi"]
    if colname not in ['variable','variable_long_name','variable_standard_name','variable_cell_methods','variable_units']:
        found = set(cat.df[colname].unique()[:10])
        assert found == expected
    else:
        # These should fail because they contains lists (unhashable)
        with pytest.raises(TypeError):
            _found = set(cat.df[colname].unique()[:10])
        # cast to tuple to make them hashable, then check the length
        found = set(cat.df[colname].apply(lambda x : tuple(x)).unique()[:10])
        assert found == expected

    # Repeat the test with the current catalog
    cat = current_catalog["1deg_jra55_ryf9091_gadi"]
    if colname not in ['variable','variable_long_name','variable_standard_name','variable_cell_methods','variable_units']:
        found = set(cat.df[colname].unique()[:10])
        assert found >= expected
    else:
        # These should fail because they contains lists (unhashable)
        with pytest.raises(TypeError):
            _found = set(cat.df[colname].unique()[:10])
        # cast to tuple to make them hashable, then check the length
        found = set(cat.df[colname].apply(lambda x : tuple(x)).unique()[:10])
        assert found >= expected



@pytest.mark.parametrize(
    "path, varname, first_ten_mean",
    [
        ('/g/data/al33/replicas/CMIP5/combined/LASG-IAP/FGOALS-s2/amip/6hr/atmos/6hrLev/r1i1p1/v1/va/va_6hrLev_FGOALS-s2_amip_r1i1p1_198201010000-198212311800.nc', 
        'va',
        -6.1719556
        ),
        ('/g/data/al33/replicas/CMIP5/combined/CMCC/CMCC-CMS/rcp45/day/seaIce/day/r1i1p1/v20120717/sit/sit_day_CMCC-CMS_rcp45_r1i1p1_20700101-20791231.nc', 
        'sit',
        np.nan
        ),
        ('/g/data/al33/replicas/CMIP5/output1/LASG-CESS/FGOALS-g2/abrupt4xCO2/mon/land/Lmon/r1i1p1/v1/prveg/prveg_Lmon_FGOALS-g2_abrupt4xCO2_r1i1p1_063001-063912.nc', 
        'prveg',
        0.0
        ),
        ('/g/data/al33/replicas/CMIP5/output1/CMCC/CMCC-CM/rcp85/6hr/atmos/6hrPlev/r1i1p1/v20170725/ta/ta_6hrPlev_CMCC-CM_rcp85_r1i1p1_2068030100-2068033118.nc', 
        'ta',
        247.55783
        ),
        ('/g/data/al33/replicas/CMIP5/combined/MOHC/HadGEM2-CC/rcp45/day/atmos/day/r1i1p1/v20120531/rlut/rlut_day_HadGEM2-CC_rcp45_r1i1p1_20351201-20401130.nc', 
        'rlut',
        200.8389
        ),
        ('/g/data/al33/replicas/CMIP5/combined/IPSL/IPSL-CM5A-LR/rcp26/day/atmos/cfDay/r1i1p1/v20120114/clw/clw_cfDay_IPSL-CM5A-LR_rcp26_r1i1p1_22060101-22151231.nc', 
        'clw',
        0.0
        ),
        ('/g/data/al33/replicas/CMIP5/output1/IPSL/IPSL-CM5A-LR/abrupt4xCO2/mon/atmos/Amon/r5i1p1/v20110921/rsds/rsds_Amon_IPSL-CM5A-LR_abrupt4xCO2_r5i1p1_185005-185504.nc', 
        'rsds',
        153.31345
        ),
        ('/g/data/al33/replicas/CMIP5/combined/MIROC/MIROC5/1pctCO2/mon/ocean/Omon/r1i1p1/v20131009/so/so_Omon_MIROC5_1pctCO2_r1i1p1_228501-228512.nc', 
        'so',
        0.0
        ),
        ('/g/data/al33/replicas/CMIP5/combined/CCCma/CanCM4/decadal1981/mon/ocean/Omon/r4i1p1/v20120622/hfls/hfls_Omon_CanCM4_decadal1981_r4i1p1_198201-199112.nc', 
        'hfls',
        np.nan
        ),
        ('/g/data/al33/replicas/CMIP5/combined/MPI-M/MPI-ESM-LR/decadal1992/mon/land/Lmon/r1i1p1/v20120529/cLitter/cLitter_Lmon_MPI-ESM-LR_decadal1992_r1i1p1_199301-200212.nc', 
        'cLitter',
        0.0
        ),
        ('/g/data/al33/replicas/CMIP5/output1/NASA-GISS/GISS-E2-R/1pctCO2/mon/aerosol/aero/r1i1p3/v20160425/emiss/emiss_aero_GISS-E2-R_1pctCO2_r1i1p3_192601-195012.nc', 
        'emiss',
        0.0
        ),
        ('/g/data/al33/replicas/CMIP5/combined/MIROC/MIROC-ESM-CHEM/rcp85/6hr/atmos/6hrLev/r1i1p1/v20111129/hus/hus_6hrLev_MIROC-ESM-CHEM_rcp85_r1i1p1_2063060106-2063070100.nc', 
        'hus',
        2.2376184e-05
        ),
        ('/g/data/al33/replicas/CMIP5/output1/MOHC/HadCM3/decadal1964/day/atmos/day/r6i3p1/v20140110/va/va_day_HadCM3_decadal1964_r6i3p1_19641101-19741230.nc', 
        'va',
        -4.4489503
        ),
        ('/g/data/al33/replicas/CMIP5/combined/LASG-CESS/FGOALS-g2/rcp45/day/seaIce/day/r1i1p1/v20161204/sit/sit_day_FGOALS-g2_rcp45_r1i1p1_20200101-20201231.nc', 
        'sit',
        0.0
        ),
        ('/g/data/al33/replicas/CMIP5/output1/NCAR/CCSM4/decadal1991/mon/seaIce/OImon/r3i2p1/v20120529/grCongel/grCongel_OImon_CCSM4_decadal1991_r3i2p1_199101-200012.nc', 
        'grCongel',
        np.nan
        ),
        ('/g/data/al33/replicas/CMIP5/output1/LASG-CESS/FGOALS-g2/decadal1960/mon/atmos/Amon/r1i1p1/v3/rsdscs/rsdscs_Amon_FGOALS-g2_decadal1960_r1i1p1_198101-199012.nc', 
        'rsdscs',
        81.612854
        ),
        ('/g/data/al33/replicas/CMIP5/output1/MRI/MRI-CGCM3/amip/mon/atmos/cfMon/r1i1p1/v20131011/hur/hur_cfMon_MRI-CGCM3_amip_r1i1p1_198901-199812.nc', 
        'hur',
        92.70255
        ),
        ('/g/data/al33/replicas/CMIP5/combined/INM/inmcm4/amip/3hr/atmos/3hr/r1i1p1/v20110323/huss/huss_3hr_inmcm4_amip_r1i1p1_2006010100-2006123121.nc', 
        'huss',
        0.0006068
        ),
        ('/g/data/al33/replicas/cordex/output/EAS-22/ICTP/MOHC-HadGEM2-ES/historical/r1i1p1/RegCM4-4/v0/day/ua925/v20190502/ua925_EAS-22_MOHC-HadGEM2-ES_historical_r1i1p1_ICTP-RegCM4-4_v0_day_19800101-19801230.nc', 
        'ua925',
        -0.32869282
        ),
        ('/g/data/al33/replicas/CMIP5/combined/CMCC/CMCC-CM/rcp45/6hr/atmos/6hrPlev/r1i1p1/v20170725/ua/ua_6hrPlev_CMCC-CM_rcp45_r1i1p1_2011010100-2011013118.nc', 
        'ua',
        -5.155791
        ),
        ('/g/data/al33/replicas/CMIP5/output1/NASA-GISS/GISS-E2-H/rcp45/mon/atmos/Amon/r4i1p3/v20160512/ccb/ccb_Amon_GISS-E2-H_rcp45_r4i1p3_215101-220012.nc', 
        'ccb',
        np.nan
        ),
        ('/g/data/al33/replicas/CMIP5/output1/MPI-M/MPI-ESM-LR/decadal1971/mon/land/Lmon/r1i1p1/v20120529/grassFrac/grassFrac_Lmon_MPI-ESM-LR_decadal1971_r1i1p1_197201-198112.nc', 
        'grassFrac',
        0.0
        ),
        ('/g/data/al33/replicas/CMIP5/combined/CNRM-CERFACS/CNRM-CM5/rcp85/6hr/atmos/6hrLev/r1i1p1/v20120525/ta/ta_6hrLev_CNRM-CM5_rcp85_r1i1p1_2095100106-2095110100.nc', 
        'ta',
        233.56656
        ),
        ('/g/data/al33/replicas/CMIP5/combined/NASA-GISS/GISS-E2-R/historical/mon/atmos/Amon/r5i1p3/v20160503/ch4/ch4_Amon_GISS-E2-R_historical_r5i1p3_197601-200012.nc', 
        'ch4',
        np.nan
        ),
        ('/g/data/al33/replicas/CMIP5/output1/ICHEC/EC-EARTH/decadal1965/mon/ocean/Omon/r8i2p1/v20120710/so/so_Omon_EC-EARTH_decadal1965_r8i2p1_196601-197512.nc', 
        'so',
        0.0
        ),
        ('/g/data/al33/replicas/CMIP5/output1/NOAA-GFDL/GFDL-ESM2G/rcp60/mon/atmos/Amon/r1i1p1/v20120412/evspsbl/evspsbl_Amon_GFDL-ESM2G_rcp60_r1i1p1_202101-202512.nc', 
        'evspsbl',
        1.9350772e-08
        ),
        ('/g/data/al33/replicas/CMIP5/output1/MOHC/HadGEM2-CC/historical/day/landIce/day/r1i1p1/v20110930/snw/snw_day_HadGEM2-CC_historical_r1i1p1_19691201-19741130.nc', 
        'snw',
        106252.55
        ),
        ('/g/data/al33/replicas/CMIP5/combined/LASG-CESS/FGOALS-g2/decadal1980/day/atmos/day/r2i1p1/v1/psl/psl_day_FGOALS-g2_decadal1980_r2i1p1_20000101-20001231.nc', 
        'psl',
        100025.44
        ),
        ('/g/data/al33/replicas/CMIP5/combined/CMCC/CMCC-CMS/piControl/mon/atmos/Amon/r1i1p1/v20120717/clivi/clivi_Amon_CMCC-CMS_piControl_r1i1p1_394401-395312.nc', 
        'clivi',
        0.00519617
        ),
        ('/g/data/al33/replicas/CMIP5/output1/NASA-GISS/GISS-E2-R/historicalMisc/mon/atmos/Amon/r1i1p315/v20160503/cli/cli_Amon_GISS-E2-R_historicalMisc_r1i1p315_197601-200012.nc', 
        'cli',
        3.8851712e-07
        ),
        ('/g/data/al33/replicas/CMIP5/output1/MPI-M/MPI-ESM-LR/1pctCO2/mon/atmos/Amon/r1i1p1/v20120308/va/va_Amon_MPI-ESM-LR_1pctCO2_r1i1p1_190001-190912.nc', 
        'va',
        -4.030592
        ),
        ('/g/data/al33/replicas/CMIP5/combined/NCC/NorESM1-ME/rcp85/mon/ocean/Omon/r1i1p1/v20130926/msftmyz/msftmyz_Omon_NorESM1-ME_rcp85_r1i1p1_204501-210012.nc', 
        'msftmyz',
        np.nan
        ),
        ('/g/data/al33/replicas/CMIP5/output1/NOAA-GFDL/GFDL-CM2p1/rcp45/mon/ocean/Omon/r3i1p1/v20110601/tauvo/tauvo_Omon_GFDL-CM2p1_rcp45_r3i1p1_201601-202012.nc', 
        'tauvo',
        np.nan
        ),
        ('/g/data/al33/replicas/CMIP5/combined/MIROC/MIROC4h/decadal1990/mon/ocean/Omon/r5i1p1/v20120326/wmo/wmo_Omon_MIROC4h_decadal1990_r5i1p1_199301-199306.nc', 
        'wmo',
        np.nan
        ),
        ('/g/data/al33/replicas/cordex/output/AUS-44i/CSIRO/CSIRO-BOM-ACCESS1-0/rcp85/r1i1p1/CCAM-2008/v1/day/vas/v20210518/vas_AUS-44i_CSIRO-BOM-ACCESS1-0_rcp85_r1i1p1_CSIRO-CCAM-2008_v1_day_20620101-20621231.nc',
        'vas',
        -3.0647216
        ),
 ]
)
def test_cmip5_values_correct(metacat,current_catalog,path, varname, first_ten_mean):
    """
    All these values are taken from the first 10 values of the first dimension 
    to minimize the amount of data we need to load. They have been verified against
    the production catalogd (as of 2024-11-20).
    """
    cmip5_cat = metacat["cmip5_al33"]
    esm_ds = cmip5_cat.search(path=path,variable=varname).to_dask()
    assert esm_ds
    # Subset to the first 10 values in the 0th dimension, first in all others
    da = esm_ds[varname]
    da = da.isel(**{da.dims[0]: slice(10), })
    da = da.isel(**{dim: 0 for dim in da.dims[1:]})
    da_val = da.mean(dim=da.dims[0], skipna=True).values

    if np.isnan(da_val).all():
        vals_equal = np.isnan(first_ten_mean)
    else:
        vals_equal = da_val == pytest.approx(first_ten_mean, abs=1e-6)
    
    assert vals_equal

    # Check that the data is the same in the current catalog
    cmip5_cat = current_catalog["cmip5_al33"]
    ... # Repeat above


@pytest.mark.order(after="test_catalog_subset_exists")
def test_built_esm_datastore():
    pass
