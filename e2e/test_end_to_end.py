from access_nri_intake.cli import build

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


def test_build_esm_datastore():
    build(
        [
            "--config_yaml",
            "/scratch/tm70/ct1163/configs/cmip5.yaml",
            "/scratch/tm70/ct1163/configs/access-om2.yaml",
            "--build_base_path",
            "/scratch/tm70/ct1163/test_cat/",
            "--catalog_base_path",
            "./",
            "--catalog_file",
            "metacatalog.csv",
            "--version",
            "v2024-11-18",
            "--no_update",
            "False",
        ]
    )

    assert True


def test_translate_esm_datastore():
    pass
