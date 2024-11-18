import os
from datetime import datetime

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


def test_build_esm_datastore(BASE_DIR):
    # Build our subset of the catalog. This should take ~2 minutes with the PBS
    # flags in build_subset.sh
    print(f"Building the catalog subset & writing to {BASE_DIR}")
    v_num = datetime.now().strftime("v%Y-%m-%d")
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

    print_directory_tree(BASE_DIR)

    assert True

    print("Catalog built successfully. Finish test tomorrow.")


def test_translate_esm_datastore():
    pass
