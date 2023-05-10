from setuptools import find_packages, setup

import versioneer

setup(
    name="catalog_manager",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    author="ACCESS-NRI",
    url="https://github.com/ACCESS-NRI/access-nri-intake-catalog",
    description="Tools and configuration info for managing ACCESS-NRI's intake catalogue",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License 2.0 (Apache-2.0)",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
    install_requires=[
        "cftime",
        "ecgtools",
        "intake-esm",
        "jsonschema",
    ],
    entry_points={
        "console_scripts": ["metacat-build=catalog_manager.cli:build"],
        "intake.catalogs": ["access_nri = catalog_manager:data"],
    },
)
