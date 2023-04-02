from setuptools import find_packages, setup
import versioneer

setup(
    name="catalog_manager",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    author="ACCESS-NRI",
    url="https://github.com/ACCESS-NRI/nri_intake_catalog",
    description="Tools and configuration info for managing ACCESS-NRI's intake catalogue",
    packages=find_packages(),
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
)
