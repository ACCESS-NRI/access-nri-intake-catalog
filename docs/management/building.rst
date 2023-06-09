.. _building:

Building the catalog
====================

Configuration files
^^^^^^^^^^^^^^^^^^^

The :code:`catalog-build` script can be used as follows::

   $ catalog-build --help
   usage: catalog-build [-h] [--build_base_path BUILD_BASE_PATH] [--catalog_file CATALOG_FILE]
                        [--version VERSION]
                        config_yaml [config_yaml ...]

   Build an intake-dataframe-catalog from YAML configuration file(s).
   
   positional arguments:
     config_yaml           Configuration YAML file(s) specifying the Intake source(s) to add.

   options:
     -h, --help            show this help message and exit
     --build_base_path BUILD_BASE_PATH
                           Directory in which to build the catalog and source(s). A directory with name
                           equal to the version (see the `--version` argument) of the catalog being built
                           will be created here. The catalog file (see the `--catalog_file` argument) will
                           be written into this version directory, and any new intake source(s) will be
                           written into a 'source' directory within the version directory. Defaults to the
                           current work directory.
     --catalog_file CATALOG_FILE
                           The name of the intake-dataframe-catalog. Defaults to 'metacatalog.csv'
     --version VERSION     The version of the catalog to build/add to. Defaults to the current version of
                           access-nri-intake.

