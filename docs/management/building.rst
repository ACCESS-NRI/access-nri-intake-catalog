.. _building:

Building the catalog
====================

The access-nri-intake package includes a command line script called :code:`catalog-build` for building 
catalogs using the tools described in the previous sections from :ref:`config_files` that specify the 
paths to sources and which Builders and Translators to use. It can be used as follows::

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

The ACCESS-NRI catalog is built using this script by submitting the :code:`build_all.sh` shell script 
in the :code:`bin/` directory of https://github.com/ACCESS-NRI/access-nri-intake-catalog.

.. _config_files:

Configuration files
^^^^^^^^^^^^^^^^^^^

:code:`metadata.yaml` files
^^^^^^^^^^^^^^^^^^^^^^^^^^^

* Ensure that there is core metadata associated with all data products
* Are add to the Intake-ESM datastore source :code:`.metadata` attribute, so are available to the 
  Translators. E.g., the product descriptions in the ACCESS-NRI catalog come from here.
