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
in the :code:`bin/` directory of https://github.com/ACCESS-NRI/access-nri-intake-catalog. See the section 
on :ref:`release` for more details.

.. _config_files:

Configuration files
^^^^^^^^^^^^^^^^^^^

The :code:`catalog-build` script reads configuration files like the ones found in 
https://github.com/ACCESS-NRI/access-nri-intake-catalog/config (these are the configuration files used to 
build the ACCESS-NRI catalog). Configuration files should include the Builder and Translator to use along 
with a list of sources to process. As a minimum, each source should specify the path(s) to pass to the 
Builder and the path to the :ref:`metadata.yaml <metadata>` file for that source. Additional 
:code:`kwargs` to pass to the Builder can also be specified. As an example, a configuration file might 
look something like::

   builder: AccessCm2Builder

   translator: DefaultTranslator

   sources:

     - path:
         - /g/data/p73/archive/non-CMIP/ACCESS-CM2/bx944
         - /g/data/p73/archive/non-CMIP/ACCESS-CM2/bx944a
         - /g/data/p73/archive/non-CMIP/ACCESS-CM2/bx944b
         - /g/data/p73/archive/non-CMIP/ACCESS-CM2/bx944c
         - /g/data/p73/archive/non-CMIP/ACCESS-CM2/bx944d
       metadata_yaml: /g/data/p73/archive/non-CMIP/ACCESS-CM2/bx944/metadata.yaml
       ensemble: true

In most cases, adding a new Intake-ESM datastore to the ACCESS-NRI catalog should be as simple as adding 
a new entry to the `configuration files <https://github.com/ACCESS-NRI/access-nri-intake-catalog/config>`_ 
and rebuilding the catalog.

.. _metadata:

:code:`metadata.yaml` files
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Each source in the catalog must have an associated :code:`metadata.yaml` file that includes key high-level 
metadata about the data product. This is to ensure that there is core metadata associated with all data 
products in the catalog. Additionally, this core metadata is added to the corresponding Intake-ESM 
datastore's :code:`metadata` attribute, meaning it is available to Translators and to catalog users wanting 
to know more about a particular product. The contents of the :code:`metadata.yaml` files are validated against 
:code:`access_nri_intake.catalog.EXP_JSONSCHEMA` (see :ref:`catalog`) when the script :code:`catalog-build` 
is called to ensure that all required metadata is available prior to building the catalog. The 
:code:`metadata.yaml` file should include the following:

.. include:: ../../metadata.yaml
   :literal:

Ideally this file will live in the base output directory of your model run so that it's easy for others to 
find, even if they aren't using the catalog (but it doesn't have to).
