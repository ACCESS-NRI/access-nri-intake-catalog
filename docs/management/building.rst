.. _building:

Building the catalog
====================

The access-nri-intake package includes a command line script called :code:`catalog-build` for building 
catalogs using the tools described in the previous sections from :ref:`config_files` that specify the 
paths to sources and which Builders and Translators to use. It can be used as follows::

   $ catalog-build --help
   usage: catalog-build [-h] [--build_base_path BUILD_BASE_PATH] [--catalog_base_path CATALOG_BASE_PATH] 
      [--catalog_file CATALOG_FILE] [--version VERSION] [--no_update] config_yaml [config_yaml ...]

   Build an intake-dataframe-catalog from YAML configuration file(s).

positional arguments:
  config_yaml           Configuration YAML file(s) specifying the Intake source(s) to add.

options:
  -h, --help            show this help message and exit
  --build_base_path BUILD_BASE_PATH
                        Directory in which to build the catalog and source(s). A directory with
                        name equal to the version (see the `--version` argument) of the catalog
                        being built will be created here. The catalog file (see the
                        `--catalog_file` argument) will be written into this version directory,
                        and any new intake source(s) will be written into a 'source' directory
                        within the version directory. Defaults to the current work directory.
  --catalog_base_path CATALOG_BASE_PATH
                        Directory in which to place the catalog.yaml file. This file is the
                        descriptor of the catalog, and provides references to the data locations
                        where the catalog data itself is stored (build_base_path). Defaults to
                        the current work directory.
  --data_base_path DATA_BASE_PATH
                        Home directory that contains the data referenced by the input experiment
                        YAMLfiles. Typically only required for testing. Defaults to None.
  --catalog_file CATALOG_FILE
                        The name of the intake-dataframe-catalog. Defaults to 'metacatalog.csv'
  --version VERSION     The version of the catalog to build/add to. Defaults to the current date.
  --no_update           Set this if you don't want to update the access_nri_intake.data (e.g. if
                        running a test)
  --no_concretize       Set this if you don't want to concretize the build, ie. keep the new
                        catalog in .$VERSION & don't update catalog.yaml

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
to know more about a particular product. Ideally this file will live in the base output directory of your model run so that it's easy for others to 
find, even if they aren't using the catalog (but it doesn't have to).

The contents of the :code:`metadata.yaml` files are validated against 
:any:`access_nri_intake.catalog.EXP_JSONSCHEMA` (see :ref:`catalog`) when the script :code:`catalog-build` 
is called to ensure that all required metadata is available prior to building the catalog. The 
:code:`metadata.yaml` file should include the following:

.. literalinclude:: metadata.yaml
   :language: yaml

.. warning:: 

   Your experiment `UUID <https://en.wikipedia.org/wiki/Universally_unique_identifier>`_ 
   must be **unique** to the experiment. Even if you're adding multiple related experiments,
   each experiment must have a unique UUID.

   There's nothing special about the UUID value - they're simply meant to be randomly-generated values that 
   are almost guaranteed to be unique. You can get a UUID value easily from any Unix system by running the 
   :code:`uuidgen` command:

   .. code-block:: bash

      > uuidgen
      36C2010B-9D65-4066-AB91-CE9D1FAE30B4

.. topic:: Quick notes on YAML structure

   For those who haven't seen a YAML file before, the structure can be subtly confusing. Here's some tips
   to help make your YAML editing time as pain-free as possible:

   - :code:`<` and :code:`>` are being used here to denote where you should replace the existing text with your experiment's
     metadata values. They should *not* be kept in your final :code:`metadata.yaml`.
   - String values simply follow the colon after the key name, e.g.:

     .. code-block:: yaml
     
         name: my_first_experiment
     
     Longer strings (in this case, the :code:`long_description`), can use a special syntax to give you room
     to input a multi-line string:

     .. code-block:: yaml
   
         long_description: >-
            This is my multi-line description.
            YAML will treat this as a text 'block', in effect.

   - The attributes shown in the template metadata where the value is preceded with a ':code:`-`' are *lists*. Lists
     are denoted by lines starting with the :code:`-` character. For example, if your experiment contains data from both
     the :code:`ocean` and :code:`seaIce` realms, your :code:`realm` should look like this:

     .. code-block:: yaml

         realm:
         - ocean
         - seaIce

     If a list is going to be left empty, it still needs to be a list. For example, if you have no 
     :code:`related_experiments`, but you want to keep the key in the YAML file, you'll need to leave it like this:

     .. code-block:: yaml

         related_experiments:
         - 


.. note::

   The access-nri-intake package includes some command-line utility scripts to help with creating and 
   validating :code:`metadata.yaml` files:

   * To create an empty :code:`metadata.yaml` template in the current directory::

      $ metadata-template

     You'll then need to replace all the values enclosed in :code:`<>`. Fields marked as :code:`REQUIRED` are
     required. All other fields are encouraged but can be deleted or commented out if they are not relevant.

   * To validate a :code:`metadata.yaml` file (i.e. to check that required fields are present with required types)::

      $ metadata-validate <path/to/metadata.yaml>

.. _versioning:

Catalog versioning
^^^^^^^^^^^^^^^^^^

.. note:: 
   
   New in version 0.1.4.

Catalog versions (as distinct from the package version of :code:`access_nri_intake_catalog`) are a date-formatted string, 
e.g., :code:`v2024-11-29`.

When a new catalog version is built (see :ref:`release`), the build script will analyze both the catalog storage directory
defined by :code:`--build_base_path`, and the catalog YAML location defined by :code:`--catalog_base_path`, and then create or update 
the catalog reference YAML (:code:`catalog.yaml`) as follows:

1. If no :code:`catalog.yaml` exists in :code:`--catalog_base_path`, then a new one will be created, with the default catalog version
   set to the new catalog version. The minimum and maximum supported catalog versions will be calculated as follows:

   a. If there are no directories or symlinks in :code:`--build_base_path` that match the version naming schema, it is assumed that 
      no other catalog versions exists, and the minimum/maximum catalog version will be set to the new version;
   b. If there are existing catalog directories in :code:`--build_base_path`, the build system will assume that those catalogs
      are compatible with the new catalog, and will compute a minimum and maximum catalog version to encompass those 
      existing directories (i.e., the minimum and maximum catalog version in :code:`catalog.yaml` will be the minimum and maximum 
      catalog versions currently in :code:`--build_base_path`, modulo the new version number).

2. If a :code:`catalog.yaml` exists in :code:`--catalog_base_path`, and the newly-built catalog appears to have a consistent structure 
   and schema to that defined in the existing :code:`catalog.yaml`, then the existing :code:`catalog.yaml` will be updated to have new default
   and maximum versions equal to the new catalog version; the previous minimum version will not be altered. The presence/absence of 
   catalog directories in :code:`--build-base-path` will not be considered.

3. If a :code:`catalog.yaml` exists in :code:`--catalog_base_path`, but the newly-built catalog has a different structure/schema to 
   what's defined in the existing catalog, then a brand-new :code:`catalog.yaml` will be created, describing the new catalog structure,
   and setting all versions (minimum, maximum, default) to the new catalog version. The existing :code:`catalog.yaml` will be renamed to
   :code:`catalog-<old min version>-<old max version>.yaml`, or :code:`catalog-<version>.yaml` if it only supported a single 
   catalog version. The presence/absence of catalog directories in :code:`--build-base-path` 
   will not be considered.

:code:`access_nri_intake_catalog` only links a singular :code:`catalog.yaml` to the entry point :code:`intake.cat.access_nri`; either the 
user's local version, or if that does not exist, the live version on Gadi (see :ref:`faq`). To load outdated catalogs from Gadi, we recommend 
copying the :code:`catalog-<old min version>-<old max version>.yaml` to :code:`~/.access_nri_intake_catalog/catalog.yaml`.
