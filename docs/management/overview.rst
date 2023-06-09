.. _overview:

Overview
========

The authoritative source code for access-nri-intake can be found in the :code:`src/` directory of 
https://github.com/ACCESS-NRI/access-nri-intake-catalog. This GitHub repo also includes configuration files 
(in :code:`config/`) that are used by access-nri-intake to create the ACCESS-NRI catalog, and some command-
line scripts (in :code:`bin/`) for doing various catalog-related tasks.

High-level structure of access-nri-intake
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The access-nri-intake package contains three sub-packages:

* :code:`access_nri_intake.source` contains Builders and supporting tools for creating Intake-ESM datastores 
  from ACCESS model output
* :code:`access_nri_intake.catalog` contains tools to build the ACCESS-NRI catalog including Translators for 
  translating metadata in source datastores to be compatible with the :ref:`catalog schema <schema>`.
* :code:`access_nri_intake.data` is an `Intake catalog data package 
  <https://intake.readthedocs.io/en/latest/data-packages.html>`_ for the ACCESS-NRI catalog files on Gadi. This 
  is registered as a entry in Intake's :code:`cat` sub-package when access-nri-intake is installed, making 
  opening the catalog as easy as::

    import intake
    cat = intake.cat.access_nri

More detailed documentation of access-nri-intake source code is provided in the following sections.

.. _schema:

Schema
^^^^^^

Entries in the ACCESS-NRI catalog are validated against the schema at https://github.com/ACCESS-NRI/schema. In 
particular:

* Rows in Intake-ESM datastores created with access-nri-intake Builders are validated against 
  https://github.com/ACCESS-NRI/schema/blob/main/file_asset.json upon creation
* Entries in the ACCESS-NRI catalog are validated against 
  https://github.com/ACCESS-NRI/schema/blob/main/experiment_asset.json when they are added to the catalog

These schema are downloaded locally the first time :code:`access_nri_intake` is imported.

.. note::

   This means that network access must be available to import access-nri-intake for the first time after 
   installing a new version.