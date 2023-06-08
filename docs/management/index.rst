.. _management:

Catalog management
==================

The ACCESS-NRI Intake catalog is managed using a purpose-built Python package called access-nri-intake. 
Instructions for how to install access-nri-intake can be found in the :ref:`installation` section.

Structure of access-nri-intake
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The authoritative source code for access-nri-intake can be found in the :code:`src/` directory of 
https://github.com/ACCESS-NRI/access-nri-intake-catalog. This GitHub repo also includes configuration files 
(in :code:`config/`) that are used by access-nri-intake to create the ACCESS-NRI catalog.

The access-nri-intake package contains three sub-packages:

* :code:`access_nri_intake.source` includes Builders and supporting tools for creating Intake-ESM datastores 
  from ACCESS model output
* :code:`access_nri_intake.catalog` includes tools to build the ACCESS-NRI catalog including Translators for 
  translating metadata in source datastores to be compatible with the catalog schema
* :code:`access_nri_intake.data` is an `Intake catalog data package 
  <https://intake.readthedocs.io/en/latest/data-packages.html>`_ for the ACCESS-NRI catalog files on Gadi. This 
  is registered as a entry in Intake's :code:`cat` sub-package, making opening the catalog as easy as::

    import intake
    cat = intake.cat.access_nri

