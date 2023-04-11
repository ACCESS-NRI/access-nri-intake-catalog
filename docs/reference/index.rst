.. _Reference:

Reference
=========

.. toctree::
   :maxdepth: 1
   :hidden:

   API_reference

The ACCESS-NRI Intake catalog is managed using a purpose-built Python library called `catalog_manager`. The `catalog_manager` library includes:

* A set of Builders for creating `intake-esm <https://intake-esm.readthedocs.io/en/stable/#>`_ catalogs from a structured directory or directories of climate data assets (e.g. netcdf files).
* A set of Translators for translating metadata (using intake-esm columns) in a subcatalog into a common language across subcatalogs (e.g. translating realm metadata from "atm" to "atmos", "ocn" to "ocean", "lnd" to "land" etc).
* A tool for adding/updating subcatalogs in a metacatalog (a catalog of catalogs).

