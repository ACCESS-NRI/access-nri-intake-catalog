.. _source:

Building sources
================

Each entry in an intake-dataframe-catalog refers to an Intake source. Intake sources may be pre-generated (e.g. 
the `CMIP5 and CMIP6 Intake-ESM datastore sources managed by NCI <https://opus.nci.org.au/pages/viewpage.action?pageId=213713098>`_) 
or they can be built using the :code:`access_nri_intake.source` sub-package. Currently, 
:code:`access_nri_intake.source` only supports building Intake-ESM datastore sources.

When :code:`access_nri_intake.source` is first imported, it downloads and parses a specific commit of the 
schema at https://github.com/ACCESS-NRI/schema/blob/main/file_asset.json and stores it in the variable
:code:`access_nri_intake.source.ESM_JSONSCHEMA`. This schema defines what metadata must be included in an 
Intake-ESM catalog, and what types and fields are allowed. Subsequent imports read the downloaded schema, 
unless the schema is changed (see :ref:`schema`), in which case the new schema is downloaded.

.. note::

   The "required" field in the downloaded schema is replaced with :code:`access_nri_intake.source.CORE_COLUMNS`
   prior to saving to :code:`access_nri_intake.source.ESM_JSONSCHEMA` to allow this field to be customized.

Builders
^^^^^^^^

New Intake-ESM datastores can be created for ACCESS model output using the Builder classes in the sub-module 
:code:`access_nri_intake.source.builders`. All public Builders inherit from 
:code:`access_nri_intake.source.builders.BaseBuilder`, which inherits from :code:`ecgtools.builder.Builder`. 
:code:`access_nri_intake.source.builders.BaseBuilder` simply reorganises the functionality in 
:code:`ecgtools.builder.Builder` so that self-contained Builder classes can be easily written for different 
types of model output. All Builders validate against the schema in 
:code:`access_nri_intake.source.ESM_JSONSCHEMA`.

.. _builder_create:

Creating a new Builder
^^^^^^^^^^^^^^^^^^^^^^

New Builders should inherit from :code:`access_nri_intake.source.builders.BaseBuilder`. In general, all that 
should be required is to:

* Hardcode some of the optional :code:`kwargs` on :code:`BaseBuilder.__init__` in the :code:`__init__` method 
  of the new Builder. The exact choice of which :code:`kwargs` are hardcoded will depend on the specifics of 
  the target model output. It might be useful to look at existing Builder class implementations and at the 
  :code:`ecgtools.builder.Builder` class, which is inhereted by 
  :code:`access_nri_intake.source.builders.BaseBuilder` - see the :code:`ecgtools` documentation 
  `here <https://ecgtools.readthedocs.io/en/latest/reference/index.html#builder>`_. The 
  `Intake-ESM specifications <https://intake-esm.readthedocs.io/en/stable/reference/esm-catalog-spec.html>`_ 
  will also be helpful for interpreting many of the :code:`kwargs`

* Overwrite the static :code:`parser` method. This method should receive a single file (usually netcdf) and 
  return a dictionary containing all of the metadata to include in the Intake-ESM datastore. As described above 
  the dictionary is validated against :code:`access_nri_intake.source.ESM_JSONSCHEMA`. The :code:`parser` method serves the 
  same function as custom parsers in :code:`ecgtools`, so `the documentation on these 
  <https://ecgtools.readthedocs.io/en/latest/how-to/use-a-custom-parser.html>`_ may be useful. Again, it will 
  also be helpful to look at existing Builder class implementations.

.. _source_api:

API for :code:`access_nri_intake.source`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This documentation has been auto-generated using `sphinx-autoapi <https://sphinx-autoapi.readthedocs.io/en/latest/>`_

.. toctree::
   :maxdepth: 6

   ../autoapi/access_nri_intake/source/index
