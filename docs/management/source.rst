.. _source:

Building sources
================

Each entry in the ACCESS-NRI catalog refers to an Intake source. Intake sources may be pre-generated (e.g. the `CMIP5 and CMIP6 Intake-ESM datastore sources managed by NCI <https://opus.nci.org.au/pages/viewpage.action?pageId=213713098>`_) or they can be generated using the :code:`access_nri_intake.source` sub-package. Currently, :code:`access_nri_intake.source` only supports building Intake-ESM datastore sources.

New Intake-ESM datastores can be created from ACCESS model output using the Builder classes in the sub-module :code:`access_nri_intake.source.builders`


API for :code:`access_nri_intake.source`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This documentation has been auto-generated using `sphinx-autoapi <https://sphinx-autoapi.readthedocs.io/en/latest/>`_

.. toctree::
   :maxdepth: 6

   ../autoapi/access_nri_intake/source/index
