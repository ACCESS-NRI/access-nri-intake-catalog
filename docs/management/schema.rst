.. _schema:

Updating schema
===============

A specific version of the schema 
`here <https://github.com/ACCESS-NRI/schema/tree/main/au.org.access-nri/model/output/file-metadata>`_ is 
downloaded when :code:`access_nri_intake.source` is first imported. This schema is used to validated Intake-ESM 
datastore entries. Similarly a specific version of the schema 
`here <https://github.com/ACCESS-NRI/schema/tree/main/au.org.access-nri/model/output/experiment-metadata>`_ is 
downloaded when :code:`access_nri_intake.catalog` is first imported and this is used to validate 
intake-dataframe-catalog entries.

Schema can be updated by updating the file(s) at https://github.com/ACCESS-NRI/schema and editing the 
appropriate :code:`SCHEMA_URL` path(s) in :code:`access_nri_intake.source.__init__` and 
:code:`access_nri_intake.catalog.__init__`. A hash for the updated schema files is also required (as 
:code:`SCHEMA_HASH`). The easiest way to update this is to first set :code:`SCHEMA_HASH` to :code:`None`. The 
updated hash will then be printed to screen when the sub-package is imported and this can be copied and pasted 
across.

.. warning::

   Translators are schema-specific. Certain updates to the schema may require that Translators need to be 
   rewritten and potentially other source code changes.
