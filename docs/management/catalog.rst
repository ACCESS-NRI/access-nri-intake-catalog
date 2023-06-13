.. _catalog:

Adding sources
==============

The :code:`access_nri_intake.catalog` sub-package contains tools to create/extend/trim 
intake-dataframe-catalogs of Intake-ESM datastores. The 
:code:`access_nri_intake.catalog.manager.CatalogManager` class can be used to create a new 
intake-dataframe-catalog or load an existing one. Intake-ESM datastore sources can be built (using an 
:code:`access_nri_intake.source.builders` Builder) or loaded and then added to the catalog. 
:ref:`translators` are specified to translate the metadata in source datastores to be compatible with the 
catalog schema.

When :code:`access_nri_intake.catalog` is first imported, it downloads and parses the a specific commit 
of the schema at https://github.com/ACCESS-NRI/schema/blob/main/experiment_asset.json. The raw schema is 
stored in the variable :code:`access_nri_intake.catalog.EXP_JSONSCHEMA` (more on this later) and a 
version with the "required" field replaced with :code:`access_nri_intake.catalog.CORE_COLUMNS` is stored 
in the variable :code:`access_nri_intake.catalog.CATALOG_JSONSCHEMA` (this is to allow this field to be 
customized). The latter defines what metadata must be included in the intake-dataframe-catalog, and what 
types and fields are allowed. Subsequent imports read the downloaded schema, unless the schema is changed 
(see :ref:`schema`), in which case the new schema is downloaded.

.. _translators:

Translators
^^^^^^^^^^^

Translators receive an Intake source to translate from and a list of metadata columns to target (these 
are the columns in the intake-dataframe-catalog), and return a dataframe of translated data when their 
:code:`translate` method is called. The returned dataframe has rows containing tuples of unique values 
of the translated metadata after grouping by the metadata columns specified in
:code:`access_nri_intake.catalog.TRANSLATOR_GROUPBY_COLUMNS`.

When a source is added to the catalog and no translator 
is specified, the translator defaults to :code:`access_nri_intake.catalog.translators.DefaultTranslator` 
which operates as follows:

* If the input source is an Intake-ESM datastore, the translator will first look for the column in 
  the :code:`esmcat.df` attribute, casting iterable columns to tuples. If the source is not an 
  Intake-ESM datastore, this step is skipped.
* If that fails, the translator will then look for the column name as an attribute on the source 
  itself
* If that fails, the translator will then look for the column name in the :code:`metadata` attribute of 
  the source

The :code:`access_nri_intake.catalog.translators.DefaultTranslator` is appropriate for Intake-ESM 
datastore sources built using :code:`access_nri_intake.source.builders` because the schema used to validate 
the datastores is consistent with the schema used to validate the catalog.

When adding a pre-generated Intake-ESM datastore to the catalog, a dedicated Translator may be required. 
For example, the 
`CMIP5 and CMIP6 NCI-managed Intake-ESM datastores <https://opus.nci.org.au/pages/viewpage.action?pageId=213713098>`_
that are included in the ACCESS-NRI catalog use dedicated translators which implement specific translations 
from the CMIP vocabulary used in the Intake-ESM datastore to the vocabulary used in the catalog schema (e.g.
see :code:`access_nri_intake.catalog.translators.Cmip6Translator`).

Creating a new Translator
^^^^^^^^^^^^^^^^^^^^^^^^^

New Translators should inherit from :code:`access_nri_intake.catalog.translators.DefaultTranslator`. The 
general approach to creating a new translator is to create a specific translator method for each input 
column that cannot use the default translator. These methods should return a dataframe object. Take a look
at the existing Translator class implementations for examples.

API for :code:`access_nri_intake.catalog`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This documentation has been auto-generated using `sphinx-autoapi <https://sphinx-autoapi.readthedocs.io/en/latest/>`_

.. toctree::
   :maxdepth: 6

   ../autoapi/access_nri_intake/catalog/index

