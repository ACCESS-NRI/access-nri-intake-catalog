.. _builders:

Datastore Builders
==================

The same Python package that includes the ACCESS-NRI catalog, access-nri-intake, also includes a 
set of Intake-ESM datastore Builders for different ACCESS model outputs. In general, building an Intake-ESM 
datastore for your ACCESS model output should be as simple as passing your output base directory to an 
appropriate Builder.

The access-nri-intake package is installed in the :code:`xp65` analysis environment, or 
users can install it into their own environment (see :ref:`installation` for details). The Builders can be 
imported from the :code:`access_nri_intake.source.builders` submodule.

There are currently seven Builders available. Their core public APIs are given below (their full APIs can be
found in :ref:`source_api`).

.. note::
   These Builders are used by ACCESS-NRI to create the ACCESS-NRI catalog.

ACCESS-OM2 output: :code:`AccessOm2Builder`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: access_nri_intake.source.builders.AccessOm2Builder
   :special-members: __init__, build, save
   :noindex:

ACCESS-ESM1.5 output: :code:`AccessEsm15Builder`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: access_nri_intake.source.builders.AccessEsm15Builder
   :special-members: __init__, build, save
   :noindex:

ACCESS-CM2 output: :code:`AccessCm2Builder`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: access_nri_intake.source.builders.AccessCm2Builder
   :special-members: __init__, build, save
   :noindex:

ACCESS-OM3 output: :code:`AccessOm3Builder`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: access_nri_intake.source.builders.AccessOm3Builder
   :special-members: __init__, build, save
   :noindex:

MOM6 output: :code:`Mom6Builder`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: access_nri_intake.source.builders.Mom6Builder
   :special-members: __init__, build, save
   :noindex:

ROMSIceShelf output: :code:`ROMSBuilder`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: access_nri_intake.source.builders.ROMSBuilder
   :special-members: __init__, build, save
   :noindex:

World Ocean Atlas output: :code:`WoaBuilder`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: access_nri_intake.source.builders.WoaBuilder
   :special-members: __init__, build, save
   :noindex:

.. note::

   If you have ACCESS model output that isn't compatible with the existing set of Builders, check out the
   :ref:`builder_create` section or open an issue 
   `here <https://github.com/ACCESS-NRI/access-nri-intake-catalog/issues/new/choose>`_.
