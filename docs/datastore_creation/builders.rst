.. _builders:

Datastore Builders
==================

The same Python package that includes the ACCESS-NRI catalog, :code:`access-nri-intake`, also includes a set of Intake-ESM datastore Builders for different ACCESS model outputs. In general, building an Intake-ESM datastore for your model output should be as simple as passing your output base directory to an appropriate Builder.

The :code:`access-nri-intake` package is installed in the :code:`xp65` and :code:`hh5` analysis environments, or users can install it into their own environment (see :ref:`installation` for details). The Builders can be imported from the :code:`access_nri_intake.esmcat.builders` submodule.

There are currently three Builders available. Their public APIs are given below.

.. note::
   These Builders are used by ACCESS-NRI to create the ACCESS-NRI catalog.

ACCESS-OM2 output: :code:`AccessOm2Builder`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: access_nri_intake.esmcat.builders.AccessOm2Builder
   :special-members: __init__, build, save

ACCESS-ESM1.5 output: :code:`AccessEsm15Builder`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: access_nri_intake.esmcat.builders.AccessEsm15Builder
   :special-members: __init__, build, save

ACCESS-CM2 output: :code:`AccessCm2Builder`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: access_nri_intake.esmcat.builders.AccessCm2Builder
   :special-members: __init__, build, save
