.. _builders:

Datastore :code:`Builders`
==========================

The same Python package that includes the ACCESS-NRI catalog, :code:`access-nri-intake`, also includes a set of Intake-ESM datastore Bbuilders for different ACCESS model outputs. In general, building an Intake-ESM datastore for your model output should be as simple as passing your output base directory to an appropriate Builder.

The :code:`access-nri-intake` package is installed in the :code:`xp65` and :code:`hh5` analysis environments, or users can install it into their own environment (see :ref:`installation` for details). The Builders can be imported from the :code:`access-nri-intake.esmcat.builders` submodule.

There are currently three Builders available. Their APIs are given below.

.. note::
   These Builders are used by ACCESS-NRI to create the ACCESS-NRI catalog.

:code:`AccessOm2Builder`
^^^^^^^^^^^^^^^^^^^^^^^^

For building Intake-ESM datastores for ACCESS-OM2 model output. Import this Builder using::

   from access_nri_intake.esmcat.builders import AccessOm2Builder

:code:`AccessCm2Builder`
^^^^^^^^^^^^^^^^^^^^^^^^

For building Intake-ESM datastores for ACCESS-CM2 model output. Import this Builder using::

   from access_nri_intake.esmcat.builders import AccessCm2Builder

:code:`AccessESM15Builder`
^^^^^^^^^^^^^^^^^^^^^^^^^^

For building Intake-ESM datastores for ACCESS-ESM1.5 model output. Import this Builder using::

   from access_nri_intake.esmcat.builders import AccessESM15Builder