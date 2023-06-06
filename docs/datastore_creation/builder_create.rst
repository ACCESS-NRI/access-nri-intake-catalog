.. _builder_create:

Creating a custom Builder
=========================

New Builders should inherit from :code:`access_nri_intake.esmcat.builders.BaseBuilder`. In general, all that should be required is to:

* Hardcode some of the optional :code:`kwargs` on :code:`BaseBuilder.__init__` in the :code:`__init__` method of the new Builder. The exact choice of which :code:`kwargs` are hardcoded will depend on the specifics of the target model output. It might be useful to look at existing Builder class implementations and at the :code:`ecgtools.builder.Builder` class, which is inhereted by :code:`access_nri_intake.esmcat.builders.BaseBuilder` - see the :code:`ecgtools` documentation `here <https://ecgtools.readthedocs.io/en/latest/reference/index.html#builder>`_.

* Overwrite the static :code:`parser` method. This method should receive a single file (usually netcdf) and return a dictionary containing all of the metadata to include in the Intake-ESM datastore. The dictionary is checked against a `schema that requires that a core set of metadata keys are present <https://github.com/ACCESS-NRI/schema/blob/main/file_asset.json>`_. The :code:`parser` method serves the same function as custom parsers in :code:`ecgtools`, so `the documentation on these <https://ecgtools.readthedocs.io/en/latest/how-to/use-a-custom-parser.html>`_ may be useful. Again, it will also be helpful to look at existing Builder class implementations.

.. note::
   ACCESS-NRI is here to help. If you have ACCESS model output that isn't compatible with the existing set of Builders, please open an issue `here <https://github.com/ACCESS-NRI/access-nri-intake-catalog/issues>`_.

