.. _adding:

Adding datastores to the catalog
================================

Adding your datastore to the ACCESS-NRI catalog is an easy way to make your data findable and useable by others in the community. If you've created an Intake-ESM datastore for your data using one of the :code:`access-nri-intake` Builders, then it should be trivial to add it to the catalog. In addition to your functioning datastore, all that's required is a file called :code:`metadata.yaml` containing the following:

.. include:: ../../metadata.yaml
   :literal:

Ideally this file will live in the base output directory of your model run, but it doesn't have to.

Submitting a data request
^^^^^^^^^^^^^^^^^^^^^^^^^

Please open a catalog data request `here <https://github.com/ACCESS-NRI/access-nri-intake-catalog/issues/new?assignees=&labels=&projects=&template=catalog-data-request.md&title=%5BDATA+REQUEST%5D+Add+%3Cname+of+data+product%3E>`_. Please don't feel like you need to have a datastore and :code:`metadata.yaml` ready before submitting a request - we're happy to help you through the process.

.. note::
   Datastores don't have to have been created by :code:`access-nri-intake` Builders in order to be added to the catalog. If you have an Intake-ESM datastore (or indeed another type of Intake Source) that you think should be in the catalog, please open a catalog data request.