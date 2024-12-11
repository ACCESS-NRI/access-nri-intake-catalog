.. _adding:

Adding datastores to the catalog
================================

Adding your datastore to the ACCESS-NRI catalog is an easy way to make your data findable and useable by others 
in the community. If you've created an Intake-ESM datastore for your data using one of the access-nri-intake 
Builders, then it should be trivial to add it to the catalog. In addition to your functioning datastore, all that's 
required is a file called :code:`metadata.yaml` containing core high-level metadata describing your data - see the 
section on :ref:`metadata`.

Submitting a data request
^^^^^^^^^^^^^^^^^^^^^^^^^

Please open a catalog data request `here <https://github.com/ACCESS-NRI/access-nri-intake-catalog/issues/new/choose>`_. 
Please don't feel like you need to have a datastore and :code:`metadata.yaml` ready before submitting a request - 
we're happy to help you through the process.

.. note::
   Datastores don't have to have been created by access-nri-intake Builders in order to be added to the 
   catalog. If you have an Intake-ESM datastore (or indeed another type of Intake source) that you think should be in the 
   catalog, please open a catalog data request.

.. warning:: 
    If you are providing an existing Intake-ESM datastore to be added to :code:`access-nri-intake-catalog`, the 
    datastore must be in its final form **before** you make a data request. If a datastore is changed
    after we have verified that we are able to ingest it, it will break future catalog builds and may be 
    removed.

    If you need to update a datastore that is already in :code:`access-nri-intake-catalog`, please contact us as
    described above.