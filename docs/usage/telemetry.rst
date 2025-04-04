.. _telemetry:

Telemetry
====

In order to understand how our users interact with the ACCESS-NRI Intake catalog, we collect some 
usage statistics, in accordance with the privacy policy detailed 
`here <https://reporting.access-nri-store.cloud.edu.au/>`_.

A typical telemetry record is shown below:

.. code-block:: python

   {
       "timestamp": "2025-04-01T01:07:53.146786Z",
       "name": "unknown",
       "function": "esm_datastore.search",
       "args": [],
       "kwargs": {
           "file_id": "ocean_month"
       },
       "session_id": "df07384b-acca-49e9-949c-dbef13b45562",
       "catalog_version": "v2025-03-04"
   }

In this example, the user has called the :code:`.search` function on an ESM-Datastore, with the 
following signature:

.. code-block:: python

   esm_ds.search(file_id='ocean_month') # Searching for a file with the ID 'ocean_month' - this is our recorded call

The other data collected are:

- :code:`timestamp` 
   The time at which the telemetry was recorded.

- :code:`name` 
   A name identifier which may in future be used to connect usage to a particular user. 
   Presently, we do not collect any identifying information about users, such as usernames, and so this
   field is always :code:`unknown`. In future, we may collect fully anonymised identifiers relating to 
   users, but this will be communicated to users in advance.

- :code:`function`
   The function that was called, in this case :code:`.search`. We collect this in 
   order to understand how our users interact with the catalog, and to help us improve it.

- :code:`args`
   The positional arguments that were passed to the function. In this case, there are none.
   This is used in order to, for example, understand which experiments users are searching for.

- :code:`kwargs`
   The keyword arguments that were passed to the function. This allows us to see, for 
   example, how users tend to select a dataset from within a datastore.

- :code:`session_id`
   A unique identifier for the Python session in which the telemetry was recorded. 
   This allows us to understand the series of interactions a user makes with the catalog in a single session.
   Restarting the notebook kernel will generate a new session ID - and so in combination with other data
   we collect, allows us to improve the stability of the catalog and related functionality.

- :code:`catalog_version`
   The version of the catalog that was used to generate the telemetry. This allows us to
   understand whether old versions of the catalog are still being used, for example.

Below is a list of frequently asked questions and accompanying answers:
-----------------------------------------------------------------------

.. topic:: Will telemetry slow down my JupyterLab session?
   
   No. All telemetry happens in a background thread, and if it fails for any reason, the errors will be
   silently ignored. It will not affect your JupyterLab session in any way.

.. topic:: Will telemetry affect my privacy?

   We collect data on the fields specified above. In addition, we do not record all function calls made 
   in an ARE Session, only a specific subset of those that are made on the ACCESS-NRI Intake Catalog 
   functionality. An up to date list of these functions can be found 
   `here <https://github.com/ACCESS-NRI/access-py-telemetry/blob/main/src/access_py_telemetry/config.yaml>`_.

   No data that could identify a specific user is collected. In future, we may begin to collect fully anonymised
   identifiers relating to users, but this will be communicated to users in advance, and will follow 
   industry best practices to ensure that this data not, nor could be used to, identify users.

.. topic:: Can I disable telemetry?

   Yes - but please note, if you do so, we will not be able to differentiate usage of the resources you 
   are using from no usage at all. This may lead to deprioritisation of resources you are interested in. 
   If you are sure you wish to disable telemetry, you can do so by running the following code:

   .. code-block:: python

      from access_py_telemetry.api import ApiHandler
      ApiHandler().server_url = ""

   This will disable telemetry until you restart your JupyterLab session, at which point you will need 
   to disable telemetry again.
   
.. note::
   Any questions or concerns about telemetry on the ACCESS-NRI Intake catalog? Please open an issue
   `here <https://github.com/ACCESS-NRI/access-nri-intake-catalog/issues/new/choose>`_.