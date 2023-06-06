.. how:

How do I use it?
================

The ACCESS-NRI Intake catalog is intended to be used in Jupyter notebooks running on Gadi. It can be used outside of Jupyter notebooks, but interactive user experience will be impacted if HTML output formatting is not supported. 

.. _prerequisites:

Prerequisites
^^^^^^^^^^^^^

In order to use the catalog, you will need to have the following:

#. **An account at NCI**: see the `NCI documentation for creating an account <https://opus.nci.org.au/display/Help/How+to+create+an+NCI+user+account>`_ if you don't have one. Note you will need to join a project with a compute allocation. If you don't know what project is appropriate you will need to seek help from your local group or IT support.

#. **Access to the projects that house the data you're interested in**: the catalog references data products across multiple projects on Gadi.  Currently, data is included from the following projects:

   * ACCESS-CM2 and ACCESS-ESM1.5 data: :code:`p73`
   * ACCESS-OM2 data: :code:`ik11`, :code:`cj50`
   * CMIP5 data: :code:`dk92`, :code:`al33`, :code:`rr3`
   * CMIP6 data: :code:`dk92`, :code:`fs38`, :code:`oi10`

   If you wish to be able to access all the data in the catalog, you will need to be a member of all these projects. See the `NCI documentation for how to join projects <https://opus.nci.org.au/display/Help/How+to+connect+to+a+project>`_.

   .. attention::

      Catalog users will only be able to load data from projects that they have access to.

#. **Access to the** :code:`xp65` **or** :code:`hh5` **projects**: these projects provide public analysis environments in which the ACCESS-NRI catalog is installed (along with many other useful packages). Alternatively, you can install the catalog into your own environment.

   .. attention::
      The ACCESS-NRI catalog is actually not yet installed in the :code:`hh5` environments, so for now you'll have to use the :code:`xp65` environment.

.. _installation:

Installing the catalog
^^^^^^^^^^^^^^^^^^^^^^

Most users will not need to install the catalog themselves and will instead use the catalog through one of the public analysis environments provided in either :code:`xp65` or :code:`hh5` (see below).

Advanced users that want to install the catalog into their own environment can do so in three ways:

============================================ ===========================================
Install method                               Code
============================================ ===========================================
`conda <https://docs.conda.io/en/latest/>`_  .. code-block:: bash

                                                $ conda install -c accessnri access-nri-intake

`pip <https://pypi.org/project/pip/>`_       .. code-block:: bash

                                                $ python -m pip install access-nri-intake

From source                                  .. code-block:: bash

                                                $ git clone git@github.com:ACCESS-NRI/access-nri-intake.git
                                                $ cd access-nri-intake
                                                $ python -m pip install -e .

============================================ ===========================================

.. _are_setup:

Using the catalog on the ARE
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The easiest way to use the catalog is via the NCI Australian Research Environment (ARE). The ARE is a web-based graphical interface that makes it easy for NCI users to start JupyterLab sessions on Gadi compute nodes. NCI documentation for the ARE JupyterLab app can be found `here <https://opus.nci.org.au/display/Help/3.+JupyterLab+App>`_.

After `logging in to the ARE <https://are.nci.org.au/>`_ using your NCI credentials and clicking the JupyterLab icon, you’ll arrive at a page to configure your JupyterLab session. Many of the configuration options are hopefully self-explanatory, but a few must be set carefully to ensure the catalog and the data it references are available. In particular:

* **Setting the storage flags**: in addition to being a member of the projects you want to access, you also have to explicity tell the JupyterLab app that you want to access them in your session. Specify the project storage paths by entering them in the “Storage” dropdown. To allow access to all data products in the catalog enter :code:`gdata/p73+gdata/ik11+gdata/cj50+gdata/dk92+gdata/al33+gdata/rr3+gdata/fs38+gdata/oi10`. If you want to use the :code:`xp65` or :code:`hh5` analysis environment, you'll also need to add :code:`gdata/xp65` or :code:`gdata/hh5`, respectively.

  .. attention::
     You need to be a member of all projects you enter here. You can see what projects you are part of at `https://my.nci.org.au/mancini <https://my.nci.org.au/mancini>`_.

* **Setting the environment**: you need to make sure that the catalog is installed in your JupyterLab session. As mentioned above, the easiest way to do this is to use either the :code:`xp65` or :code:`hh5` public analysis environments. You can activate the :code:`xp65` environment within your JupyterLab session using the "Advanced options" to set the "Module directories" to :code:`/g/data/xp65/public/modules` and "Modules" to :code:`conda/are`. Similarly, to use the `hh5` environment, set "Module directories" to :code:`/g/data/hh5/public/modules` and "Modules" to :code:`conda/analysis3`.
