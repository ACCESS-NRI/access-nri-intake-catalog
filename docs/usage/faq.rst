.. _faq:

FAQs
====

Below is a list of frequently asked questions and accompanying answers:

.. topic:: Why do I get a :code:`FileNotFound` error when I try to load data from a datastore?
   
   This is usually because you didn't include the storage flags for these data when you started your 
   JupyterLab session. Please make sure that you're a member of the projects that house the data you 
   want to access (see :ref:`prerequisites`) and that you've set your storage flags correctly 
   (see :ref:`are_setup`).

.. topic:: Can other data products be added to the catalog?

   Yes! If you know of a climate data product on Gadi that should be included in the catalog, please 
   open an issue 
   `here <https://github.com/ACCESS-NRI/access-nri-intake-catalog/issues/new/choose>`_ describing the 
   product.

.. topic:: Can I use an old, or custom, version of the catalog?

   Yes! There's a couple of ways you can do this:

   - If you simply want to use an older version of the catalog, you can specify the version number when 
     you load the catalog: :code:`cat = intake.cat.access_nri(version="vYYYY-MM-DD")`.

     Note that versions of the catalog loaded in this way must be compatible with the current 'live' 
     catalog in terms of structure. If they're not, you will need to find the relevant catalog file
     on Gadi in :code:`xp65`, and place that in your home area (see below);
   - If you want to use a custom catalog (perhaps an old catalog structure, or a catalog that you've built yourself), 
     you can place the :code:`catalog.yaml` file into your home directory at :code:`~/.access_nri_intake_catalog/catalog.yaml`. If this
     file exists, :code:`intake.cat.access_nri` will load that catalog in preference to the 'live' one.
     This is the best way to give yourself access to catalog versions that are no longer compatible with 
     the 'live' catalog: see :ref:`versioning`.

.. note::
   Need help? Please open an issue 
   `here <https://github.com/ACCESS-NRI/access-nri-intake-catalog/issues/new/choose>`_.