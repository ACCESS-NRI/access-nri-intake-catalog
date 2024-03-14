.. _release:

Preparing a new release
^^^^^^^^^^^^^^^^^^^^^^^

New releases of access-nri-intake to PyPI and conda are published automatically when a tag is pushed to Github. A new release may 
or may not include an update to the ACCESS-NRI catalog files on Gadi and associated 
`data sub-package <https://intake.readthedocs.io/en/latest/data-packages.html>`_ :code:`access_nri_intake.data`. If it does, the 
person doing the release must ensure that the version of the new catalog matches the version of the new release by carefully 
following all steps below. Steps 1 and 2 below should be done in a PR and merged before commencing step 3. If the release does 
not include an update to the catalog on Gadi, skip the first two steps below:

#. [IF UPDATING THE CATALOG] Create a new version of the catalog on Gadi (this will take about 1 hour)::

     $ export RELEASE=vX.X.X
     $ cd bin
     $ qsub -v version=${RELEASE} build_all.sh

   .. note:: 
      If the `schema <https://github.com/ACCESS-NRI/schema>`_ has changed, or you have not used the intake catalog recently, this step may fail with a *Network is unreachable* error trying to download the schema json files. To download and cache the schema, first import the :code:`access_nri_intake.source` and :code:`access_nri_intake.catalog` sub-packages from a Gadi node with network access (e.g. a login or ARE node). I.e., using the release version of :code:`access_nri_intake`::

        $ python3 -c "from access_nri_intake import source, catalog"
      
      This will cache a copy of the schema in your home directory. Then re-run ``$ qsub -v version=${RELEASE} build_all.sh``
    
#. [IF UPDATING THE CATALOG] Upon successful completion of the previous step, the :code:`access_nri_intake` data package module 
   will be updated to point at the new version just created. Commit this update::
   
      $ cd ../
      $ git add src/access_nri_intake/cat
      $ git commit "Update catalog to $RELEASE"

#. Go to https://github.com/ACCESS-NRI/access-nri-intake-catalog

#. Click on "Releases"->"Draft new release" on the right-hand side of the screen

#. Enter the new version (vX.X.X) as the tag and release title. Add a brief description of the release.

#. Click on "Publish release". This should create the release on GitHub and trigger the workflow that builds and uploads 
   the new version to PyPI and conda

Alternatively (though discouraged), one can trigger the new release from the command line. Replace steps 3 onwards with::

    $ git fetch --all --tags
    $ git commit --allow-empty -m "Release $RELEASE"
    $ git tag -a $RELEASE -m "Version $RELEASE"
    $ git push --tags
