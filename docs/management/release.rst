.. _release:

Releases
########

Preparing a new code release
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

New releases of access-nri-intake to PyPI and conda are published automatically when a tag is pushed to Github. A new release may 
or may not include an update to the ACCESS-NRI catalog files on Gadi.

#. Go to https://github.com/ACCESS-NRI/access-nri-intake-catalog

#. Click on "Releases"->"Draft new release" on the right-hand side of the screen

#. Enter the new version (vX.X.X) as the tag and release title. Add a brief description of the release.

   .. note::

      It is recommended to attempt a beta release before committing to a major code update.
      In this case, the version number requires an ordinal after the :code:`b`, e.g., :code:`v1.2.3b0`. If the
      ordinal isn't provided, the GitHub PyPI build action will append one, which breaks the linkage
      between the PyPI and Conda build actions.

#. Click on "Publish release". This should create the release on GitHub and trigger the workflow that builds and uploads 
   the new version to PyPI and conda

Alternatively (though discouraged), one can trigger the new release from the command line::

    $ export RELEASE=vX.X.X
    $ git fetch --all --tags
    $ git commit --allow-empty -m "Release $RELEASE"
    $ git tag -a $RELEASE -m "Version $RELEASE"
    $ git push --tags

Generating a new catalog version
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

#. Create a new version of the catalog on Gadi (this will take about 1 hour)::

     $ export RELEASE=vYYYY-MM-DD
     $ cd bin
     $ qsub -v version=${RELEASE} build_all.sh

   .. note::
      Running the build script requires access to an up-to-date checkout of the :code:`access-nri-intake-catalog`
      repository. The default location for this is :code:`/g/data/xp65/admin/access-nri-intake-catalog`. If you do 
      not have the ability to update this checkout, you may use a local one; however, you will need to update
      the :code:`CONFIG_DIR` variable in :code:`bin/build_all.sh` to point at your checkout location.

   .. note:: 
      If :code:`version` is not provided, the default used is the current date, in the format :code:`vYYYY-MM-DD`. This should 
      be acceptable in most cases.
    
#. Updating :code:`access_nri_intake_catalog` is no longer necessary - the new catalog will be available immediately as 
   :code:`intake.cat.access_nri`.

#. Run the Jupyter notebook ``bin/new-build-checks.ipynb``. This confirms the catalog versions that are available, and runs a 
   comparison between the new catalog and a selected previous catalog for additions, deletions, etc. Verify that there are 
   no unexpected changes in the catalog composition.


New release with new catalog
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In the case of a linked release of a new major :code:`access-nri-intake-catalog` and a new catalog 
build, the recommened process is:

#. Create a beta release of :code:`access-nri-intake-catalog`;
#. Use the beta release to build a new catalog;
#. Iterate over the above steps until the desired result is achieved;
#. Make a definitive code release.