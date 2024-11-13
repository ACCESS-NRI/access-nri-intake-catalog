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
      If :code:`version` is not provided, the default used is the current date, in the format :code:`vYYYY-MM-DD`. This should 
      be acceptable in most cases.
    
#. Updating :code:`access_nri_intake_catalog` is no longer necessary - the new catalog will be available immediately as 
   :code:`intake.cat.access_nri`.
