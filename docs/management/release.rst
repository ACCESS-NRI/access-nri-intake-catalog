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

#. Create a new version of the catalog on Gadi (this will take about 2 hours)::

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
   
   .. note::
      If you wish to perform a new catalog build without updating the default catalog version, you can use the :code:`--no-concrete` 
      flag. This will create and save a new catalog version, but leave it in a folder named :code:`.$VERSION` in the specified catalog
      build location. To subsequently concretize this build, you can use the :code:`catalog-concretize` command. Instructions for how
      to concretize the build will be available in the output of the build script.
    
#. Updating :code:`access_nri_intake_catalog` is no longer necessary - the new catalog will be available immediately as 
   :code:`intake.cat.access_nri`.

#. Run the Jupyter notebook ``bin/new-build-checks.ipynb``. This confirms the catalog versions that are available, and runs a 
   comparison between the new catalog and a selected previous catalog for additions, deletions, etc. Verify that there are 
   no unexpected changes in the catalog composition.


Concretizing new catalog builds
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
If you have built a new catalog without concretizing it (either by using the :code:`--no-concrete` flag or due to
releasing two new catalog builds with the same version number), you can concretize the new catalog build by 
running the :code:`catalog-concretize` command. This will concretize the specified catalog verison:

   $ catalog-concretize --help 
   usage: catalog-concretize [-h] [--build_base_path BUILD_BASE_PATH] [--version VERSION] [--catalog_file CATALOG_FILE]
                          [--catalog_base_path CATALOG_BASE_PATH] [--no_update] [--force]

Concretize a build by moving it to the final location and updating the paths in the catalog.json files.

options:
  -h, --help            show this help message and exit
  --build_base_path BUILD_BASE_PATH
                        The base path for the build.
  --version VERSION     The version of the build.
  --catalog_file CATALOG_FILE
                        The name of the catalog file.
  --catalog_base_path CATALOG_BASE_PATH
                        The base path for the catalog. If None, the catalog_base_path will be set to the build_base_path.
                        Defaults to None.
  --no_update           Set this if you don't want to update the catalog.yaml file. Defaults to False. If False, the
                        catalog.yaml file will be updated.
  --force               Force the concretization of the build, even if a version of the catalog with the specified version
                        number already exists in the catalog_base_path. Defaults to False.

Running :code:`catalog-build` with the :code:`--no_concretize` flag will return a specification of how to concretize the build in 
its output. Similarly, whilst attempting to concretize a build with a previously existing version number will fail (unless 
:code:`--force` is set), the error message will contain the correct command to concretize the build.


New release with new catalog
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In the case of a linked release of a new major :code:`access-nri-intake-catalog` and a new catalog 
build, the recommened process is:

#. Create a beta release of :code:`access-nri-intake-catalog`;
#. Use the beta release to build a new catalog;
#. Iterate over the above steps until the desired result is achieved;
#. Make a definitive code release.