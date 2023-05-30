Contributing code
=================

Code contributions are handled through "pull requests" on GitHub. The following describes how to go about making your 
contributions and submitting a pull request.

#. Fork this respository.

#. Clone your fork locally, connect your repository to the upstream (main project), and create a branch to work on::

    $ git clone git@github.com:YOUR_GITHUB_USERNAME/access-nri-intake-catalog.git
    $ cd access-nri-intake-catalog
    $ git remote add upstream git@github.com:ACCESS-NRI/access-nri-intake-catalog.git
    $ git checkout -b YOUR-BUGFIX-FEATURE-BRANCH-NAME main

#. Install `access-nri-intake`'s dependencies into a new conda environment::

    $ conda env create -f environment-dev.yml
    $ conda activate access-nri-intake-dev

#. Install `access-nri-intake` using the editable flag (meaning any changes you make to the package will be reflected 
directly in your environment without having to reinstall)::

    $ pip install --no-deps -e .

#. This project uses `black` to format code and `flake8` for linting. We use `pre-commit` to ensure these have been run. 
Please set up commit hooks by running the following. This will mean that `black` and `flake8` are run whenever you make a commit::

    pre-commit install

You can also run `pre-commit` manually at any point to format your code::

    pre-commit run --all-files

#. Start making and committing your edits, including adding docstrings to functions and adding unit tests to check that 
your contributions are doing what they're suppose to. Please try to follow `numpydoc style 
<https://numpydoc.readthedocs.io/en/latest/format.html>`_ for docstrings. To run the test suite::

    pytest src

#. Once you are happy with your contribution, navigate to `here <https://github.com/ACCESS-NRI/access-nri-intake-catalog/pulls>`_ 
and open a new pull request to merge your branch of your fork with the main branch of the base.

Preparing a new release
-----------------------

New releases to PyPI and conda are published automatically when a tag is pushed to Github. A new release may or may not include 
an update to the catalog files on Gadi and associated 
`data package <https://intake.readthedocs.io/en/latest/data-packages.html>`_ module :code:`access_nri_intake.cat`. If it does, 
the person doing the release must ensure that the version of the new catalog matches the version of the new release by carefully 
following all steps below. Ideally steps 1 and 2 below will be done in a PR and merged before commencing step 3. If the release 
does not include an update to the catalog on Gadi, skip the first two steps below:

#. [OPTIONAL] Create a new version of the catalog on Gadi (this will take about 45 mins)::

    $ export RELEASE=vX.X.X
    $ cd bin
    $ qsub -v version=${RELEASE} build_all.sh
    
#. [OPTIONAL] Upon successful completion of the previous step, the :code:`access_nri_intake` data package module will be updated 
   to point at the new version just created. Commit this update::
   
   $ cd ../
   $ git add src/access_nri_intake/cat
   $ git commit "Update catalog to $RELEASE"

#. Go to https://github.com/ACCESS-NRI/access-nri-intake-catalog

#. Click on "Releases"/"Draft new release" on the right-hand side of the screen

#. Enter the new version (vX.X.X) as the tag and release title. Add a brief description of the release.

#. Click on "Publish release". This should create the release on GitHub and trigger the workflow that builds and uploads 
   the new version to PyPI and conda

Alternatively (any discouraged), one can trigger the new release from the command line. Replace steps 3 onwards with::

    $ git fetch --all --tags
    $ git commit --allow-empty -m "Release $RELEASE"
    $ git tag -a $RELEASE -m "Version $RELEASE"
    $ git push --tags
