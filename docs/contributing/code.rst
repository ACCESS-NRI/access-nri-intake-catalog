.. _code:

Contributing code
=================

Contributions to code are welcome. Please start by opening an issue 
`here <https://github.com/ACCESS-NRI/access-nri-intake-catalog/issues/new/choose>`_ describing what you would like
to contribute and why. Repo maintainers might discuss the issue with you a little and then invite you to submit a 
"pull request".

Submitting pull requests
^^^^^^^^^^^^^^^^^^^^^^^^

Code contributions are handled through "pull requests" on GitHub. The following describes how to go about making your 
contributions and submitting a pull request.

#. Fork this respository.

#. Clone your fork locally, connect your repository to the upstream (main project), and create a branch to work on. It's
   good practice to include the issue number of the issue that motivated your pull request at the start of your branch 
   name::

      $ git clone git@github.com:YOUR_GITHUB_USERNAME/access-nri-intake-catalog.git
      $ cd access-nri-intake-catalog
      $ git remote add upstream git@github.com:ACCESS-NRI/access-nri-intake-catalog.git
      $ git checkout -b <issue#_description> main

   .. note::

      The above assumes that you have 
      `ssh keys set up to access GitHub <https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent>`_. 
      If you don't, replace :code:`git@github.com:` with :code:`https://github.com/`.

#. Install :code:`access-nri-intake`'s dependencies into a new conda environment::

      $ conda env create -f environment-dev.yml
      $ conda activate access-nri-intake-dev

#. Install :code:`access-nri-intake` using the editable flag (meaning any changes you make to the package will be 
   reflected directly in your environment without having to reinstall)::

      $ pip install --no-deps -e .

#. This project uses :code:`ruff` for linting and :code:`black` to format code . We use :code:`pre-commit` to ensure these 
   have been run. Please set up commit hooks by running the following. This will mean that :code:`ruff` and :code:`black` 
   are run whenever you make a commit::

      pre-commit install

   You can also run :code:`pre-commit` manually at any point to format your code::

      pre-commit run --all-files

#. Start making and committing your edits, including adding docstrings to functions and adding unit tests to check that 
   your contributions are doing what they're suppose to. Please try to follow `numpydoc style 
   <https://numpydoc.readthedocs.io/en/latest/format.html>`_ for docstrings. To run the test suite::

      pytest src

   .. attention::

      pytest is not actually set up yet on this project, but it's coming soon.

#. Once you are happy with your contribution, go `here <https://github.com/ACCESS-NRI/access-nri-intake-catalog/pulls>`_ 
   and open a new pull request to merge your branch of your fork with the main branch of the base.

Preparing a new release
-----------------------

New releases to PyPI and conda are published automatically when a tag is pushed to Github. A new release may or may not include 
an update to the catalog files on Gadi and associated 
`data package <https://intake.readthedocs.io/en/latest/data-packages.html>`_ module :code:`access_nri_intake.data`. If it does, 
the person doing the release must ensure that the version of the new catalog matches the version of the new release by carefully 
following all steps below. Steps 1 and 2 below should be done in a PR and merged before commencing step 3. If the release does 
not include an update to the catalog on Gadi, skip the first two steps below:

#. [IF UPDATING THE CATALOG] Create a new version of the catalog on Gadi (this will take about 1 hour)::

      $ export RELEASE=vX.X.X
      $ cd bin
      $ qsub -v version=${RELEASE} build_all.sh
    
#. [IF UPDATING THE CATALOG] Upon successful completion of the previous step, the :code:`access_nri_intake` data package module 
   will be updated to point at the new version just created. Commit this update::
   
      $ cd ../
      $ git add src/access_nri_intake/cat
      $ git commit "Update catalog to $RELEASE"

#. Go to https://github.com/ACCESS-NRI/access-nri-intake-catalog

#. Click on "Releases"/"Draft new release" on the right-hand side of the screen

#. Enter the new version (vX.X.X) as the tag and release title. Add a brief description of the release.

#. Click on "Publish release". This should create the release on GitHub and trigger the workflow that builds and uploads 
   the new version to PyPI and conda

Alternatively (though discouraged), one can trigger the new release from the command line. Replace steps 3 onwards with::

    $ git fetch --all --tags
    $ git commit --allow-empty -m "Release $RELEASE"
    $ git tag -a $RELEASE -m "Version $RELEASE"
    $ git push --tags
