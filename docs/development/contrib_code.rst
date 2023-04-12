Contributing code
=================

Code contributions are handled through "pull requests" on GitHub. The following describes how to go about making your contributions and submitting a pull request.

#. Fork this respository.

#. Clone your fork locally, connect your repository to the upstream (main project), and create a branch to work on::

    $ git clone git@github.com:YOUR_GITHUB_USERNAME/nri_intake_catalog.git
    $ cd nri_intake_catalog
    $ git remote add upstream git@github.com:ACCESS-NRI/nri_intake_catalog.git
    $ git checkout -b YOUR-BUGFIX-FEATURE-BRANCH-NAME main

#. Install `catalog_manager`'s dependencies into a new conda environment::

    $ conda env create -f environment-dev.yml
    $ conda activate catalog-manager-dev

#. Install `catalog_manager` using the editable flag (meaning any changes you make to the package will be reflected directly in your environment without having to reinstall)::

    $ pip install --no-deps -e .

#. This project uses `black` to format code and `flake8` for linting. We use `pre-commit` to ensure these have been run. Please set up commit hooks by running the following. This will mean that `black` and `flake8` are run whenever you make a commit::

    pre-commit install

You can also run `pre-commit` manually at any point to format your code::

    pre-commit run --all-files

#. Start making and committing your edits, including adding docstrings to functions and adding unit tests to check that your contributions are doing what they're suppose to. Please try to follow `numpydoc style <https://numpydoc.readthedocs.io/en/latest/format.html>`_ for docstrings. To run the test suite::

    pytest src

#. Once you are happy with your contribution, navigate to `here <https://github.com/ACCESS-NRI/nri_intake_catalog/pulls>`_ and open a new pull request to merge your branch of your fork with the main branch of the base.
