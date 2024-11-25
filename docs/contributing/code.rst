.. _code:

Contributing code
=================

Documentation of the code that supports the ACCESS-NRI catalog and how it fits together can be found in the 
:ref:`management` section. Contributions to code are welcome. 

If you'd like to contribute code, please start by opening an issue 
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

#. Install access-nri-intake's dependencies into a new conda environment::

      $ conda env create -f ci/environment-3.11.yml
      $ conda activate access-nri-intake-test

#. Install access-nri-intake using the editable flag (meaning any changes you make to the package will be 
   reflected directly in your environment without having to reinstall)::

      $ pip install --no-deps -e .

#. This project uses :code:`ruff` for linting and :code:`black` to format code . We use :code:`pre-commit` to ensure these 
   have been run. Please set up commit hooks by running the following. This will mean that :code:`ruff` and :code:`black` 
   are run whenever you make a commit::

      pre-commit install

   You can also run :code:`pre-commit` manually at any point to format your code::

      pre-commit run --all-files

#. Start making and committing your edits, including adding docstrings to functions, updating the documentation where 
   appropriate, and adding unit tests to check that your contributions are doing what they're suppose to. Please try to 
   follow `numpydoc style <https://numpydoc.readthedocs.io/en/latest/format.html>`_ for docstrings. To run the test suite::

      pytest .

   This project has both unit tests and integration tests. Integration tests are disabled by default due to computational
   expense, and can only be run on Gadi. To run the full test suite, including integration tests, run::

      pytest --e2e .

#. Once you are happy with your contribution, go `here <https://github.com/ACCESS-NRI/access-nri-intake-catalog/pulls>`_ 
   and open a new pull request to merge your branch of your fork with the main branch of the base.
