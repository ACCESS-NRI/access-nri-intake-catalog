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

#. Make your code changes, and test them. The easiest way to do this is to run::

      pixi run test
   
   This will allow you to select which python version you want to run your tests against, and will
   handle all dependency management for you. If you wish to alter the test execution (eg. to increase verbosity or alter 
   flags), you can do::
      
      pixi shell
   
   and then interact with tests directly - as if you were in a conda or virtual environment. :code:`pixi shell` will drop 
   you into a subshell in which pixi has set up your virtual environment. Alternatively, you can do::

      pixi run $CMD

#. This project uses :code:`ruff` for linting and :code:`black` to format code . We use :code:`pre-commit` to ensure these 
   have been run. We also use :code:`mypy` for type checking. Please set up commit hooks by running the following. This will
   mean that :code:`ruff` and :code:`black` are run whenever you make a commit::

      pixi run pre-commit-install

   You can also run :code:`pre-commit` manually at any point to format your code::

      pixi run pre-commit

   In addition, you can use any of the following commands to run the various linters and type checkers manually::

      pixi run ruff
      pixi run black
      pixi run mypy

   :code:`mypy` will require you to run :code:`pixi run mypy-setup` first.

#. Continue making and committing your edits, including adding docstrings to functions, updating the documentation where 
   appropriate, and adding unit tests to check that your contributions are doing what they're suppose to. Please try to 
   follow `numpydoc style <https://numpydoc.readthedocs.io/en/latest/format.html>`_ for docstrings. To run the test suite::

      pixi run test

   This project has both unit tests and integration tests. Integration tests are disabled by default due to computational
   expense, and can only be run on Gadi. To run the full test suite, including integration tests, run::

      pixi run test-e2e

#. Once you are happy with your contribution, go `here <https://github.com/ACCESS-NRI/access-nri-intake-catalog/pulls>`_ 
   and open a new pull request to merge your branch of your fork with the main branch of the base.
