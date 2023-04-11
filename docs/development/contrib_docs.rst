Contributing documentation
==========================

Adding documentation is always helpful. This may include:

* Extending or clarifying existing documentation. Have you perhaps found something unclear?
* Adding examples of the ACCESS-NRI catalog being used for real analyses

This documentation is written in reStructuredText. You can follow the conventions in already
written documents. Some helpful guides can be found
`here <https://docutils.sourceforge.io/docs/user/rst/quickref.html>`_
and `here <https://github.com/ralsina/rst-cheatsheet/blob/master/rst-cheatsheet.rst>`_.

The documentation is built and uploaded to readthedocs automatically when changes are pushed
to GitHub. It can be useful to see the resulting build without having to push to GitHub. You
can build the documentation locally by running:

.. code-block:: bash

    $ conda env create -f docs/environment-doc.yml
    $ conda activate catalog-manager-doc
    $ pip install --no-deps -e .
    $ cd docs/
    $ make html

This will build the documentation locally in ``doc/_build/``. You can then open
``_build/html/index.html`` in your web browser to view the documentation. For
example, if you have ``xdg-open`` installed:

.. code-block:: 

    $ xdg-open _build/html/index.html

