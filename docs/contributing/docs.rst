.. _docs:

Contributing documentation
==========================

Adding documentation is always helpful. This may include:

* Extending or clarifying existing documentation. Have you perhaps found something unclear?
* Adding examples of the ACCESS-NRI catalog being used for real analyses
* Improving docstrings e.g. by adding examples

This documentation is written in reStructuredText. You can follow the conventions in already written documents. Some 
helpful guides can be found `here <https://docutils.sourceforge.io/docs/user/rst/quickref.html>`_ and 
`here <https://github.com/ralsina/rst-cheatsheet/blob/master/rst-cheatsheet.rst>`_.

Contributions to documentation should be submitted via pull requests to the access-nri-intake 
`core repository <https://github.com/ACCESS-NRI/access-nri-intake-catalog>`_. Follow the steps in the :ref:`code` 
section, replacing step 3 with the following::

   $ conda env create -f docs/environment-doc.yml
   $ conda activate access-nri-intake-doc

The documentation is built and uploaded to readthedocs automatically when changes are pushed to GitHub. When writing 
and editing documentation, it can be useful to see the resulting build without having to push to Github. You can build 
the documentation locally by running::

   $ cd docs/
   $ make html

This will build the documentation locally in :code:`doc/_build/`. You can then open :code:`_build/html/index.html` in 
your web browser to view the documentation.