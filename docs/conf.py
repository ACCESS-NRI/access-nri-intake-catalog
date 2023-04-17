# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import sys

print(sys.executable)

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

html_title = "ACCESS-NRI Intake catalog"
project = "ACCESS-NRI Intake catalog"
copyright = "2023, ACCESS-NRI"
author = "ACCESS-NRI"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

# Add any Sphinx extension module names here, as strings. They can be extensions
# coming with Sphinx (named 'sphinx.ext.*') or your custom ones.
extensions = [
    #    "IPython.sphinxext.ipython_console_highlighting",
    "numpydoc",
    "sphinx.ext.napoleon",
    "autoapi.extension",
    "nbsphinx",
    "sphinx_panels",
    "sphinx_copybutton",
]

# autoapi directives
autoapi_dirs = ["../src/catalog_manager"]
autoapi_add_toctree_entry = False
autoapi_ignore = ["**.ipynb_checkpoints"]
autoapi_python_class_content = "class"

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "**.ipynb_checkpoints"]

# The suffix of source filenames.
source_suffix = ".rst"

# The master toctree document.
master_doc = "index"

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = "sphinx"


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "pydata_sphinx_theme"
html_static_path = []  # ['_static']
html_theme_options = {
    "use_edit_page_button": True,
    "github_url": "https://github.com/ACCESS-NRI/nri_intake_catalog",
}

# If true, "(C) Copyright ..." is shown in the HTML footer. Default is True.
# html_show_copyright = True
copyright = "(C) Copyright 2023 ACCESS-NRI"
