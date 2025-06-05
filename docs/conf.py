# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import sys
from datetime import datetime

print(sys.executable)

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
project = "ACCESS-NRI Intake catalog"
author = "ACCESS-NRI"
copyright = f"{datetime.now().year}, {author}"


# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

# Add any Sphinx extension module names here, as strings. They can be extensions
# coming with Sphinx (named 'sphinx.ext.*') or your custom ones.
extensions = [
    #    "IPython.sphinxext.ipython_console_highlighting",
    "numpydoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "autoapi.extension",
    "myst_nb",
    "sphinx_design",
    "sphinx_copybutton",
]

autosummary_generate = False
autodoc_typehints = "none"
autodoc_member_order = "groupwise"

# Config numpydoc
numpydoc_show_class_members = True
numpydoc_show_inherited_class_members = True
numpydoc_class_members_toctree = False

# autoapi directives
autoapi_dirs = ["../src/access_nri_intake"]
autoapi_add_toctree_entry = False
autoapi_ignore = ["*/.ipynb_checkpoints", "*/_version.py"]
autoapi_python_class_content = "both"
autoapi_options = [
    "members",
    "inherited-members",
    "show-inheritance",
    "show-module-summary",
    "undoc-members",  # workaround for https://github.com/readthedocs/sphinx-autoapi/issues/448
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "**.ipynb_checkpoints"]

# The master toctree document.
master_doc = "index"

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = "sphinx"

# Config myst-nb
nb_execution_excludepatterns = [
    "quickstart.ipynb",
    "chunking.ipynb",
]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_book_theme"
html_static_path = ["_static"]
html_context = {
    "github_user": "ACCESS-NRI",
    "github_repo": "access-nri-intake-catalog",
    "github_version": "main",
    "doc_path": "./docs",
}
html_theme_options = {
    "use_edit_page_button": True,
    "github_url": "https://github.com/ACCESS-NRI/access-nri-intake-catalog",
    "logo": {
        "image_light": "_static/accessnri_light.png",
        "image_dark": "_static/accessnri_dark.png",
    },
}
