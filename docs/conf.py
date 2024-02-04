# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "Envirodata"
copyright = "2024, Christoph Knote"
author = "Christoph Knote"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ["sphinx.ext.autodoc", "enum_tools.autoenum", "sphinx.ext.intersphinx"]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# use class header doc as well as __init__ for parameters
# (https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html#confval-autoclass_content)
# autoclass_content = "both"

# do not resolve constants in function arguments
autodoc_preserve_defaults = True


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "classic"
html_static_path = ["_static"]
# -- intersphinx --

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
    "confuse": ("https://confuse.readthedocs.io/en/latest/", None),
}
