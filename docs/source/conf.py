import os

_version_path = os.path.join(os.path.dirname(__file__), '..', '..', 'VERSION')
with open(_version_path) as _f:
    version = release = _f.read().strip()

# -- Project information -----------------------------------------------------

project = 'redup.python.servicekit'
copyright = '2026, REDUP'
author = 'REDUP'

master_doc = 'index'

# -- General configuration ---------------------------------------------------

extensions = [
    'sphinx.ext.autodoc', 'sphinx.ext.doctest', 'sphinx.ext.intersphinx',
    'sphinx.ext.todo', 'sphinx.ext.ifconfig', 'sphinx.ext.viewcode',
    'sphinx.ext.inheritance_diagram', 'sphinx.ext.autosummary',
    'sphinx.ext.mathjax', 'sphinx_rtd_theme', 'm2r2',
]

autodoc_mock_imports = [
    'yaml',
    'pythonjsonlogger',
    'prometheus_client',
    'grpc_health',
    'grpc',
]

add_module_names = False

autodoc_default_options = {
    'members': True,
    'member-order': 'bysource',
    'special-members': '__init__',
    'undoc-members': True,
    'show-inheritance': True,
}

autodoc_typehints = 'none'

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

html_extra_path = []

html_theme_options = {
    "collapse_navigation": False,
    "navigation_depth": 2
}

html_context = {
    "display_github": True,
    "github_user": "redup-ai",
    "github_repo": "redup.python.servicekit",
    "github_version": "master",
    "conf_py_path": "/docs/source/",
}


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_rtd_theme'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = []