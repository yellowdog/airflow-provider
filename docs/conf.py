# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import warnings
from datetime import date

warnings.filterwarnings("ignore", category=FutureWarning, module=r"airflow")
warnings.filterwarnings("ignore", category=DeprecationWarning, module=r"airflow")

# Sphinx resets Python warning filters before running autodoc, so filter-based
# suppression is unreliable for warnings triggered during module import.
# Instead, supply the new Airflow 3.x config values via environment variables so
# Airflow never falls back to the deprecated airflow.cfg keys and never issues
# these warnings in the first place.
os.environ.setdefault("AIRFLOW__API__SECRET_KEY", "docs-build")
os.environ.setdefault("AIRFLOW__API__SSL_CERT", "")
os.environ.setdefault("AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL", "300")
os.environ.setdefault("AIRFLOW__METRICS__TIMER_UNIT_CONSISTENCY", "True")

from yellowdog_provider import __version__

project = f"YellowDog Provider for Apache Airflow"
copyright = f"{date.today().year}, YellowDog Limited. Version {__version__}"
author = "YellowDog Limited"
release = __version__

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ["sphinx.ext.todo", "sphinx.ext.viewcode", "sphinx.ext.autodoc"]

autodoc_mock_imports = ["airflow"]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_logo = "_static/yellowdog-wordmark.svg"
html_static_path = ["_static"]
html_css_files = ["custom.css"]
