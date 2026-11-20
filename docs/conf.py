# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import html
import os
import re
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

project = "YellowDog Provider for Apache Airflow"
copyright = f"{date.today().year}, YellowDog Limited. Version {__version__}"
author = "YellowDog Limited"
release = __version__

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.todo",
    "sphinx.ext.viewcode",
    "sphinx.ext.autodoc",
    "sphinx.ext.extlinks",
    "sphinx_copybutton",
]

# A copy button on every code block. Where a block shows commands after a "$ "
# or "% " prompt, it copies only the commands, without the prompts or the output,
# and a command continued with a trailing backslash whole.
copybutton_prompt_text = r"[%$] "
copybutton_prompt_is_regexp = True
copybutton_line_continuation_char = "\\"

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "superpowers"]

# The repository on GitHub, at this version's tag, or for a version that is no
# release (a development version), 'main'
_GITHUB = "https://github.com/yellowdog/airflow-provider"
_REF = f"v{release}" if re.fullmatch(r"\d+\.\d+\.\d+", release) else "main"

# :repo:`yellowdog_provider/example_dags` links to a file or directory in the
# repository, at this version, shown as its path; GitHub shows a file at a
# 'tree' address too
extlinks = {"repo": (f"{_GITHUB}/tree/{_REF}/%s", "%s")}

# make docs-linkcheck: GitHub and the YellowDog documentation and Portal make a
# page's '#...' anchors in the browser, where the checker cannot see them, so it
# checks those pages and not their anchors
linkcheck_anchors_ignore_for_url = [
    r"https://github\.com/",
    r"https://(docs|portal)\.yellowdog\.ai/",
]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_logo = "_static/yellowdog-wordmark.svg"
html_static_path = ["_static"]
html_css_files = ["custom.css", "provider.css"]
html_title = project
# Where the site is served: each page names its address there (its canonical
# link), so a search engine indexes the site rather than any copy of it
html_baseurl = "https://docs.yellowdog.ai/airflow-provider/"
# The breadcrumbs' 'View on GitHub': each page's source at this version
# (_templates/breadcrumbs.html)
html_context = {"github_source": f"{_GITHUB}/blob/{_REF}/docs/"}


def _overview_sections(app, pagename, templatename, context, doctree) -> None:
    """
    The home page's sections, for the menu to show under Overview while the
    home page is shown (_templates/layout.html), as it shows any other page's
    under its entry: Sphinx expands no entry for the page that holds the menu
    itself.
    """
    if pagename != app.config.root_doc or doctree is None:
        return
    # The home page's address is the site's own, not its index.html
    context["pageurl"] = html_baseurl
    from docutils import nodes

    def items(section, level: int) -> str:
        entries = []
        for child in section.children:
            if not isinstance(child, nodes.section):
                continue
            anchor = child["ids"][0]
            title = html.escape(child[0].astext())
            nested = items(child, level + 1) if level < 3 else ""
            entries.append(
                f'<li class="toctree-l{level}"><a class="reference internal"'
                f' href="#{anchor}">{title}</a>{nested}</li>'
            )
        return f"<ul>{''.join(entries)}</ul>" if entries else ""

    title = next(n for n in doctree.children if isinstance(n, nodes.section))
    context["overview_sections"] = items(title, 2)


def setup(app) -> None:
    app.connect("html-page-context", _overview_sections)
