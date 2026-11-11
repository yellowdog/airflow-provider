#!/usr/bin/env python3
"""The lowest and highest supported Apache Airflow versions (stdlib only).

Single source of truth for CI, the Makefile and the system tests:

- ``min``: the ``apache-airflow>=`` floor in ``pyproject.toml``, as ``X.Y.Z``
- ``max``: the latest ``apache-airflow`` release on PyPI
- ``sdk-min``: the ``yellowdog-sdk>=`` floor in ``pyproject.toml``

Usage: ``python bin/airflow_versions.py min|max|sdk-min`` prints the value.
"""

import argparse
import functools
import json
import re
import sys
import tomllib
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = ROOT / "pyproject.toml"
PYPI_AIRFLOW_URL = "https://pypi.org/pypi/apache-airflow/json"
PYPI_SDK_URL = "https://pypi.org/pypi/yellowdog-sdk/json"
CONSTRAINTS_URL = (
    "https://raw.githubusercontent.com/apache/airflow/"
    "constraints-{version}/constraints-{python}.txt"
)
DEFAULT_PYTHON = "3.12"
LABELS = ("min", "max")
_VERSION_RE = re.compile(r"\d+\.\d+\.\d+")


class VersionError(Exception):
    pass


def _normalise(version: str) -> str:
    """Pad a ``X`` or ``X.Y`` version to ``X.Y.Z``."""
    parts = version.split(".")
    return ".".join(parts + ["0"] * (3 - len(parts)))


def _floor(package: str, pyproject: Path) -> str:
    with pyproject.open("rb") as f:
        dependencies: list[str] = tomllib.load(f)["project"]["dependencies"]
    pattern = re.compile(rf"{re.escape(package)}\s*>=\s*(\d+(?:\.\d+){{0,2}})\s*$")
    # The package name, not a longer name it prefixes (e.g. apache-airflow-x)
    name = re.compile(rf"{re.escape(package)}(?![\w.-])")
    for dependency in dependencies:
        match = pattern.fullmatch(dependency.strip())
        if match:
            return _normalise(match.group(1))
        if name.match(dependency.strip()):
            raise VersionError(
                f"Unsupported dependency format {dependency!r} in {pyproject}: "
                f"only a plain '{package}>=X.Y[.Z]' floor is supported"
            )
    raise VersionError(f"No '{package}>=X.Y[.Z]' dependency in {pyproject}")


def min_airflow(pyproject: Path = PYPROJECT) -> str:
    return _floor("apache-airflow", pyproject)


def min_sdk(pyproject: Path = PYPROJECT) -> str:
    return _floor("yellowdog-sdk", pyproject)


def _latest(url: str, what: str) -> str:
    with urllib.request.urlopen(url, timeout=30) as response:
        version = str(json.load(response)["info"]["version"])
    if not _VERSION_RE.fullmatch(version):
        raise VersionError(f"Unexpected latest {what} version on PyPI: {version!r}")
    return version


@functools.cache
def latest_airflow() -> str:
    return _latest(PYPI_AIRFLOW_URL, "Airflow")


@functools.cache
def latest_sdk() -> str:
    """The latest ``yellowdog-sdk`` release on PyPI."""
    return _latest(PYPI_SDK_URL, "yellowdog-sdk")


def is_label(label: str) -> bool:
    """True for ``min``, ``max`` or an explicit ``X.Y.Z`` version."""
    return label in LABELS or _VERSION_RE.fullmatch(label) is not None


def resolve(label: str, pyproject: Path = PYPROJECT) -> str:
    """Resolve ``min``, ``max`` or an explicit ``X.Y.Z`` to an Airflow version."""
    if label == "min":
        return min_airflow(pyproject)
    if label == "max":
        return latest_airflow()
    if _VERSION_RE.fullmatch(label):
        return label
    raise VersionError(
        f"Invalid Airflow version label {label!r}: use 'min', 'max' or X.Y.Z"
    )


def constraints_url(airflow_version: str, python: str = DEFAULT_PYTHON) -> str:
    return CONSTRAINTS_URL.format(version=airflow_version, python=python)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("which", choices=["min", "max", "sdk-min"])
    args = parser.parse_args(argv)
    print(min_sdk() if args.which == "sdk-min" else resolve(args.which))
    return 0


if __name__ == "__main__":
    sys.exit(main())
