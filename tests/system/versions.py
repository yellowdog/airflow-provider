"""Typed access to ``bin/airflow_versions.py`` (the supported Airflow versions)."""

import importlib.util
import sys
from pathlib import Path
from typing import Any


def load_module() -> Any:
    """``bin/airflow_versions.py``, loaded once and shared via ``sys.modules``
    (``bin/systest_env.py`` imports it by the same name)."""
    name = "airflow_versions"
    if name in sys.modules:
        return sys.modules[name]
    path = Path(__file__).resolve().parents[2] / "bin" / "airflow_versions.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


_av = load_module()

VersionError: type[Exception] = _av.VersionError


def is_label(label: str) -> bool:
    return bool(_av.is_label(label))


def resolve(label: str) -> str:
    return str(_av.resolve(label))


def constraints_url(airflow_version: str) -> str:
    return str(_av.constraints_url(airflow_version))
