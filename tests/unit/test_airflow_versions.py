"""Tests for bin/airflow_versions.py. No network access (urlopen is patched)."""

import io
import json
import re
import sys
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from tests.system.versions import load_module

av: Any = load_module()


def _pyproject(tmp_path: Path, airflow: str = ">=3.1", sdk: str = ">=15.2.0") -> Path:
    path = tmp_path / "pyproject.toml"
    path.write_text(
        "[project]\n"
        'name = "x"\n'
        f'dependencies = ["apache-airflow{airflow}", "yellowdog-sdk{sdk}"]\n'
    )
    return path


class _Response(io.BytesIO):
    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


def _pypi(version: str, package: str = "apache-airflow") -> Any:
    def fake_urlopen(url: str, timeout: float = 0) -> _Response:
        assert url == f"https://pypi.org/pypi/{package}/json"
        return _Response(json.dumps({"info": {"version": version}}).encode())

    return patch.object(av.urllib.request, "urlopen", side_effect=fake_urlopen)


@pytest.fixture(autouse=True)
def _clear_cache() -> None:
    av.latest_airflow.cache_clear()
    av.latest_sdk.cache_clear()


def test_min_airflow_normalised(tmp_path: Path) -> None:
    assert av.min_airflow(_pyproject(tmp_path)) == "3.1.0"


def test_min_airflow_three_part(tmp_path: Path) -> None:
    assert av.min_airflow(_pyproject(tmp_path, airflow=">=3.2.1")) == "3.2.1"


def test_min_sdk(tmp_path: Path) -> None:
    assert av.min_sdk(_pyproject(tmp_path)) == "15.2.0"


def test_min_from_repo_pyproject() -> None:
    assert re.fullmatch(r"\d+\.\d+\.\d+", av.min_airflow())
    assert re.fullmatch(r"\d+\.\d+\.\d+", av.min_sdk())


def test_missing_floor_raises(tmp_path: Path) -> None:
    with pytest.raises(av.VersionError, match="apache-airflow"):
        av.min_airflow(_pyproject(tmp_path, airflow="==3.1.0"))


@pytest.mark.parametrize("spec", [">=3.1,<4", ">=3.1.0, <4.0", "~=3.1"])
def test_unsupported_floor_format_reported(tmp_path: Path, spec: str) -> None:
    with pytest.raises(av.VersionError) as exc:
        av.min_airflow(_pyproject(tmp_path, airflow=spec))
    message = str(exc.value)
    assert "unsupported" in message.lower()
    assert f"apache-airflow{spec}" in message


def test_module_shared_with_existing_import() -> None:
    assert sys.modules["airflow_versions"] is av
    assert load_module() is av


def test_latest_airflow_from_pypi() -> None:
    with _pypi("3.4.0") as urlopen:
        assert av.latest_airflow() == "3.4.0"
        assert av.latest_airflow() == "3.4.0"
    assert urlopen.call_count == 1  # cached


def test_latest_sdk_from_pypi() -> None:
    with _pypi("15.3.0", package="yellowdog-sdk") as urlopen:
        assert av.latest_sdk() == "15.3.0"
        assert av.latest_sdk() == "15.3.0"
    assert urlopen.call_count == 1  # cached


@pytest.mark.parametrize("latest", ["latest_airflow", "latest_sdk"])
def test_latest_rejects_unexpected_version(latest: str) -> None:
    package = "apache-airflow" if latest == "latest_airflow" else "yellowdog-sdk"
    with (
        _pypi("15.3.0rc1", package=package),
        pytest.raises(av.VersionError, match=r"15\.3\.0rc1"),
    ):
        getattr(av, latest)()


def test_resolve_labels(tmp_path: Path) -> None:
    pyproject = _pyproject(tmp_path)
    with _pypi("3.4.0"):
        assert av.resolve("min", pyproject) == "3.1.0"
        assert av.resolve("max", pyproject) == "3.4.0"
    assert av.resolve("3.2.1", pyproject) == "3.2.1"


@pytest.mark.parametrize("label", ["3.2", "latest", "v3.2.1", "3.2.1rc1", ""])
def test_resolve_rejects_bad_labels(label: str) -> None:
    with pytest.raises(av.VersionError):
        av.resolve(label)


def test_constraints_url() -> None:
    assert av.constraints_url("3.1.0") == (
        "https://raw.githubusercontent.com/apache/airflow/"
        "constraints-3.1.0/constraints-3.12.txt"
    )
    assert av.constraints_url("3.4.0", python="3.14").endswith(
        "constraints-3.4.0/constraints-3.14.txt"
    )


@pytest.mark.parametrize("arg", ["min", "max", "sdk-min"])
def test_cli(arg: str, capsys: pytest.CaptureFixture[str]) -> None:
    expected = {"min": av.min_airflow(), "max": "3.4.0", "sdk-min": av.min_sdk()}
    with _pypi("3.4.0"):
        assert av.main([arg]) == 0
    assert capsys.readouterr().out == expected[arg] + "\n"


def test_cli_rejects_unknown(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        av.main(["nope"])
