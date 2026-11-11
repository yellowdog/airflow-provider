"""Tests for bin/systest_env.py. No real subprocesses or network access."""

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import patch

import pytest


def _load() -> ModuleType:
    path = Path(__file__).parents[2] / "bin" / "systest_env.py"
    spec = importlib.util.spec_from_file_location("systest_env", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["systest_env"] = module
    spec.loader.exec_module(module)
    return module


se: Any = _load()

VERSIONS = {
    "apache-airflow": "3.1.0",
    "apache-airflow-task-sdk": "1.1.0",
    "yellowdog-sdk": "15.2.0",
    "urllib3": "2.5.0",
}


class FakeRunner:
    def __init__(self) -> None:
        self.commands: list[list[str]] = []

    def __call__(self, cmd: list[str]) -> str:
        self.commands.append(cmd)
        if cmd[:2] == ["uv", "venv"]:
            (Path(cmd[-1]) / "bin").mkdir(parents=True, exist_ok=True)
            (Path(cmd[-1]) / "bin" / "python").touch()
        return json.dumps(VERSIONS) if "-c" in cmd else ""


@pytest.fixture(autouse=True)
def latest_sdk() -> Any:
    """No PyPI access: the latest yellowdog-sdk is 15.2.0 unless a test changes it."""
    with patch.object(se.airflow_versions, "latest_sdk", return_value="15.2.0") as m:
        yield m


@pytest.fixture
def root(tmp_path: Path) -> Path:
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "x"\n'
        'dependencies = ["apache-airflow>=3.1", "yellowdog-sdk>=15.2.0"]\n'
    )
    return tmp_path


def test_min_spec_has_no_sdk_pin(root: Path) -> None:
    spec = se.install_spec("min", root)
    assert spec["packages"] == ["apache-airflow==3.1.0", "-e", ".[dev]"]
    assert spec["constraints"].endswith("constraints-3.1.0/constraints-3.12.txt")


def test_max_spec_has_no_sdk_pin(root: Path) -> None:
    with patch.object(se.airflow_versions, "latest_airflow", return_value="3.4.0"):
        spec = se.install_spec("max", root)
    assert spec["packages"] == ["apache-airflow==3.4.0", "-e", ".[dev]"]
    assert spec["constraints"].endswith("constraints-3.4.0/constraints-3.12.txt")


def test_creates_venv_then_skips_when_unchanged(root: Path) -> None:
    run = FakeRunner()
    venv = se.ensure_env("min", run, root)
    assert venv == root / ".venv-systest-min"
    assert run.commands[0][:3] == ["uv", "venv", "--clear"]
    install = run.commands[1]
    assert install[:3] == ["uv", "pip", "install"]
    assert install[-2] == "--constraint"
    stamp = json.loads((venv / se.STAMP_NAME).read_text())
    assert stamp["versions"] == VERSIONS

    again = FakeRunner()
    assert se.ensure_env("min", again, root) == venv
    assert again.commands == []


def test_reinstalls_when_interpreter_missing(root: Path) -> None:
    venv = se.ensure_env("min", FakeRunner(), root)
    (venv / "bin" / "python").unlink()
    run = FakeRunner()
    se.ensure_env("min", run, root)
    assert run.commands[0][:3] == ["uv", "venv", "--clear"]


def test_reinstalls_when_pyproject_changes(root: Path) -> None:
    se.ensure_env("min", FakeRunner(), root)
    (root / "pyproject.toml").write_text(
        '[project]\nname = "x"\n'
        'dependencies = ["apache-airflow>=3.2", "yellowdog-sdk>=15.2.0"]\n'
    )
    run = FakeRunner()
    se.ensure_env("min", run, root)
    assert "apache-airflow==3.2.0" in run.commands[1]


def test_spec_records_latest_sdk(root: Path) -> None:
    spec = se.install_spec("min", root)
    assert spec["yellowdog_sdk"] == "15.2.0"
    assert not any("yellowdog-sdk" in p for p in spec["packages"])


def test_reinstalls_when_new_sdk_released(root: Path, latest_sdk: Any) -> None:
    se.ensure_env("min", FakeRunner(), root)
    latest_sdk.return_value = "15.3.0"
    run = FakeRunner()
    se.ensure_env("min", run, root)
    assert run.commands[0][:3] == ["uv", "venv", "--clear"]


def test_main_reports_network_failure(
    latest_sdk: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    latest_sdk.side_effect = OSError("PyPI unreachable")
    assert se.main(["min"]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "systest_env: PyPI unreachable\n"
