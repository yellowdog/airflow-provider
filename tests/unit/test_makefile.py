"""Tests for the Makefile's AIRFLOW handling (``make -n``: nothing is run)."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.skipif(shutil.which("make") is None, reason="needs make")


def _dry_run(*args: str, env_airflow: str | None = None) -> str:
    env = {k: v for k, v in os.environ.items() if k != "AIRFLOW"}
    if env_airflow is not None:
        env["AIRFLOW"] = env_airflow
    return subprocess.run(
        ["make", "-n", *args],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    ).stdout


@pytest.mark.parametrize("value", ["min,max", "min max", "min, max"])
def test_system_test_labels_split_on_commas_and_spaces(value: str) -> None:
    out = _dry_run("system-test", f"AIRFLOW={value}")
    assert "for label in min max; do" in " ".join(out.split())


def test_system_test_ignores_exported_airflow() -> None:
    out = _dry_run("system-test", env_airflow="3.2.1")
    assert "for label in min max; do" in " ".join(out.split())


def test_e2e_test_ignores_exported_airflow() -> None:
    out = _dry_run("e2e-test", env_airflow="3.2.1")
    assert "YD_SYSTEM_TEST_AIRFLOW" not in out


def test_e2e_test_uses_command_line_airflow() -> None:
    out = _dry_run("e2e-test", "AIRFLOW=min,3.2.1")
    assert 'YD_SYSTEM_TEST_AIRFLOW="min 3.2.1"' in out
