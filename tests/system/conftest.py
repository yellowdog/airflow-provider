"""Fixtures and gating for the live-platform system tests."""

import importlib.metadata
import json
import os
from collections.abc import Iterator
from itertools import pairwise
from pathlib import Path

import pytest
from yellowdog_client.model import ApiKey, ServicesSchema
from yellowdog_client.platform_client import PlatformClient

from tests.system.cleanup import sweep
from tests.system.config import (
    AIRFLOW_ENV_VAR,
    SystemTestConfig,
    config_path,
    load_config,
    new_run_tag,
)

CONNECTION_ENV_VAR = "AIRFLOW_CONN_YD_SYSTEM_TEST"
REPORTED_PACKAGES = ("apache-airflow", "apache-airflow-task-sdk", "yellowdog-sdk")
SWEEP_REPORTS = pytest.StashKey[list[str]]()


def record_sweep(config: pytest.Config, line: str) -> None:
    """Keep a sweep report for the terminal summary (shown even without -s)."""
    config.stash.setdefault(SWEEP_REPORTS, []).append(line)


def pytest_terminal_summary(
    terminalreporter: pytest.TerminalReporter,
    exitstatus: int,
    config: pytest.Config,
) -> None:
    lines = config.stash.get(SWEEP_REPORTS, [])
    if lines:
        terminalreporter.section("YellowDog system-test sweep")
        for line in lines:
            terminalreporter.write_line(line)


def pytest_report_header(config: pytest.Config) -> list[str]:
    """Record which Airflow / yellowdog-sdk stack this session runs with."""

    def version(package: str) -> str:
        try:
            return importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            return "not installed"

    stack = ", ".join(f"{p} {version(p)}" for p in REPORTED_PACKAGES)
    lines = [f"YellowDog system tests: {stack}"]
    if os.environ.get(AIRFLOW_ENV_VAR, "").strip():
        lines.append(f"{AIRFLOW_ENV_VAR}={os.environ[AIRFLOW_ENV_VAR]}")
    return lines


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    path = config_path()
    if path.exists():
        return
    skip = pytest.mark.skip(reason=f"system-test config not found at {path}")
    for item in items:
        parts = Path(str(item.path)).parts
        if any(a == "tests" and b == "system" for a, b in pairwise(parts)):
            item.add_marker(skip)


@pytest.fixture(scope="session")
def system_config() -> SystemTestConfig:
    return load_config(config_path())


@pytest.fixture(scope="session")
def run_tag() -> str:
    return new_run_tag()


@pytest.fixture(scope="session")
def sweep_run_tag_prefixes() -> list[str]:
    """
    DAG-run tag prefixes (``RunStamp.run_tag_prefix()``) of the DAG runs the
    tests use. Tests append to it; the ``yd_client`` sweep also cancels live
    work requirements whose tag carries one of them, since names derived from
    a run's IDs are shortened and need not contain the run tag.
    """
    return []


@pytest.fixture(scope="session")
def sweep_name_fragments() -> list[str]:
    """
    Extra resource names (or name fragments) the ``yd_client`` sweep also
    matches: tests append the names of resources whose names cannot contain
    the whole run tag (e.g. names derived from a DAG run ID and shortened).
    """
    return []


@pytest.fixture(scope="session")
def yd_client(
    system_config: SystemTestConfig,
    run_tag: str,
    pytestconfig: pytest.Config,
    sweep_run_tag_prefixes: list[str],
    sweep_name_fragments: list[str],
) -> Iterator[PlatformClient]:
    client = PlatformClient.create(
        ServicesSchema(defaultUrl=system_config.api_url),
        ApiKey(system_config.key_id, system_config.key_secret),
    )
    try:
        yield client
    finally:
        try:
            report = sweep(
                client,
                system_config.namespace,
                run_tag,
                SweepLog(pytestconfig),
                run_tag_prefixes=list(dict.fromkeys(sweep_run_tag_prefixes)),
                extra_fragments=[f for f in dict.fromkeys(sweep_name_fragments) if f],
            )
            record_sweep(pytestconfig, f"Sweep for '{run_tag}': {report}")
        finally:
            client.close()


@pytest.fixture(scope="session")
def yd_connection_env(system_config: SystemTestConfig) -> Iterator[str]:
    conn_id = "yd_system_test"
    os.environ[CONNECTION_ENV_VAR] = json.dumps(
        {
            "conn_type": "yellowdog",
            "host": system_config.api_url,
            "login": system_config.key_id,
            "password": system_config.key_secret,
        }
    )
    try:
        yield conn_id
    finally:
        os.environ.pop(CONNECTION_ENV_VAR, None)


class SweepLog:
    """Sweep logger: keeps every message for the terminal summary (shown even
    without -s) as well as printing it live."""

    def __init__(self, config: pytest.Config) -> None:
        self._config = config

    def info(self, msg: str, /, *args: object) -> None:
        self._emit(msg % args if args else msg)

    def warning(self, msg: str, /, *args: object) -> None:
        self._emit("WARNING: " + (msg % args if args else msg))

    def _emit(self, line: str) -> None:
        print(line)
        record_sweep(self._config, line)
