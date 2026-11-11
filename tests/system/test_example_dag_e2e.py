"""
End-to-end test: the example DAG run to completion under a real Airflow.

The test drives the Docker CLI. It starts an Airflow container, installs the
provider, adds a YellowDog connection, then triggers
``yellowdog-provider-test`` with params taken from the system-test config and
waits for the DAG run to finish.

Airflow deployment: ``airflow standalone`` (one process running the API server,
scheduler, dag-processor and triggerer against a shared SQLite database), not
separate services. Chosen because, per the Airflow 3 CLI source, nothing here
needs authentication: ``dags list/unpause/trigger/list-runs`` and
``tasks states-for-dag-run`` talk to the metadata DB directly (``dags trigger``
uses the local API client), and standalone forces the SimpleAuthManager, so no
FAB install or user creation is required. Standalone also handles DB init and
process supervision. If a live run shows standalone unsuitable, fall back to
the four separate services as in ``bin/start-airflow-container.sh``.
This has been verified live with ``airflow standalone``.

The DAG derives every YellowDog resource name from the Airflow run id, and the
test uses the run tag as run id, so all resource names contain the run tag
(lower-cased, which it already is) and the ``yd_client`` sweep can find
leftovers.

Airflow versions: the test is parametrised over ``e2e.airflow_versions`` in the
config (default ``["min", "max"]``; ``YD_SYSTEM_TEST_AIRFLOW`` overrides it),
resolved by ``bin/airflow_versions.py`` when each test starts (not at
collection): ``min`` is the ``apache-airflow`` floor in ``pyproject.toml``,
``max`` the latest release on PyPI, or an explicit X.Y.Z. Each version runs in
``apache/airflow:slim-<version>-python3.12`` (pulled first; the test fails,
naming the tag, if a new Airflow release has no such image yet), installs
the provider against that version's Airflow constraints (with the latest
``yellowdog-sdk``, never pinned), and uses its own container and DAG run id,
``<run tag>-<label>``.

Optional Airflow UI access: set ``ui_port`` under ``[e2e]`` in the config to
publish the container's UI on ``127.0.0.1:<ui_port>`` (the n-th version tested
uses ``ui_port + n``). The test then prints the URL and the command that shows
the generated admin password, and keeps the container after the test (pass or
fail) instead of removing it; the removal command is printed. YellowDog
resources are swept either way.

Credentials are passed to the container as ``-e NAME`` (values come from the
subprocess environment), never on a command line.
"""

import json
import os
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pytest
from yellowdog_client.platform_client import PlatformClient

from tests.system.config import (
    DEFAULT_AIRFLOW_VERSIONS,
    ConfigError,
    SystemTestConfig,
    airflow_labels,
    config_path,
    load_config,
)
from tests.system.versions import VersionError, constraints_url, resolve

pytestmark = pytest.mark.e2e

REPO_ROOT = Path(__file__).resolve().parents[2]
DAGS_DIR = REPO_ROOT / "yellowdog_provider" / "example_dags"
DAG_ID = "yellowdog-provider-test"
CONNECTION_ID = "yd-e2e"
DAG_DISCOVERY_TIMEOUT_SECONDS = 300
POLL_SECONDS = 10

# Runs inside the container. Values arrive via the environment (YD_* variables).
CONTAINER_SCRIPT = """
set -e
mkdir -p /tmp/repo
tar -C /opt/airflow/src --exclude=.venv --exclude=.git -cf - . | tar -C /tmp/repo -xf -
pip install --quiet /tmp/repo \
    --constraint "$YD_CONSTRAINTS_URL"
airflow db migrate
airflow connections add "$YD_CONNECTION_ID" \
    --conn-type yellowdog \
    --conn-host "$YD_API_URL" \
    --conn-login "$YD_KEY_ID" \
    --conn-password "$YD_KEY_SECRET"
exec airflow standalone
"""


@dataclass(frozen=True)
class AirflowTarget:
    """One Airflow version to test. ``version`` is None if not resolved."""

    label: str
    index: int
    version: str | None = None
    error: str | None = None

    @property
    def id(self) -> str:
        return f"airflow-{self.label}"

    @property
    def image(self) -> str:
        return f"apache/airflow:slim-{self.version}-python3.12"


def _resolve_target(
    target: AirflowTarget, resolver: Callable[[str], str]
) -> AirflowTarget:
    """Resolve the target's label, recording any failure as its error."""
    try:
        return replace(target, version=resolver(target.label))
    except (VersionError, OSError, ValueError, KeyError) as e:
        error = f"Cannot resolve Airflow version '{target.label}': {e}"
        return replace(target, error=error)


def _airflow_targets(
    labels: list[str], resolver: Callable[[str], str] | None
) -> list[AirflowTarget]:
    """Targets for ``labels``, resolved only if ``resolver`` is given."""
    targets = [AirflowTarget(label, index) for index, label in enumerate(labels)]
    if resolver is None:
        return targets
    return [_resolve_target(t, resolver) for t in targets]


def _collect_targets() -> list[AirflowTarget]:
    """
    The Airflow versions to parametrise over, by label only: the versions are
    resolved when each test starts, so collection makes no PyPI lookup.
    """
    path = config_path()
    try:
        if not path.exists():
            return _airflow_targets(airflow_labels(DEFAULT_AIRFLOW_VERSIONS), None)
        labels = airflow_labels(load_config(path).airflow_versions)
    except ConfigError as e:
        return [AirflowTarget("config-error", 0, error=str(e))]
    return _airflow_targets(labels, None)


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "airflow_target" in metafunc.fixturenames:
        targets = _collect_targets()
        metafunc.parametrize("airflow_target", targets, ids=[t.id for t in targets])


def _container_name(run_tag: str, target: AirflowTarget) -> str:
    return f"yd-e2e-{run_tag}-{target.label}"


def _target_ui_port(ui_port: int | None, target: AirflowTarget) -> int | None:
    return None if ui_port is None else ui_port + target.index


def _install_env(target: AirflowTarget) -> dict[str, str]:
    """Container env selecting the provider install's constraints."""
    assert target.version is not None
    return {
        "YD_CONSTRAINTS_URL": constraints_url(target.version),
    }


# Where SimpleAuthManager writes the generated admin password: the
# [core] simple_auth_manager_passwords_file setting if set, else this default
# (AIRFLOW_HOME is /opt/airflow in the official image).
PASSWORD_FILE = "/opt/airflow/simple_auth_manager_passwords.json.generated"
PASSWORD_COMMAND = (
    "f=$(airflow config get-value core simple_auth_manager_passwords_file "
    "2>/dev/null | tail -n 1); "
    f'cat "${{f:-{PASSWORD_FILE}}}"'
)


def _ui_port_args(ui_port: int | None) -> list[str]:
    """``docker run`` flags publishing the UI on localhost only, if enabled."""
    return [] if ui_port is None else ["-p", f"127.0.0.1:{ui_port}:8080"]


def _should_remove_container(ui_port: int | None, started: bool) -> bool:
    """
    The container is kept for inspection only if it started and the UI port is
    enabled; otherwise (including a failed ``docker run``) it is removed.
    """
    return ui_port is None or not started


def _ui_access_message(container: str, ui_port: int) -> str:
    return (
        f"\nAirflow UI: http://localhost:{ui_port} (user: admin)\n"
        f"Password: docker exec {container} sh -c '{PASSWORD_COMMAND}'"
    )


def _pull_failure_message(target: AirflowTarget, stderr: str) -> str:
    return (
        f"docker pull {target.image} failed. A new Airflow release may not have "
        f"a slim Python 3.12 image yet; check Docker Hub for that tag, or test "
        f"an explicit version (YD_SYSTEM_TEST_AIRFLOW or e2e.airflow_versions) "
        f"instead of Airflow {target.version}.\n{stderr}"
    )


def _docker_available() -> bool:
    try:
        return (
            subprocess.run(
                ["docker", "info"], capture_output=True, timeout=30
            ).returncode
            == 0
        )
    except (OSError, subprocess.SubprocessError):
        return False


def _run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, capture_output=True, text=True, timeout=120, **kwargs)


def _airflow(container: str, *args: str) -> subprocess.CompletedProcess[str]:
    # Suppress CLI log lines that would otherwise precede the JSON on stdout
    return _run(
        [
            "docker",
            "exec",
            "-e",
            "AIRFLOW__LOGGING__LOGGING_LEVEL=ERROR",
            container,
            "airflow",
            *args,
        ]
    )


def _parse_json(text: str) -> Any:
    """
    Parse the JSON payload in Airflow CLI output.

    Airflow can write log lines (which may contain ``[`` such as
    ``[warning  ]``) to stdout before the payload, so try every ``[`` or ``{``
    as a possible start and accept the one whose JSON value runs to the end of
    the text. Failing that (log lines after the payload), accept a list or
    object followed by other text, preferring one that starts a line, then the
    longest. Raises ValueError if there is none.
    """
    try:
        return json.loads(text)
    except ValueError:
        pass
    decoder = json.JSONDecoder()
    fallback: tuple[bool, int, Any] | None = None
    for start, char in enumerate(text):
        if char not in "[{":
            continue
        try:
            value, end = decoder.raw_decode(text, start)
        except ValueError:
            continue
        if not text[end:].strip():
            return value
        rank = (start == 0 or text[start - 1] == "\n", end - start, value)
        if fallback is None or rank[:2] > fallback[:2]:
            fallback = rank
    if fallback is not None:
        return fallback[2]
    raise ValueError("No JSON payload found in Airflow CLI output")


def _poll(
    check: Callable[[], Any], timeout: float, what: str, interval: float = POLL_SECONDS
) -> Any:
    """
    Call ``check`` until it returns a truthy value. A timed-out CLI call counts
    as "not yet" and is retried until the overall ``timeout``.
    """
    deadline = time.monotonic() + timeout
    while True:
        try:
            result = check()
        except subprocess.TimeoutExpired:
            result = None
        if result:
            return result
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Timed out after {timeout:.0f}s waiting for {what}")
        time.sleep(interval)


def _dag_listed(container: str) -> bool:
    proc = _airflow(container, "dags", "list", "-o", "json")
    if proc.returncode != 0:
        return False  # DB not migrated yet, or the container is still starting
    try:
        return any(d.get("dag_id") == DAG_ID for d in _parse_json(proc.stdout))
    except ValueError:
        return False


def _container_ready(container: str) -> bool:
    _ensure_running(container)
    return _dag_listed(container)


def _run_state(container: str, run_id: str) -> str | None:
    proc = _airflow(container, "dags", "list-runs", DAG_ID, "-o", "json")
    if proc.returncode != 0:
        return None
    try:
        runs = _parse_json(proc.stdout)
    except ValueError:
        return None
    for run in runs:
        if run.get("run_id") == run_id and run.get("state") in ("success", "failed"):
            return str(run["state"])
    return None


def _task_states(container: str, run_id: str) -> list[dict[str, Any]]:
    proc = _airflow(
        container, "tasks", "states-for-dag-run", DAG_ID, run_id, "-o", "json"
    )
    print(f"\nTask states for run '{run_id}':\n{proc.stdout}{proc.stderr}")
    if proc.returncode != 0:
        return []
    try:
        return list(_parse_json(proc.stdout))
    except ValueError:
        return []


def _ensure_running(container: str) -> None:
    """Raise if the container has exited (e.g. its setup script failed)."""
    proc = _run(
        [
            "docker",
            "inspect",
            "-f",
            "{{.State.Running}} {{.State.ExitCode}}",
            container,
        ]
    )
    running, _, exit_code = proc.stdout.strip().partition(" ")
    if proc.returncode == 0 and running == "false":
        raise RuntimeError(f"Container {container} exited with code {exit_code}")


def _last_line(text: str) -> str:
    lines = text.strip().splitlines()
    return lines[-1].strip() if lines else ""


def _check_versions(container: str, target: AirflowTarget) -> None:
    """Log the in-container Airflow and yellowdog-sdk versions; check Airflow."""
    airflow = _last_line(_airflow(container, "version").stdout)
    sdk_proc = _run(
        [
            "docker",
            "exec",
            container,
            "python",
            "-c",
            "import importlib.metadata as m; print(m.version('yellowdog-sdk'))",
        ]
    )
    sdk = _last_line(sdk_proc.stdout)
    print(f"\nIn container {container}: Airflow {airflow}, yellowdog-sdk {sdk}")
    assert airflow == target.version, f"Expected Airflow {target.version}"


def _dump_diagnostics(container: str, run_id: str, task_states: bool) -> None:
    """Print task states (if wanted) and the container log tail. Never raises,
    so a failure here can't replace the test's original exception."""
    try:
        if task_states:
            _task_states(container, run_id)
        logs = _run(["docker", "logs", "--tail", "200", container])
        print(f"\nContainer log tail:\n{logs.stdout}{logs.stderr}")
    except Exception as e:
        print(f"\nDiagnostics failed: {e!r}")


def _dag_conf(config: SystemTestConfig) -> dict[str, Any]:
    """The example DAG's params, as a ``--conf`` payload."""
    return {
        "connection_id": CONNECTION_ID,
        "namespace": config.namespace,
        "compute_requirement_template": config.compute_requirement_template,
        "image_family": config.image_family,
        "task_type": config.task_type,
        "task_arguments": config.task_arguments,
        "sensor_timeout_seconds": config.sensor_timeout_seconds,
        "poke_interval_seconds": config.poke_interval_seconds,
        "max_nodes": config.max_instances,
        "user_data": config.user_data or "",
    }


def test_example_dag_runs_to_success(
    system_config: SystemTestConfig,
    yd_client: PlatformClient,
    run_tag: str,
    airflow_target: AirflowTarget,
) -> None:
    if airflow_target.error is not None:
        pytest.fail(airflow_target.error)
    if not _docker_available():
        pytest.skip("Docker is not available (`docker info` failed)")
    airflow_target = _resolve_target(airflow_target, resolve)
    if airflow_target.error is not None:
        pytest.fail(airflow_target.error)

    # The sweep itself runs in the yd_client fixture teardown; every container
    # name and DAG run id (hence every YellowDog resource name) contains run_tag.
    container = _container_name(run_tag, airflow_target)
    run_id = f"{run_tag}-{airflow_target.label}"
    ui_port = _target_ui_port(system_config.ui_port, airflow_target)
    image = airflow_target.image
    print(f"\n=== Airflow {airflow_target.label}: {airflow_target.version} ({image})")
    env = {
        **os.environ,
        **_install_env(airflow_target),
        "YD_CONNECTION_ID": CONNECTION_ID,
        "YD_API_URL": system_config.api_url,
        "YD_KEY_ID": system_config.key_id,
        "YD_KEY_SECRET": system_config.key_secret,
    }
    started = False
    # Task states still to print on failure (not yet printed by the test)
    want_task_states = False
    try:
        pull = subprocess.run(
            ["docker", "pull", image], capture_output=True, text=True, timeout=900
        )
        assert pull.returncode == 0, _pull_failure_message(airflow_target, pull.stderr)
        proc = subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                container,
                *_ui_port_args(ui_port),
                "-e",
                "YD_CONSTRAINTS_URL",
                "-e",
                "YD_CONNECTION_ID",
                "-e",
                "YD_API_URL",
                "-e",
                "YD_KEY_ID",
                "-e",
                "YD_KEY_SECRET",
                "-v",
                f"{REPO_ROOT}:/opt/airflow/src:ro",
                "-v",
                f"{DAGS_DIR}:/opt/airflow/dags:ro",
                "--entrypoint",
                "/bin/bash",
                image,
                "-c",
                CONTAINER_SCRIPT,
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=600,
        )
        assert proc.returncode == 0, f"docker run failed: {proc.stderr}"
        started = True
        if ui_port is not None:
            print(_ui_access_message(container, ui_port))

        try:
            _poll(
                lambda: _container_ready(container),
                DAG_DISCOVERY_TIMEOUT_SECONDS,
                f"DAG '{DAG_ID}' to be discovered",
            )
            _check_versions(container, airflow_target)

            unpause = _airflow(container, "dags", "unpause", DAG_ID)
            assert unpause.returncode == 0, unpause.stderr
            trigger = _airflow(
                container,
                "dags",
                "trigger",
                DAG_ID,
                "--run-id",
                run_id,
                "--conf",
                json.dumps(_dag_conf(system_config)),
            )
            assert trigger.returncode == 0, trigger.stderr
            want_task_states = True

            state = _poll(
                lambda: _run_state(container, run_id),
                4 * system_config.sensor_timeout_seconds,
                f"DAG run '{run_id}' to finish",
            )
            tasks = _task_states(container, run_id)
            want_task_states = False  # just printed
            assert state == "success", f"DAG run finished in state '{state}'"
            assert tasks, "No task states were reported for the DAG run"
            bad = {t["task_id"]: t["state"] for t in tasks if t["state"] != "success"}
            assert not bad, f"Tasks not successful: {bad}"
        except BaseException:
            _dump_diagnostics(container, run_id, want_task_states)
            raise
    finally:
        if _should_remove_container(ui_port, started):
            _run(["docker", "rm", "-f", container])  # no-op if it doesn't exist
        else:
            print(
                f"\nContainer {container} kept; remove it with:\n"
                f"docker rm -f {container}"
            )
