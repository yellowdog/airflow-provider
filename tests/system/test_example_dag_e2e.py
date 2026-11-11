"""
End-to-end tests: the example DAG run under a real Airflow.

The tests drive the Docker CLI. For each Airflow version, a module-scoped
fixture starts an Airflow container, installs the provider, adds a YellowDog
connection, and triggers ``yellowdog-provider-test`` twice with params taken
from the system-test config; the two DAG runs proceed concurrently:

* the main run (``deferrable=true``: every wait is deferred to the
  triggerer, which ``airflow standalone`` runs) must succeed in full;
* the failure run (``task_arguments`` = ``resources.failing_task_arguments``,
  not deferred; skipped if that is unset with no default for the task type)
  must fail, with both run tasks failed and the teardown
  (``shutdown-worker-pool``) successful.

On Airflow 3.3 and later, a third run checks the trigger kill hook: with
long-running work (``resources.long_task_arguments``) and both run tasks
deferred, one is marked failed through the REST API (as the
SimpleAuthManager's generated admin user), and its work requirement must be
cancelled by the trigger's ``on_kill`` while the other is still deferred.

After each run, nothing of it (work requirement, pool or compute requirement)
may be left live.

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

The DAG derives every YellowDog resource name from the Airflow run id. Each
run's id is ``<label>-<scenario>-<run tag>``. Names are shortened to 60
characters, so the id puts what tells runs apart first, and the second work
requirement's derived name holds only part of the run id; so every run's pool
and first work requirement names, and its
run tag prefix (which every work requirement it submits carries in its tag),
are registered with the ``yd_client`` sweep, which finds leftovers by any of
these as well as by the run tag.

Airflow versions: the test is parametrised over ``e2e.airflow_versions`` in the
config (default ``["min", "max"]``; ``YD_SYSTEM_TEST_AIRFLOW`` overrides it),
resolved by ``bin/airflow_versions.py`` when the fixture starts (not at
collection): ``min`` is the ``apache-airflow`` floor in ``pyproject.toml``,
``max`` the latest release on PyPI, or an explicit X.Y.Z. Each version runs in
``apache/airflow:slim-<version>-python3.12`` (pulled first; the test fails,
naming the tag, if a new Airflow release has no such image yet), installs
the provider against that version's Airflow constraints (with the latest
``yellowdog-sdk``, never pinned), and uses its own container and DAG run ids.

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
import re
import subprocess
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import pytest
from yellowdog_client.model import WorkRequirementStatus
from yellowdog_client.platform_client import PlatformClient

from tests.system.cleanup import (
    find_compute_requirements,
    find_work_requirements,
    find_worker_pools,
)
from tests.system.config import (
    DEFAULT_AIRFLOW_VERSIONS,
    ConfigError,
    SystemTestConfig,
    airflow_labels,
    config_path,
    load_config,
    task_arguments_or_skip,
)
from tests.system.versions import VersionError, constraints_url, resolve
from yellowdog_provider.utils.run_stamp import RunStamp

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
        # Module scope: each Airflow version's tests share one container
        metafunc.parametrize(
            "airflow_target", targets, ids=[t.id for t in targets], scope="module"
        )


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


def _dag_conf(config: SystemTestConfig, **overrides: Any) -> dict[str, Any]:
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
        "deferrable": False,
        **overrides,
    }


def _normalise_yd_name(name: str) -> str:
    """As the example DAG's ``_normalise_yd_name``."""
    name = name.replace("/", "-").replace(" ", "_").replace(".", "_").lower()
    name = re.sub("[^a-z0-9_-]", "", name).strip("-_")
    if not name[:1].isalpha():
        name = "x-" + name if name else "x"
    return name[:60].rstrip("-_")


def _dag_entity_name(prefix: str, run_id: str) -> str:
    """As the example DAG's ``_get_entity_name``."""
    return _normalise_yd_name(f"{prefix}-{run_id.replace('manual__', '')}")


def dag_pool_name(run_id: str) -> str:
    """The name the example DAG gives the worker pool (and its compute
    requirement) of DAG run ``run_id``."""
    return _dag_entity_name(f"wp-{DAG_ID}", run_id)


def dag_first_wr_name(run_id: str) -> str:
    """The name the example DAG gives the first work requirement of DAG run
    ``run_id``."""
    return _dag_entity_name("a-wr-airflow", run_id)


def dag_run_tag_prefix(run_id: str) -> str:
    """
    The tag prefix of the work requirements of DAG run ``run_id`` (the
    container sets no ``[yellowdog] deployment_id``).
    """
    return RunStamp(DAG_ID, run_id, "", -1).run_tag_prefix()


def supports_trigger_kill(version: str | None) -> bool:
    """Whether Airflow calls a trigger's ``on_kill`` (from Airflow 3.3)."""
    if version is None:
        return False
    major_minor = tuple(int(part) for part in version.split(".")[:2])
    return major_minor >= TRIGGER_KILL_MIN


# A task in one of these states will not defer (again) in this DAG run
TERMINAL_TASK_STATES = frozenset({"failed", "success", "upstream_failed", "skipped"})
SETUP_TASK = "provision-worker-pool"


def run_tasks_deferred(
    states: dict[str, str], run_state: str | None, task_ids: list[str]
) -> bool:
    """
    Whether all of ``task_ids`` are deferred. Raises AssertionError at once if
    that can no longer happen: the DAG run has finished (``run_state`` is set),
    the setup task ended without success, or one of the tasks reached a
    terminal state.
    """
    if run_state is not None:
        raise AssertionError(f"DAG run finished ({run_state}): {states}")
    setup = states.get(SETUP_TASK)
    if setup in TERMINAL_TASK_STATES and setup != "success":
        raise AssertionError(f"Setup task {SETUP_TASK} {setup}: {states}")
    ended = {t: states[t] for t in task_ids if states.get(t) in TERMINAL_TASK_STATES}
    if ended:
        raise AssertionError(f"Run tasks ended before deferring: {ended}: {states}")
    return all(states.get(t) == "deferred" for t in task_ids)


MAIN = "main"
FAILURE = "fail"
KILL = "kill"
TRIGGER_KILL_MIN = (3, 3)
# The triggerer notices a deleted trigger within its loop, and bounds on_kill
# by [triggerer] on_kill_timeout (default 30 seconds)
KILL_CANCEL_TIMEOUT_SECONDS = 180
WR_CANCELLED = {WorkRequirementStatus.CANCELLING, WorkRequirementStatus.CANCELLED}
# How long _assert_nothing_live waits for CANCELLING work requirements
CANCELLING_GRACE_SECONDS = 60


@dataclass
class AirflowContainer:
    """A running Airflow container with the example DAG unpaused."""

    name: str
    target: AirflowTarget
    run_tag: str
    config: SystemTestConfig
    sweep_run_tag_prefixes: list[str]
    sweep_name_fragments: list[str]
    # The DAG run IDs triggered so far, by scenario
    runs: dict[str, str] = field(default_factory=dict[str, str])

    def run_id(self, scenario: str) -> str:
        """
        The DAG run ID for a scenario: ``<label>-<scenario>-<run tag>``. The
        label and scenario come first so that pool names (shortened to 60
        characters) differ across versions and scenarios; the names are
        registered with the sweep, as they need not contain the whole run tag.
        """
        return f"{self.target.label}-{scenario}-{self.run_tag}"

    def trigger(self, scenario: str, conf: dict[str, Any]) -> str:
        """Trigger a DAG run, registering its resources with the sweep."""
        run_id = self.run_id(scenario)
        # Registered before triggering, so the sweep covers a partial run
        self.sweep_run_tag_prefixes.append(dag_run_tag_prefix(run_id))
        self.sweep_name_fragments.extend(
            [dag_pool_name(run_id), dag_first_wr_name(run_id)]
        )
        proc = _airflow(
            self.name,
            "dags",
            "trigger",
            DAG_ID,
            "--run-id",
            run_id,
            "--conf",
            json.dumps(conf),
        )
        assert proc.returncode == 0, proc.stderr
        self.runs[scenario] = run_id
        return run_id

    def wait_for_run(self, run_id: str) -> str:
        """Wait until the DAG run finishes; return its state."""
        return _poll(
            lambda: _run_state(self.name, run_id),
            4 * self.config.sensor_timeout_seconds,
            f"DAG run '{run_id}' to finish",
        )


@pytest.fixture(scope="module")
def airflow_container(
    system_config: SystemTestConfig,
    yd_client: PlatformClient,
    run_tag: str,
    airflow_target: AirflowTarget,
    sweep_run_tag_prefixes: list[str],
    sweep_name_fragments: list[str],
) -> Iterator[AirflowContainer]:
    """
    Start an Airflow container for ``airflow_target``, wait until the example
    DAG is listed, unpause it, and trigger the main (deferrable) and failure
    runs, which then run concurrently while the tests wait for them in turn.
    The container is removed afterwards (unless kept for the UI). The
    YellowDog sweep itself runs in the ``yd_client`` fixture teardown.
    """
    if airflow_target.error is not None:
        pytest.fail(airflow_target.error)
    if not _docker_available():
        pytest.skip("Docker is not available (`docker info` failed)")
    airflow_target = _resolve_target(airflow_target, resolve)
    if airflow_target.error is not None:
        pytest.fail(airflow_target.error)

    container = AirflowContainer(
        name=_container_name(run_tag, airflow_target),
        target=airflow_target,
        run_tag=run_tag,
        config=system_config,
        sweep_run_tag_prefixes=sweep_run_tag_prefixes,
        sweep_name_fragments=sweep_name_fragments,
    )
    name = container.name
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
                name,
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
            print(_ui_access_message(name, ui_port))

        try:
            _poll(
                lambda: _container_ready(name),
                DAG_DISCOVERY_TIMEOUT_SECONDS,
                f"DAG '{DAG_ID}' to be discovered",
            )
            _check_versions(name, airflow_target)
            unpause = _airflow(name, "dags", "unpause", DAG_ID)
            assert unpause.returncode == 0, unpause.stderr
            cfg = system_config
            # The main run defers every wait; the failure run waits on workers
            container.trigger(MAIN, _dag_conf(cfg, deferrable=True))
            # Not triggered without failing task arguments (its test skips)
            if cfg.failing_task_arguments is not None:
                container.trigger(
                    FAILURE,
                    _dag_conf(cfg, task_arguments=list(cfg.failing_task_arguments)),
                )
        except BaseException:
            _dump_diagnostics(name, "", task_states=False)
            raise
        yield container
    finally:
        if _should_remove_container(ui_port, started):
            _run(["docker", "rm", "-f", name])  # no-op if it doesn't exist
        else:
            print(f"\nContainer {name} kept; remove it with:\ndocker rm -f {name}")


def _states_by_task(tasks: list[dict[str, Any]]) -> dict[str, str]:
    return {str(t["task_id"]): str(t["state"]) for t in tasks}


def _live_resources(
    client: PlatformClient, namespace: str, run_id: str
) -> tuple[list[str], list[str]]:
    """
    The run's unfinished resources: those still live, and its work
    requirements that are CANCELLING (on their way to CANCELLED).
    """
    pool_name = dag_pool_name(run_id)
    wrs = find_work_requirements(
        client, namespace, dag_first_wr_name(run_id), [dag_run_tag_prefix(run_id)]
    )
    pools = find_worker_pools(client, namespace, pool_name)
    crs = find_compute_requirements(client, namespace, pool_name)
    cancelling = [
        f"work requirement {w.id} ({w.name}, {w.status})"
        for w in wrs
        if w.status == WorkRequirementStatus.CANCELLING
    ]
    live = [
        f"work requirement {w.id} ({w.name}, {w.status})"
        for w in wrs
        if w.status != WorkRequirementStatus.CANCELLING
    ]
    live += [f"worker pool {p.id} ({p.name}, {p.status})" for p in pools]
    live += [f"compute requirement {c.id} ({c.name}, {c.status})" for c in crs]
    return live, cancelling


def _assert_nothing_live(client: PlatformClient, namespace: str, run_id: str) -> None:
    """
    No unfinished work requirement, pool or compute requirement of the run.
    A CANCELLING work requirement (e.g. one whose running tasks are being
    aborted) is waited for briefly, then accepted.
    """
    deadline = time.monotonic() + CANCELLING_GRACE_SECONDS
    while True:
        live, cancelling = _live_resources(client, namespace, run_id)
        assert not live, f"Left live after DAG run '{run_id}': {live + cancelling}"
        if not cancelling:
            return
        if time.monotonic() >= deadline:
            print(f"\nStill cancelling after DAG run '{run_id}': {cancelling}")
            return
        time.sleep(5)


def test_example_dag_runs_to_success(
    airflow_container: AirflowContainer, yd_client: PlatformClient
) -> None:
    """The main run, deferring every wait to the triggerer, succeeds."""
    container = airflow_container
    run_id = container.runs[MAIN]
    # Task states still to print on failure (not yet printed by the test)
    want_task_states = True
    try:
        state = container.wait_for_run(run_id)
        tasks = _task_states(container.name, run_id)
        want_task_states = False  # just printed
        assert state == "success", f"DAG run finished in state '{state}'"
        assert tasks, "No task states were reported for the DAG run"
        bad = {k: v for k, v in _states_by_task(tasks).items() if v != "success"}
        assert not bad, f"Tasks not successful: {bad}"
        _assert_nothing_live(yd_client, container.config.namespace, run_id)
    except BaseException:
        _dump_diagnostics(container.name, run_id, want_task_states)
        raise


def test_example_dag_failure_runs_teardown(
    airflow_container: AirflowContainer, yd_client: PlatformClient
) -> None:
    """
    A run whose work fails (its tasks exit non-zero; waits not deferred): the
    run tasks and the DAG run fail, the teardown still succeeds, and nothing
    of the run is left live.
    """
    container = airflow_container
    task_arguments_or_skip(container.config, "failing_task_arguments")
    run_id = container.runs[FAILURE]
    want_task_states = True
    try:
        state = container.wait_for_run(run_id)
        tasks = _states_by_task(_task_states(container.name, run_id))
        want_task_states = False  # just printed
        assert state == "failed", f"DAG run finished in state '{state}'"
        assert tasks == {
            "provision-worker-pool": "success",
            "run-work-requirement-1": "failed",
            "run-work-requirement-2": "failed",
            "shutdown-worker-pool": "success",
        }
        _assert_nothing_live(yd_client, container.config.namespace, run_id)
    except BaseException:
        _dump_diagnostics(container.name, run_id, want_task_states)
        raise


# Runs in the container: marks a task instance failed through the REST API,
# authenticating as the SimpleAuthManager's generated user, and prints the
# HTTP status. Arguments: DAG ID, run ID, task ID.
MARK_FAILED_SCRIPT = """
import json, os, sys, urllib.parse, urllib.request
from airflow.configuration import conf
dag_id, run_id, task_id = sys.argv[1:4]
path = conf.get("core", "simple_auth_manager_passwords_file", fallback=None) or (
    os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"),
                 "simple_auth_manager_passwords.json.generated"))
with open(path) as f:
    username, password = next(iter(json.load(f).items()))
base = "http://localhost:8080"
request = urllib.request.Request(
    base + "/auth/token",
    data=json.dumps({"username": username, "password": password}).encode(),
    headers={"Content-Type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(request, timeout=30) as response:
    token = json.load(response)["access_token"]
quote = lambda s: urllib.parse.quote(s, safe="")
request = urllib.request.Request(
    f"{base}/api/v2/dags/{quote(dag_id)}/dagRuns/{quote(run_id)}"
    f"/taskInstances/{quote(task_id)}",
    data=json.dumps({"new_state": "failed"}).encode(),
    headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
    method="PATCH",
)
with urllib.request.urlopen(request, timeout=30) as response:
    print(response.status)
"""


def _mark_failed(container: str, run_id: str, task_id: str) -> None:
    proc = _run(
        [
            "docker",
            "exec",
            container,
            "python",
            "-c",
            MARK_FAILED_SCRIPT,
            DAG_ID,
            run_id,
            task_id,
        ]
    )
    assert proc.returncode == 0, (
        f"Marking {task_id} failed via the REST API failed:\n{proc.stdout}{proc.stderr}"
    )
    print(f"\nMarked {task_id} of '{run_id}' failed: HTTP {_last_line(proc.stdout)}")


def _find_wr(
    client: PlatformClient, namespace: str, name: str
) -> tuple[str, WorkRequirementStatus | None] | None:
    """The named work requirement's ID and status, or None if not found."""
    try:
        wr = client.work_client.get_work_requirement_by_name(namespace, name)
    except Exception:
        return None
    return (str(wr.id), wr.status) if wr.id else None


def _wr_cancelled(client: PlatformClient, wr_id: str) -> bool:
    return client.work_client.get_work_requirement_by_id(wr_id).status in WR_CANCELLED


def test_trigger_kill_cancels_deferred_work_requirement(
    airflow_container: AirflowContainer, yd_client: PlatformClient
) -> None:
    """
    Airflow 3.3 and later: marking a deferred ``RunWorkRequirement`` failed
    makes the triggerer call the trigger's ``on_kill``, which cancels its
    work requirement while the sibling task is still deferred (so it is not
    the teardown's doing). The teardown then cleans up the rest.
    """
    container = airflow_container
    if not supports_trigger_kill(container.target.version):
        pytest.skip(
            f"Airflow {container.target.version} has no kill hook for deferred "
            "tasks (needs 3.3 or later)"
        )
    cfg = container.config
    long_task_arguments = task_arguments_or_skip(cfg, "long_task_arguments")
    run_id = container.trigger(
        KILL,
        _dag_conf(
            cfg,
            deferrable=True,
            task_arguments=long_task_arguments,
            # The work must outlast the check (it is cancelled by the test)
            sensor_timeout_seconds=max(cfg.sensor_timeout_seconds, 1800),
        ),
    )
    first, second = "run-work-requirement-1", "run-work-requirement-2"
    wr_name = dag_first_wr_name(run_id)
    want_task_states = True
    try:

        def both_deferred() -> bool:
            proc = _airflow(
                container.name,
                "tasks",
                "states-for-dag-run",
                DAG_ID,
                run_id,
                "-o",
                "json",
            )
            try:
                states = _states_by_task(list(_parse_json(proc.stdout)))
            except ValueError:
                return False
            return run_tasks_deferred(
                states, _run_state(container.name, run_id), [first, second]
            )

        _poll(both_deferred, 2 * cfg.sensor_timeout_seconds, "both run tasks to defer")
        found = _find_wr(yd_client, cfg.namespace, wr_name)
        assert found is not None, f"Work requirement '{wr_name}' not found"
        wr_id, status = found
        assert status is not None and not status.finished, status

        _mark_failed(container.name, run_id, first)
        _poll(
            lambda: _wr_cancelled(yd_client, wr_id),
            KILL_CANCEL_TIMEOUT_SECONDS,
            f"work requirement {wr_id} to be cancelled by the trigger's on_kill",
            interval=5,
        )
        # Not the teardown: that waits for the sibling, which is still deferred
        states = _states_by_task(_task_states(container.name, run_id))
        assert states.get(second) == "deferred", states
        assert states.get("shutdown-worker-pool") not in ("running", "success"), states

        # Finish the run: kill the sibling the same way; the teardown then runs
        _mark_failed(container.name, run_id, second)
        state = container.wait_for_run(run_id)
        tasks = _states_by_task(_task_states(container.name, run_id))
        want_task_states = False  # just printed
        assert state == "failed", f"DAG run finished in state '{state}'"
        assert tasks.get("shutdown-worker-pool") == "success", tasks
        _assert_nothing_live(yd_client, cfg.namespace, run_id)
    except BaseException:
        _dump_diagnostics(container.name, run_id, want_task_states)
        raise
