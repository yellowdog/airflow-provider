"""
Live-platform component tests: each operator, sensor and the hook against the
real YellowDog platform. Tests that need workers share one provisioned worker
pool and run in file order; the shutdown tests (from
``test_provision_wait_until_ready_and_shutdown_wait``) must stay last.

Every resource name contains the run tag so the ``yd_client`` sweep can find
and remove anything left behind. The run-stamped operators are given task
contexts whose DAG run IDs contain the run tag, and each such run's tag prefix
is registered with the sweep (``sweep_run_tag_prefixes``) as a second
backstop.
"""

import logging
import secrets
import threading
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, cast

import pytest
from airflow.sdk import DAG, Context
from yellowdog_client.model import (
    AutoShutdown,
    ComputeRequirementStatus,
    ComputeRequirementTemplateUsage,
    MachineImageFamilySearch,
    NodeWorkerTarget,
    NodeWorkerTargetType,
    ProvisionedWorkerPool,
    ProvisionedWorkerPoolProperties,
    RunSpecification,
    Task,
    TaskGroup,
    WorkerPoolStatus,
    WorkRequirement,
    WorkRequirementStatus,
)
from yellowdog_client.model.exceptions import InvalidRequestException
from yellowdog_client.platform_client import PlatformClient

from tests.support import NoOp, make_context, render
from tests.system.config import SystemTestConfig, task_arguments_or_skip
from yellowdog_provider.exceptions.yellowdog_exceptions import (
    WaitTimeoutError,
    YellowDogStateError,
)
from yellowdog_provider.hooks.yellowdog_hooks import YellowDogHook, attempt_tag
from yellowdog_provider.operators.yellowdog_operators import (
    AddPopulatedWorkRequirement,
    AddTaskGroupsToWorkRequirement,
    AddTasksToTaskGroup,
    AddWorkRequirement,
    CancelWorkRequirement,
    ProvisionWorkerPool,
    RunWorkRequirement,
    ShutdownProvisionedWorkerPool,
)
from yellowdog_provider.sensors.yellowdog_sensors import (
    ComputeRequirementStateSensor,
    WorkerPoolStateSensor,
    WorkRequirementStateSensor,
)
from yellowdog_provider.utils.run_stamp import RunStamp, try_name

pytestmark = pytest.mark.system

CONN_ID = "yd_system_test"
GROUP_NAMES = ["g1", "g2", "g3"]
# DAG ID of the task contexts given to run-stamped operators
DAG_ID = "yd-systest"


log = logging.getLogger(__name__)

# Pool states from which IDLE/RUNNING can no longer be reached
POOL_FINISHED = {WorkerPoolStatus.SHUTDOWN, WorkerPoolStatus.TERMINATED}
POOL_READY = {WorkerPoolStatus.IDLE, WorkerPoolStatus.RUNNING}
WR_CANCELLED = {WorkRequirementStatus.CANCELLING, WorkRequirementStatus.CANCELLED}


def _run(op: Any) -> Any:
    """
    Execute ``op`` as a task of its own one-off DAG run. Run-stamped operators
    treat work requirements with the same stamp as earlier attempts of the
    same task (and a teardown cancels its DAG run's), so each call gets a
    unique run ID: separate calls, tests and concurrent sessions never share
    a stamp.
    """
    ctx = make_context(dag_id=DAG_ID, run_id=f"adhoc-{secrets.token_hex(8)}")
    render(op, ctx)
    return op.execute(ctx)


def _wait(sensor: Any, cfg: SystemTestConfig) -> None:
    """
    Poke until satisfied, honouring the configured interval and timeout. A
    sensor raises YellowDogStateError at once if its resource reaches one of
    its failure states (by default, final states that are not targets).
    """
    ctx = make_context()
    render(sensor, ctx)
    deadline = time.monotonic() + cfg.sensor_timeout_seconds
    while not sensor.poke(ctx):
        if time.monotonic() >= deadline:
            pytest.fail(
                f"{type(sensor).__name__} timed out after {cfg.sensor_timeout_seconds}s"
            )
        time.sleep(cfg.poke_interval_seconds)


def _sensor_kwargs(cfg: SystemTestConfig) -> dict[str, Any]:
    return {
        "connection_id": CONN_ID,
        "poke_interval": cfg.poke_interval_seconds,
        "timeout": cfg.sensor_timeout_seconds,
    }


def _image_family_id(client: PlatformClient, family: str) -> str:
    """Resolve 'namespace/name' to an image family ID (else pass through)."""
    parts = family.split("/")
    if len(parts) != 2:
        return family
    namespace, name = parts
    found = client.images_client.get_image_families(
        MachineImageFamilySearch(
            namespace=namespace, familyName=name, includePublic=True
        )
    ).list_all()
    return next((f.id for f in found if f.name == name and f.id), family)


def _run_spec(cfg: SystemTestConfig, run_tag: str) -> RunSpecification:
    return RunSpecification(taskTypes=[cfg.task_type], workerTags=[run_tag])


def _groups(cfg: SystemTestConfig, run_tag: str, names: list[str]) -> list[TaskGroup]:
    return [TaskGroup(name=n, runSpecification=_run_spec(cfg, run_tag)) for n in names]


def _tasks(cfg: SystemTestConfig, names: list[str]) -> list[Task]:
    return [
        Task(name=n, taskType=cfg.task_type, arguments=list(cfg.task_arguments))
        for n in names
    ]


def _new_wr(cfg: SystemTestConfig, run_tag: str, suffix: str) -> str:
    """Add an empty work requirement; return its ID."""
    return _run(
        AddWorkRequirement(
            task_id="add-wr",
            connection_id=CONN_ID,
            work_requirement=WorkRequirement(
                namespace=cfg.namespace, name=f"{run_tag}-{suffix}"
            ),
        )
    )


def _cancel(wr_id: str) -> None:
    _run(
        CancelWorkRequirement(
            task_id="cancel-wr",
            connection_id=CONN_ID,
            work_requirement_id=wr_id,
            abort_running_tasks=True,
        )
    )


@contextmanager
def _cancelling(wr_id: str) -> Iterator[None]:
    """Best-effort cancel on exit (the sweep is the final backstop)."""
    try:
        yield
    finally:
        with suppress(Exception):
            _cancel(wr_id)


def _fetch_wr(client: PlatformClient, wr_id: str) -> WorkRequirement:
    return client.work_client.get_work_requirement_by_id(wr_id)


def _pool_status(client: PlatformClient, pool_id: str) -> WorkerPoolStatus | None:
    return client.worker_pool_client.get_worker_pool_by_id(pool_id).status


def _await_pool_finished(
    client: PlatformClient, pool_id: str, cfg: SystemTestConfig
) -> None:
    """
    Wait (up to the sensor timeout) for the pool to shut down. Never raises:
    used in teardown, where the sweep is the final backstop.
    """
    deadline = time.monotonic() + cfg.sensor_timeout_seconds
    try:
        while _pool_status(client, pool_id) not in POOL_FINISHED:
            if time.monotonic() >= deadline:
                log.warning(
                    f"Worker pool {pool_id} not shut down after "
                    f"{cfg.sensor_timeout_seconds}s"
                )
                return
            time.sleep(cfg.poke_interval_seconds)
    except Exception as e:
        log.warning(f"Cannot check worker pool {pool_id} shutdown: {e}")


@dataclass(frozen=True)
class Pool:
    id: str
    name: str  # Also the name of the compute requirement it creates
    # The pool's status fetched by the fixture straight after
    # ProvisionWorkerPool(wait_until_ready=True) returned (a separate check
    # made moments later, not the status the operator saw), and the ID the
    # operator pushed to XCom
    status_after_return: WorkerPoolStatus | None
    pushed_id: object


@pytest.fixture(scope="module")
def pool(
    system_config: SystemTestConfig,
    yd_client: PlatformClient,
    yd_connection_env: str,
    run_tag: str,
) -> Iterator[Pool]:
    """Provision one worker pool (tagged ``run_tag``), waiting until it is
    ready with ``wait_until_ready``, and shut it down on teardown."""
    cfg = system_config
    name = f"{run_tag}-pool"
    crtu = ComputeRequirementTemplateUsage(
        templateId=cfg.compute_requirement_template,
        requirementName=name,
        requirementNamespace=cfg.namespace,
        targetInstanceCount=cfg.max_instances,
        imagesId=_image_family_id(yd_client, cfg.image_family),
        userData=cfg.user_data,
    )
    pwpp = ProvisionedWorkerPoolProperties(
        createNodeWorkers=NodeWorkerTarget(
            targetCount=1, targetType=NodeWorkerTargetType.PER_VCPU
        ),
        # Cost bounds in case a run is interrupted before cleanup
        idleNodeShutdown=AutoShutdown(enabled=True, timeout=timedelta(minutes=10)),
        idlePoolShutdown=AutoShutdown(enabled=True, timeout=timedelta(minutes=30)),
        minNodes=0,
        maxNodes=cfg.max_instances,
        workerTag=run_tag,
    )
    pushed: dict[str, Any] = {}
    ctx = make_context(dag_id=DAG_ID, run_id=f"{run_tag}-pool", pushed=pushed)
    op = ProvisionWorkerPool(
        task_id="provision",
        connection_id=CONN_ID,
        compute_requirement_template_usage=crtu,
        provisioned_worker_pool_properties=pwpp,
        # An idle pool never reaches RUNNING (that needs a claimed worker), so
        # "ready" is IDLE or RUNNING. If the wait fails, the operator itself
        # shuts the pool down (and the sweep is the backstop).
        wait_until_ready=True,
        poke_interval=cfg.poke_interval_seconds,
        timeout=cfg.sensor_timeout_seconds,
        deferrable=False,
    )
    render(op, ctx)
    pool_id = cast(str, op.execute(ctx))
    try:
        status = _pool_status(yd_client, pool_id)
        yield Pool(
            id=pool_id,
            name=name,
            status_after_return=status,
            pushed_id=pushed.get("return_value"),
        )
    finally:
        # Best-effort; the pool is normally already shut down by the shutdown
        # tests
        with suppress(Exception):
            _run(
                ShutdownProvisionedWorkerPool(
                    task_id="pool-teardown",
                    connection_id=CONN_ID,
                    worker_pool_id=pool_id,
                    terminate_immediately=True,
                )
            )
        _await_pool_finished(yd_client, pool_id, cfg)


def test_hook_test_connection(yd_connection_env: str) -> None:
    ok, message = YellowDogHook(connection_id=yd_connection_env).test_connection()
    assert ok, message


def test_provision_and_wait_running(
    pool: Pool, yd_client: PlatformClient, run_tag: str
) -> None:
    # The fixture provisioned the pool and waited (wait_until_ready) for
    # IDLE/RUNNING
    fetched = yd_client.worker_pool_client.get_worker_pool_by_id(pool.id)
    assert fetched.status in POOL_READY
    assert run_tag in pool.name


def test_add_work_requirement_incremental(
    pool: Pool,
    system_config: SystemTestConfig,
    yd_client: PlatformClient,
    run_tag: str,
) -> None:
    cfg = system_config
    wr_id = _new_wr(cfg, run_tag, "wr-incremental")
    with _cancelling(wr_id):
        group_ids = _run(
            AddTaskGroupsToWorkRequirement(
                task_id="add-groups",
                connection_id=CONN_ID,
                work_requirement_id=wr_id,
                task_groups=_groups(cfg, run_tag, ["task-group-1", "task-group-2"]),
            )
        )
        assert len(group_ids) == 2

        by_id = _run(
            AddTasksToTaskGroup(
                task_id="add-tasks-by-id",
                connection_id=CONN_ID,
                task_group_id=group_ids[0],
                tasks=_tasks(cfg, ["task-1"]),
            )
        )
        by_name = _run(
            AddTasksToTaskGroup(
                task_id="add-tasks-by-name",
                connection_id=CONN_ID,
                namespace=cfg.namespace,
                work_requirement_name=f"{run_tag}-wr-incremental",
                task_group_name="task-group-2",
                tasks=_tasks(cfg, ["task-1"]),
            )
        )
        assert len(by_id) == 1 and len(by_name) == 1

        _wait(
            WorkRequirementStateSensor(
                task_id="wr-completed",
                work_requirement_id=wr_id,
                target_states=[WorkRequirementStatus.COMPLETED],
                **_sensor_kwargs(cfg),
            ),
            cfg,
        )
        assert _fetch_wr(yd_client, wr_id).status == WorkRequirementStatus.COMPLETED


def test_task_group_order_preserved_incremental(
    yd_connection_env: str,
    system_config: SystemTestConfig,
    yd_client: PlatformClient,
    run_tag: str,
) -> None:
    cfg = system_config
    wr_id = _new_wr(cfg, run_tag, "wr-order-incremental")
    with _cancelling(wr_id):
        _run(
            AddTaskGroupsToWorkRequirement(
                task_id="add-groups",
                connection_id=CONN_ID,
                work_requirement_id=wr_id,
                task_groups=_groups(cfg, run_tag, GROUP_NAMES),
            )
        )
        fetched = _fetch_wr(yd_client, wr_id)
        assert [g.name for g in fetched.taskGroups or []] == GROUP_NAMES


def test_add_populated_task_group_order(
    yd_connection_env: str,
    system_config: SystemTestConfig,
    yd_client: PlatformClient,
    run_tag: str,
) -> None:
    cfg = system_config
    name = f"{run_tag}-wr-populated"
    wr_id: str = _run(
        AddPopulatedWorkRequirement(
            task_id="add-populated",
            connection_id=CONN_ID,
            work_requirement=WorkRequirement(namespace=cfg.namespace, name=name),
            task_groups_and_tasks=[
                (group, _tasks(cfg, ["task-1"]))
                for group in _groups(cfg, run_tag, GROUP_NAMES)
            ],
        )
    )
    with _cancelling(wr_id):
        fetched = _fetch_wr(yd_client, wr_id)
        assert [g.name for g in fetched.taskGroups or []] == GROUP_NAMES


def test_duplicate_task_group_name_rejected(
    yd_connection_env: str, system_config: SystemTestConfig, run_tag: str
) -> None:
    cfg = system_config
    wr_id = _new_wr(cfg, run_tag, "wr-duplicate")
    with _cancelling(wr_id):
        op = AddTaskGroupsToWorkRequirement(
            task_id="add-dup",
            connection_id=CONN_ID,
            work_requirement_id=wr_id,
            task_groups=_groups(cfg, run_tag, ["dup"]),
        )
        _run(op)
        with pytest.raises(InvalidRequestException):
            _run(op)


def test_cancel_idempotent(
    pool: Pool, system_config: SystemTestConfig, run_tag: str
) -> None:
    cfg = system_config
    wr_id = _new_wr(cfg, run_tag, "wr-cancel")
    with _cancelling(wr_id):
        _run(
            AddTaskGroupsToWorkRequirement(
                task_id="add-groups",
                connection_id=CONN_ID,
                work_requirement_id=wr_id,
                task_groups=_groups(cfg, run_tag, ["task-group-1"]),
            )
        )
        _cancel(wr_id)
        _cancel(wr_id)  # Must not raise


def test_sensor_fails_on_failure_state(
    yd_connection_env: str, system_config: SystemTestConfig, run_tag: str
) -> None:
    """A cancelled work requirement fails a sensor waiting for COMPLETED."""
    cfg = system_config
    wr_id = _new_wr(cfg, run_tag, "wr-failure-state")
    with _cancelling(wr_id):
        _run(
            AddTaskGroupsToWorkRequirement(
                task_id="add-groups",
                connection_id=CONN_ID,
                work_requirement_id=wr_id,
                task_groups=_groups(cfg, run_tag, ["task-group-1"]),
            )
        )
        _cancel(wr_id)
        sensor = WorkRequirementStateSensor(
            task_id="wr-failure-state",
            work_requirement_id=wr_id,
            target_states=[WorkRequirementStatus.COMPLETED],
            **_sensor_kwargs(cfg),
        )
        # Pokes return False while CANCELLING, then raise once CANCELLED
        with pytest.raises(YellowDogStateError) as exc_info:
            _wait(sensor, cfg)
        assert exc_info.value.resource_id == wr_id
        assert exc_info.value.current_state == WorkRequirementStatus.CANCELLED


###############################################################################
# RunWorkRequirement and the workflow features of the worker pool operators


def _run_context(
    run_tag: str,
    prefixes: list[str],
    run: str,
    *,
    task_id: str = "run-wr",
    try_number: int = 1,
    max_tries: int = 0,
    pushed: dict[str, Any] | None = None,
) -> Context:
    """
    A task context for DAG run ``<run_tag>-<run>`` of DAG ``DAG_ID``, whose
    tag prefix is registered with the sweep.
    """
    ctx = make_context(
        dag_id=DAG_ID,
        run_id=f"{run_tag}-{run}",
        task_id=task_id,
        try_number=try_number,
        max_tries=max_tries,
        pushed=pushed,
    )
    prefixes.append(RunStamp.from_context(ctx).run_tag_prefix())
    return ctx


def _execute(op: Any, ctx: Context) -> Any:
    render(op, ctx)
    return op.execute(ctx)


def _one_task_group(
    cfg: SystemTestConfig, worker_tag: str, arguments: Sequence[str]
) -> list[tuple[TaskGroup, list[Task]]]:
    """One task group of one task, run by workers tagged ``worker_tag``."""
    group = TaskGroup(
        name="g1",
        runSpecification=RunSpecification(
            taskTypes=[cfg.task_type], workerTags=[worker_tag]
        ),
        # A failed task fails the work requirement at once
        finishIfAnyTaskFailed=True,
    )
    task = Task(name="task-1", taskType=cfg.task_type, arguments=list(arguments))
    return [(group, [task])]


def _run_op(
    cfg: SystemTestConfig,
    name: str,
    worker_tag: str,
    arguments: Sequence[str],
    *,
    timeout: float | None = None,
    task_id: str = "run-wr",
) -> RunWorkRequirement:
    return RunWorkRequirement(
        task_id=task_id,
        connection_id=CONN_ID,
        work_requirement=WorkRequirement(namespace=cfg.namespace, name=name),
        task_groups_and_tasks=_one_task_group(cfg, worker_tag, arguments),
        poke_interval=cfg.poke_interval_seconds,
        timeout=cfg.sensor_timeout_seconds if timeout is None else timeout,
        deferrable=False,
    )


def _no_worker_tag(run_tag: str) -> str:
    """A worker tag no worker has: work with it stays unfinished (no cost)."""
    return f"{run_tag}-noworker"


def test_run_work_requirement_completes(
    pool: Pool,
    system_config: SystemTestConfig,
    yd_client: PlatformClient,
    run_tag: str,
    sweep_run_tag_prefixes: list[str],
) -> None:
    """Submit, wait until COMPLETED; a repeat of the same try does not re-run."""
    cfg = system_config
    name = f"{run_tag}-run-complete"
    pushed: dict[str, Any] = {}
    ctx = _run_context(run_tag, sweep_run_tag_prefixes, "complete", pushed=pushed)
    wr_id = _execute(_run_op(cfg, name, run_tag, cfg.task_arguments), ctx)
    with _cancelling(wr_id):
        assert pushed["return_value"] == wr_id
        fetched = _fetch_wr(yd_client, wr_id)
        assert fetched.status == WorkRequirementStatus.COMPLETED
        assert fetched.name == name
        assert fetched.tag == attempt_tag(RunStamp.from_context(ctx), 0)
        # The same task instance again (e.g. its worker died after the work
        # completed): the completed work requirement is returned, not re-run
        again = _execute(_run_op(cfg, name, run_tag, cfg.task_arguments), ctx)
        assert again == wr_id


def test_run_work_requirement_failure_state(
    pool: Pool,
    system_config: SystemTestConfig,
    yd_client: PlatformClient,
    run_tag: str,
    sweep_run_tag_prefixes: list[str],
) -> None:
    """A task that exits non-zero fails the work requirement and the task."""
    cfg = system_config
    name = f"{run_tag}-run-failure"
    ctx = _run_context(run_tag, sweep_run_tag_prefixes, "failure")
    failing = task_arguments_or_skip(cfg, "failing_task_arguments")
    op = _run_op(cfg, name, run_tag, failing)
    with pytest.raises(YellowDogStateError) as exc_info:
        _execute(op, ctx)
    wr_id = cast(str, exc_info.value.resource_id)
    with _cancelling(wr_id):
        assert exc_info.value.current_state == WorkRequirementStatus.FAILED
        fetched = _fetch_wr(yd_client, wr_id)
        assert fetched.name == name
        assert fetched.status == WorkRequirementStatus.FAILED


def test_run_retry_after_failed_creates_try_copy(
    pool: Pool,
    system_config: SystemTestConfig,
    yd_client: PlatformClient,
    run_tag: str,
    sweep_run_tag_prefixes: list[str],
) -> None:
    """
    Try 1 fails; try 2 (an automatic retry: same clear cycle, i.e. the same
    ``max_tries``) submits a fresh ``-try2`` copy, which completes.
    """
    cfg = system_config
    failing = task_arguments_or_skip(cfg, "failing_task_arguments")
    name = f"{run_tag}-run-retry"

    def ctx(try_number: int) -> Context:
        return _run_context(
            run_tag,
            sweep_run_tag_prefixes,
            "retry",
            try_number=try_number,
            max_tries=1,  # retries=1: unchanged across automatic retries
        )

    with pytest.raises(YellowDogStateError) as exc_info:
        _execute(_run_op(cfg, name, run_tag, failing), ctx(1))
    first_id = cast(str, exc_info.value.resource_id)
    with _cancelling(first_id):
        assert exc_info.value.current_state == WorkRequirementStatus.FAILED
        # The retry runs work that succeeds (the task's arguments are not part
        # of the earlier attempt's identity)
        second_id = _execute(_run_op(cfg, name, run_tag, cfg.task_arguments), ctx(2))
        with _cancelling(second_id):
            assert second_id != first_id
            second = _fetch_wr(yd_client, second_id)
            assert second.name == try_name(name, 2)
            assert second.status == WorkRequirementStatus.COMPLETED
            assert second.tag == attempt_tag(RunStamp.from_context(ctx(2)), 1)
            first = _fetch_wr(yd_client, first_id)
            assert first.name == name
            assert first.status == WorkRequirementStatus.FAILED


def test_run_name_collision_other_stamp_fresh_copy(
    pool: Pool,
    system_config: SystemTestConfig,
    yd_client: PlatformClient,
    run_tag: str,
    sweep_run_tag_prefixes: list[str],
) -> None:
    """
    A work requirement with the name, submitted by another DAG run, is never
    adopted or touched: a fresh ``-try1`` copy is submitted instead.
    """
    cfg = system_config
    name = f"{run_tag}-run-collision"
    other_ctx = _run_context(
        run_tag, sweep_run_tag_prefixes, "collision-other", task_id="add-wr"
    )
    other_id: str = _execute(
        AddWorkRequirement(
            task_id="add-wr",
            connection_id=CONN_ID,
            work_requirement=WorkRequirement(namespace=cfg.namespace, name=name),
        ),
        other_ctx,
    )
    with _cancelling(other_id):
        other_before = _fetch_wr(yd_client, other_id)
        ctx = _run_context(run_tag, sweep_run_tag_prefixes, "collision")
        wr_id = _execute(_run_op(cfg, name, run_tag, cfg.task_arguments), ctx)
        with _cancelling(wr_id):
            assert wr_id != other_id
            fetched = _fetch_wr(yd_client, wr_id)
            assert fetched.name == try_name(name, 1)
            assert fetched.status == WorkRequirementStatus.COMPLETED
            other_after = _fetch_wr(yd_client, other_id)
            assert other_after.name == name
            assert other_after.tag == other_before.tag
            assert other_after.status == other_before.status
            assert other_after.status not in WR_CANCELLED


def test_run_own_timeout_cancels(
    yd_connection_env: str,
    system_config: SystemTestConfig,
    yd_client: PlatformClient,
    run_tag: str,
    sweep_run_tag_prefixes: list[str],
) -> None:
    """On its own timeout the operator cancels the work requirement."""
    cfg = system_config
    name = f"{run_tag}-run-timeout"
    pushed: dict[str, Any] = {}
    ctx = _run_context(run_tag, sweep_run_tag_prefixes, "timeout", pushed=pushed)
    op = _run_op(
        cfg,
        name,
        _no_worker_tag(run_tag),
        cfg.task_arguments,
        timeout=2 * cfg.poke_interval_seconds,
    )
    with pytest.raises(WaitTimeoutError) as exc_info:
        _execute(op, ctx)
    wr_id = cast(str, pushed["return_value"])
    with _cancelling(wr_id):
        assert exc_info.value.resource_id == wr_id
        assert _fetch_wr(yd_client, wr_id).status in WR_CANCELLED


def test_run_on_kill_cancels(
    yd_connection_env: str,
    system_config: SystemTestConfig,
    yd_client: PlatformClient,
    run_tag: str,
    sweep_run_tag_prefixes: list[str],
) -> None:
    """
    Killing the task while it waits (Airflow calls ``on_kill`` from another
    thread) cancels the work requirement; the wait then fails on CANCELLED.
    """
    cfg = system_config
    name = f"{run_tag}-run-on-kill"
    pushed: dict[str, Any] = {}
    ctx = _run_context(run_tag, sweep_run_tag_prefixes, "on-kill", pushed=pushed)
    op = _run_op(cfg, name, _no_worker_tag(run_tag), cfg.task_arguments)
    render(op, ctx)
    errors: list[BaseException] = []

    def execute() -> None:
        try:
            op.execute(ctx)
        except BaseException as e:
            errors.append(e)

    thread = threading.Thread(target=execute, daemon=True)
    thread.start()
    deadline = time.monotonic() + cfg.sensor_timeout_seconds
    while "return_value" not in pushed and thread.is_alive():
        if time.monotonic() >= deadline:
            pytest.fail("RunWorkRequirement did not submit its work requirement")
        time.sleep(1)
    assert "return_value" in pushed, f"execute ended early: {errors}"
    wr_id = cast(str, pushed["return_value"])
    with _cancelling(wr_id):
        assert thread.is_alive(), f"execute ended early: {errors}"
        op.on_kill()
        thread.join(timeout=cfg.sensor_timeout_seconds)
        assert not thread.is_alive(), "execute still waiting after on_kill"
        assert _fetch_wr(yd_client, wr_id).status in WR_CANCELLED
        assert len(errors) == 1 and isinstance(errors[0], YellowDogStateError)
        assert errors[0].current_state == WorkRequirementStatus.CANCELLED


###############################################################################
# Shutdown: these tests shut the shared pool down, so they must stay last


def test_provision_wait_until_ready_and_shutdown_wait(
    pool: Pool,
    system_config: SystemTestConfig,
    yd_client: PlatformClient,
    run_tag: str,
    sweep_run_tag_prefixes: list[str],
) -> None:
    """
    ``ProvisionWorkerPool(wait_until_ready=True)`` (the pool fixture) pushed
    the pool's ID, and the pool was ready when checked just after it
    returned; ``ShutdownProvisionedWorkerPool(wait=True)`` returns only once
    the pool has shut down and its compute requirement has terminated.
    """
    cfg = system_config
    assert pool.status_after_return in POOL_READY
    assert pool.pushed_id == pool.id
    fetched = yd_client.worker_pool_client.get_worker_pool_by_id(pool.id)
    assert isinstance(fetched, ProvisionedWorkerPool)
    cr_id = cast(str, fetched.computeRequirementId)

    pushed: dict[str, Any] = {}
    # A DAG run with no work requirements: the whole-run cancel finds none
    ctx = _run_context(
        run_tag, sweep_run_tag_prefixes, "shutdown", task_id="shutdown", pushed=pushed
    )
    returned = _execute(
        ShutdownProvisionedWorkerPool(
            task_id="shutdown",
            connection_id=CONN_ID,
            worker_pool_id=pool.id,
            terminate_immediately=True,
            wait=True,
            poke_interval=cfg.poke_interval_seconds,
            timeout=cfg.sensor_timeout_seconds,
            deferrable=False,
            cancel_work_requirements="run",
        ),
        ctx,
    )
    assert returned == pool.id
    assert pushed["return_value"] == pool.id
    assert _pool_status(yd_client, pool.id) in POOL_FINISHED
    cr = yd_client.compute_client.get_compute_requirement_by_id(cr_id)
    assert cr.status == ComputeRequirementStatus.TERMINATED


def test_teardown_cancels_only_this_runs_work_requirements(
    pool: Pool,
    system_config: SystemTestConfig,
    yd_client: PlatformClient,
    yd_connection_env: str,
    run_tag: str,
    sweep_run_tag_prefixes: list[str],
) -> None:
    """
    The teardown with ``cancel_work_requirements="run"`` cancels the
    unfinished work requirements of its own DAG run only. The pool is already
    shut down by the previous test, which the teardown tolerates (as on a
    retry).

    Depends on order: run it after
    ``test_provision_wait_until_ready_and_shutdown_wait``. Run alone, it shuts
    the shared pool down itself.
    """
    cfg = system_config
    groups = _one_task_group(cfg, _no_worker_tag(run_tag), cfg.task_arguments)
    ours_ctx = _run_context(run_tag, sweep_run_tag_prefixes, "teardown-ours")
    theirs_ctx = _run_context(run_tag, sweep_run_tag_prefixes, "teardown-theirs")
    with YellowDogHook(connection_id=yd_connection_env) as hook:
        ours = hook.submit_work_requirement(
            WorkRequirement(namespace=cfg.namespace, name=f"{run_tag}-td-ours"),
            groups,
            stamp=RunStamp.from_context(ours_ctx),
            try_number=1,
            cycle=0,
        ).work_requirement_id
        with _cancelling(ours):
            theirs = hook.submit_work_requirement(
                WorkRequirement(namespace=cfg.namespace, name=f"{run_tag}-td-theirs"),
                groups,
                stamp=RunStamp.from_context(theirs_ctx),
                try_number=1,
                cycle=0,
            ).work_requirement_id
            with _cancelling(theirs):
                teardown_ctx = _run_context(
                    run_tag, sweep_run_tag_prefixes, "teardown-ours", task_id="teardown"
                )
                _execute(
                    ShutdownProvisionedWorkerPool(
                        task_id="teardown",
                        connection_id=CONN_ID,
                        worker_pool_id=pool.id,
                        cancel_work_requirements="run",
                    ),
                    teardown_ctx,
                )
                assert _fetch_wr(yd_client, ours).status in WR_CANCELLED
                status = _fetch_wr(yd_client, theirs).status
                assert status is not None and not status.finished
                assert status not in WR_CANCELLED


def test_shutdown_cancels_own_scope_only(
    pool: Pool,
    system_config: SystemTestConfig,
    yd_client: PlatformClient,
    yd_connection_env: str,
    run_tag: str,
    sweep_run_tag_prefixes: list[str],
) -> None:
    """
    In a DAG with two parallel setup/teardown scopes, s1 >> w1 >> t1 and
    s2 >> w2 >> t2, the teardown t1 (``cancel_work_requirements="scope"``,
    the default) cancels the unfinished work requirement of w1 only, not that
    of w2 in the same DAG run. The pool is already shut down, which the
    teardown tolerates.

    Depends on order: run it after
    ``test_provision_wait_until_ready_and_shutdown_wait``. Run alone, it shuts
    the shared pool down itself.
    """
    cfg = system_config
    with DAG(DAG_ID, schedule=None):
        s1, w1, s2, w2 = (NoOp(task_id=i) for i in ("s1", "w1", "s2", "w2"))
        t1 = ShutdownProvisionedWorkerPool(
            task_id="t1", connection_id=CONN_ID, worker_pool_id=pool.id
        )
        t2 = ShutdownProvisionedWorkerPool(
            task_id="t2", connection_id=CONN_ID, worker_pool_id=pool.id
        )
        _ = s1 >> w1 >> t1.as_teardown(setups=s1)
        _ = s2 >> w2 >> t2.as_teardown(setups=s2)

    groups = _one_task_group(cfg, _no_worker_tag(run_tag), cfg.task_arguments)
    prefixes = sweep_run_tag_prefixes

    def submit(task_id: str) -> str:
        """Submit a work requirement stamped as ``task_id`` of this run."""
        ctx = _run_context(run_tag, prefixes, "scope", task_id=task_id)
        with YellowDogHook(connection_id=yd_connection_env) as hook:
            return hook.submit_work_requirement(
                WorkRequirement(
                    namespace=cfg.namespace, name=f"{run_tag}-scope-{task_id}"
                ),
                groups,
                stamp=RunStamp.from_context(ctx),
                try_number=1,
                cycle=0,
            ).work_requirement_id

    in_scope = submit("w1")
    with _cancelling(in_scope):
        out_of_scope = submit("w2")
        with _cancelling(out_of_scope):
            _execute(t1, _run_context(run_tag, prefixes, "scope", task_id="t1"))
            assert _fetch_wr(yd_client, in_scope).status in WR_CANCELLED
            status = _fetch_wr(yd_client, out_of_scope).status
            assert status is not None and not status.finished
            assert status not in WR_CANCELLED


def test_shutdown_terminate_and_sensors(
    pool: Pool, system_config: SystemTestConfig
) -> None:
    """
    Must run last, after the pool has been shut down: a repeated shutdown (as
    by a retried teardown) succeeds, and the sensors see the final states.
    """
    cfg = system_config
    _run(
        ShutdownProvisionedWorkerPool(
            task_id="shutdown",
            connection_id=CONN_ID,
            worker_pool_id=pool.id,
            terminate_immediately=True,
        )
    )
    _wait(
        WorkerPoolStateSensor(
            task_id="pool-shutdown",
            worker_pool_id=pool.id,
            target_states=[WorkerPoolStatus.SHUTDOWN, WorkerPoolStatus.TERMINATED],
            **_sensor_kwargs(cfg),
        ),
        cfg,
    )
    _wait(
        ComputeRequirementStateSensor(
            task_id="cr-terminated",
            namespace=cfg.namespace,
            compute_requirement_name=pool.name,
            target_states=[ComputeRequirementStatus.TERMINATED],
            **_sensor_kwargs(cfg),
        ),
        cfg,
    )
