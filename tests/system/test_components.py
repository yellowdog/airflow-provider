"""
Live-platform component tests: each operator, sensor and the hook against the
real YellowDog platform. Tests that need workers share one provisioned worker
pool and run in file order; the shutdown test must stay last.

Every resource name contains the run tag so the ``yd_client`` sweep can find
and remove anything left behind.
"""

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, cast

import pytest
from yellowdog_client.model import (
    AutoShutdown,
    ComputeRequirementStatus,
    ComputeRequirementTemplateUsage,
    MachineImageFamilySearch,
    NodeWorkerTarget,
    NodeWorkerTargetType,
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

from tests.support import make_context, render
from tests.system.config import SystemTestConfig
from yellowdog_provider.exceptions.yellowdog_exceptions import YellowDogStateError
from yellowdog_provider.hooks.yellowdog_hooks import YellowDogHook
from yellowdog_provider.operators.yellowdog_operators import (
    AddPopulatedWorkRequirement,
    AddTaskGroupsToWorkRequirement,
    AddTasksToTaskGroup,
    AddWorkRequirement,
    CancelWorkRequirement,
    ProvisionWorkerPool,
    ShutdownProvisionedWorkerPool,
)
from yellowdog_provider.sensors.yellowdog_sensors import (
    ComputeRequirementStateSensor,
    WorkerPoolStateSensor,
    WorkRequirementStateSensor,
)

pytestmark = pytest.mark.system

CONN_ID = "yd_system_test"
GROUP_NAMES = ["g1", "g2", "g3"]


log = logging.getLogger(__name__)

# Pool states from which IDLE/RUNNING can no longer be reached
POOL_FINISHED = {WorkerPoolStatus.SHUTDOWN, WorkerPoolStatus.TERMINATED}


def _run(op: Any) -> Any:
    ctx = make_context()
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


@pytest.fixture(scope="module")
def pool(
    system_config: SystemTestConfig,
    yd_client: PlatformClient,
    yd_connection_env: str,
    run_tag: str,
) -> Iterator[Pool]:
    """Provision one worker pool (tagged ``run_tag``), wait until ready, and
    shut it down on teardown."""
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
    pool_id = cast(
        str,
        _run(
            ProvisionWorkerPool(
                task_id="provision",
                connection_id=CONN_ID,
                compute_requirement_template_usage=crtu,
                provisioned_worker_pool_properties=pwpp,
            )
        ),
    )
    try:
        # An idle pool never reaches RUNNING (that needs a claimed worker)
        _wait(
            WorkerPoolStateSensor(
                task_id="pool-ready",
                worker_pool_id=pool_id,
                target_states=[WorkerPoolStatus.IDLE, WorkerPoolStatus.RUNNING],
                **_sensor_kwargs(cfg),
            ),
            cfg,
        )
        yield Pool(id=pool_id, name=name)
    finally:
        # Best-effort; the pool may already be shut down by the shutdown test
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
    # The fixture provisioned the pool and waited (with the sensor) for IDLE/RUNNING
    fetched = yd_client.worker_pool_client.get_worker_pool_by_id(pool.id)
    assert fetched.status in {WorkerPoolStatus.IDLE, WorkerPoolStatus.RUNNING}
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


def test_shutdown_terminate_and_sensors(
    pool: Pool, system_config: SystemTestConfig
) -> None:
    """Must run last: it tears down the shared pool."""
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
