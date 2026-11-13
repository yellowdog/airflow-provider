from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import timedelta
from itertools import chain, repeat
from typing import Any, cast
from unittest.mock import MagicMock, call, patch

import pytest
from airflow.sdk import DAG, TaskGroup, task_group
from requests import HTTPError
from yellowdog_client.model import (
    ComputeRequirementStatus,
    ComputeRequirementTemplateUsage,
    ConfiguredWorkerPool,
    ProvisionedWorkerPool,
    WorkerPool,
    WorkerPoolStatus,
    WorkRequirementStatus,
)
from yellowdog_client.model.exceptions import InvalidRequestException

from tests.support import NoOp, make_context, render
from tests.unit.hooks.test_hook_operations import FakePlatform
from yellowdog_provider.exceptions.yellowdog_exceptions import (
    WaitTimeoutError,
    YellowDogArgumentError,
    YellowDogException,
    YellowDogStateError,
)
from yellowdog_provider.hooks.yellowdog_hooks import YellowDogHook
from yellowdog_provider.operators import yellowdog_operators as ops
from yellowdog_provider.operators.yellowdog_operators import (
    ProvisionWorkerPool,
    ShutdownProvisionedWorkerPool,
)
from yellowdog_provider.triggers.yellowdog_triggers import YellowDogStateTrigger
from yellowdog_provider.utils import airflow_compat
from yellowdog_provider.utils.airflow_compat import TaskDeferred
from yellowdog_provider.utils.run_stamp import RunStamp


def _with(obj: Any, **attrs: Any) -> Any:
    for key, value in attrs.items():
        setattr(obj, key, value)
    return obj


def _crtu() -> ComputeRequirementTemplateUsage:
    return ComputeRequirementTemplateUsage(
        templateId="tmpl-1", requirementNamespace="ns", requirementName="req"
    )


def _pool(cls: type = ProvisionedWorkerPool, **kw: Any) -> Any:
    return _with(cls(), id="wp-1", namespace="ns", name="pool", **kw)


def _run(op: Any) -> Any:
    ctx = make_context()
    render(op, ctx)
    return op.execute(ctx)


def test_provision_without_properties(mock_client: MagicMock) -> None:
    mock_client.worker_pool_client.provision_worker_pool.return_value = _pool()
    crtu = _crtu()
    op = ProvisionWorkerPool(task_id="t", compute_requirement_template_usage=crtu)
    _run(op)
    mock_client.worker_pool_client.provision_worker_pool.assert_called_once_with(
        crtu, None
    )


def test_provision_returns_id(mock_client: MagicMock) -> None:
    mock_client.worker_pool_client.provision_worker_pool.return_value = _pool()
    op = ProvisionWorkerPool(task_id="t", compute_requirement_template_usage=_crtu())
    assert _run(op) == "wp-1"


def test_provision_positional_args_rejected() -> None:
    with pytest.raises(TypeError):
        ProvisionWorkerPool("t", _crtu())  # type: ignore[misc]


def test_shutdown_by_id(mock_client: MagicMock) -> None:
    mock_client.worker_pool_client.get_worker_pool_by_id.return_value = _pool()
    op = ShutdownProvisionedWorkerPool(task_id="t", worker_pool_id="wp-1")
    assert _run(op) == "wp-1"
    mock_client.worker_pool_client.shutdown_worker_pool_by_id.assert_called_once_with(
        "wp-1"
    )
    mock_client.compute_client.terminate_compute_requirement_by_id.assert_not_called()


def test_shutdown_by_name(mock_client: MagicMock) -> None:
    mock_client.worker_pool_client.get_worker_pool_by_name.return_value = _pool()
    mock_client.worker_pool_client.get_worker_pool_by_id.return_value = _pool()
    op = ShutdownProvisionedWorkerPool(
        task_id="t", namespace="ns", worker_pool_name="pool"
    )
    assert _run(op) == "wp-1"
    mock_client.worker_pool_client.get_worker_pool_by_name.assert_called_once_with(
        "ns", "pool"
    )


def test_shutdown_requires_id_or_name() -> None:
    with pytest.raises(YellowDogArgumentError):
        ShutdownProvisionedWorkerPool(task_id="t", namespace="ns")


def test_shutdown_terminate_immediately(mock_client: MagicMock) -> None:
    mock_client.worker_pool_client.get_worker_pool_by_id.return_value = _pool(
        computeRequirementId="cr-1"
    )
    op = ShutdownProvisionedWorkerPool(
        task_id="t", worker_pool_id="wp-1", terminate_immediately=True
    )
    _run(op)
    mock_client.compute_client.terminate_compute_requirement_by_id.assert_called_once_with(
        "cr-1"
    )


def test_shutdown_configured_pool_raises(mock_client: MagicMock) -> None:
    mock_client.worker_pool_client.get_worker_pool_by_id.return_value = _pool(
        ConfiguredWorkerPool
    )
    op = ShutdownProvisionedWorkerPool(task_id="t", worker_pool_id="wp-1")
    with pytest.raises(YellowDogException, match="configured worker pool"):
        _run(op)
    mock_client.worker_pool_client.shutdown_worker_pool_by_id.assert_not_called()


def test_shutdown_unknown_pool_type_raises(mock_client: MagicMock) -> None:
    mock_client.worker_pool_client.get_worker_pool_by_id.return_value = _pool(
        WorkerPool
    )
    op = ShutdownProvisionedWorkerPool(task_id="t", worker_pool_id="wp-1")
    with pytest.raises(YellowDogException):
        _run(op)
    mock_client.worker_pool_client.shutdown_worker_pool_by_id.assert_not_called()


def test_id_rendering_to_None_raises(mock_client: MagicMock) -> None:
    op = ShutdownProvisionedWorkerPool(
        task_id="t", worker_pool_id="{{ ti.xcom_pull(task_ids='missing') }}"
    )
    with pytest.raises(YellowDogArgumentError, match="upstream XCom"):
        _run(op)
    mock_client.worker_pool_client.get_worker_pool_by_id.assert_not_called()


# == setup/teardown behaviour ================================================

WP = WorkerPoolStatus
CR = ComputeRequirementStatus
STAMP = RunStamp("test_dag", "test_run", "run", -1)  # Another task, this run


_REAL_SHUTDOWN = YellowDogHook.shutdown_worker_pool


def _use_real_shutdown(pool_fake: "PoolFake") -> None:
    """Route the fake's shutdown to the real hook method (its tolerances)."""
    pool_fake.shutdown.side_effect = lambda pool_id, *, terminate_immediately: (
        _REAL_SHUTDOWN(
            YellowDogHook(), pool_id, terminate_immediately=terminate_immediately
        )
    )


@dataclass
class PoolFake:
    """Stand-in hook methods for the pool operators, patched onto the hook."""

    provision: MagicMock
    shutdown: MagicMock
    pool_state: MagicMock
    cr_state: MagicMock
    pushed: dict[str, Any] = field(default_factory=dict[str, Any])


@pytest.fixture
def pool_fake(mock_client: MagicMock) -> Iterator[PoolFake]:
    provision = MagicMock(return_value="wp-1")
    shutdown = MagicMock(
        side_effect=lambda _id, *, terminate_immediately: (
            "cr-1" if terminate_immediately else None
        )
    )
    pool_state = MagicMock(return_value=WP.IDLE)
    cr_state = MagicMock(return_value=CR.TERMINATED)
    with (
        patch.object(YellowDogHook, "provision_worker_pool", provision),
        patch.object(YellowDogHook, "shutdown_worker_pool", shutdown),
        patch.object(YellowDogHook, "get_worker_pool_state", pool_state),
        patch.object(YellowDogHook, "get_compute_requirement_state", cr_state),
    ):
        yield PoolFake(provision, shutdown, pool_state, cr_state)


def _provision(**kw: Any) -> ProvisionWorkerPool:
    kw.setdefault("compute_requirement_template_usage", _crtu())
    kw.setdefault("poke_interval", 0)
    kw.setdefault("deferrable", False)
    return ProvisionWorkerPool(task_id="t", **kw)


def _exec(op: Any, pushed: dict[str, Any] | None = None) -> Any:
    ctx = make_context(pushed=pushed)
    render(op, ctx)
    return op.execute(ctx)


def test_provision_defaults() -> None:
    with patch.object(airflow_compat.conf, "getboolean", return_value=False):
        op = ProvisionWorkerPool(
            task_id="t", compute_requirement_template_usage=_crtu()
        )
    assert op.wait_until_ready is False
    assert (op.poke_interval, op.timeout, op.deferrable) == (30, 3600, False)


def test_provision_without_wait_does_not_check_state(pool_fake: PoolFake) -> None:
    assert _exec(_provision()) == "wp-1"
    pool_fake.pool_state.assert_not_called()
    pool_fake.shutdown.assert_not_called()


def test_provision_wait_until_ready_returns_on_idle(pool_fake: PoolFake) -> None:
    pool_fake.pool_state.side_effect = [WP.PENDING, WP.CONFIGURING, WP.EMPTY, WP.IDLE]
    pushed: dict[str, Any] = {}
    assert _exec(_provision(wait_until_ready=True), pushed) == "wp-1"
    assert pushed["return_value"] == "wp-1"
    assert pool_fake.pool_state.call_count == 4
    pool_fake.pool_state.assert_called_with("wp-1")
    pool_fake.shutdown.assert_not_called()


def test_provision_wait_until_ready_accepts_running(pool_fake: PoolFake) -> None:
    pool_fake.pool_state.return_value = WP.RUNNING
    assert _exec(_provision(wait_until_ready=True)) == "wp-1"


@pytest.mark.parametrize("state", [WP.SHUTDOWN, WP.TERMINATED])
def test_provision_wait_failure_terminates_pool(
    pool_fake: PoolFake, state: WorkerPoolStatus
) -> None:
    pool_fake.pool_state.side_effect = [WP.PENDING, state]
    with pytest.raises(YellowDogStateError) as info:
        _exec(_provision(wait_until_ready=True))
    assert info.value.current_state == state
    pool_fake.shutdown.assert_called_once_with("wp-1", terminate_immediately=True)


def test_provision_wait_timeout_terminates_pool(pool_fake: PoolFake) -> None:
    pool_fake.pool_state.return_value = WP.EMPTY
    with pytest.raises(WaitTimeoutError):
        _exec(_provision(wait_until_ready=True, timeout=0))
    pool_fake.shutdown.assert_called_once_with("wp-1", terminate_immediately=True)


def test_provision_wait_error_terminates_and_cleanup_failure_not_masked(
    pool_fake: PoolFake,
) -> None:
    pool_fake.pool_state.side_effect = YellowDogException("status boom")
    pool_fake.shutdown.side_effect = RuntimeError("shutdown boom")
    with pytest.raises(YellowDogException, match="status boom"):
        _exec(_provision(wait_until_ready=True))
    pool_fake.shutdown.assert_called_once_with("wp-1", terminate_immediately=True)


class _Killed(BaseException):
    """Stands in for Airflow interrupting a running task (not an Exception)."""


def test_provision_killed_mid_wait_on_kill_terminates(pool_fake: PoolFake) -> None:
    pool_fake.pool_state.side_effect = [WP.PENDING, _Killed()]
    op = _provision(wait_until_ready=True)
    with pytest.raises(_Killed):
        _exec(op)
    pool_fake.shutdown.assert_not_called()  # BaseException: no cleanup in execute
    op.on_kill()
    pool_fake.shutdown.assert_called_once_with("wp-1", terminate_immediately=True)


def test_provision_on_kill_before_provision_does_nothing(pool_fake: PoolFake) -> None:
    _provision(wait_until_ready=True).on_kill()
    pool_fake.shutdown.assert_not_called()


def test_provision_on_kill_never_raises(pool_fake: PoolFake) -> None:
    pool_fake.pool_state.side_effect = [WP.PENDING, _Killed()]
    pool_fake.shutdown.side_effect = RuntimeError("platform down")
    op = _provision(wait_until_ready=True)
    with pytest.raises(_Killed):
        _exec(op)
    op.on_kill()  # Logged, not raised
    pool_fake.shutdown.assert_called_once()


@pytest.mark.parametrize("wait", [True, False])
def test_provision_on_kill_after_ready_leaves_pool(
    pool_fake: PoolFake, wait: bool
) -> None:
    op = _provision(wait_until_ready=wait)
    assert _exec(op) == "wp-1"
    op.on_kill()
    pool_fake.shutdown.assert_not_called()


def test_provision_on_kill_after_deferred_ready_leaves_pool(
    pool_fake: PoolFake,
) -> None:
    op = _provision(wait_until_ready=True, deferrable=True)
    op.execute_complete(
        make_context(), _pool_event("target", "IDLE"), worker_pool_id="wp-1"
    )
    op.on_kill()
    pool_fake.shutdown.assert_not_called()


def test_provision_defers_when_deferrable(pool_fake: PoolFake) -> None:
    pool_fake.pool_state.return_value = WP.PENDING
    pushed: dict[str, Any] = {}
    op = _provision(
        wait_until_ready=True,
        deferrable=True,
        poke_interval=15,
        timeout=900,
        connection_id="my_conn",
    )
    with patch(f"{ops.__name__}.time") as clock:
        clock.time.return_value = 1000.0
        with pytest.raises(TaskDeferred) as info:
            _exec(op, pushed)
    trigger = info.value.trigger
    assert isinstance(trigger, YellowDogStateTrigger)
    assert trigger.serialize()[1] == {
        "connection_id": "my_conn",
        "kind": "worker_pool",
        "resource_id": "wp-1",
        "target_states": ["IDLE", "RUNNING"],
        "failure_states": ["SHUTDOWN", "TERMINATED"],
        "poke_interval": 15.0,
        "end_time": 1900.0,
        "kill_action": "terminate_worker_pool",
    }
    assert info.value.method_name == "execute_complete"
    assert info.value.kwargs == {"worker_pool_id": "wp-1"}
    assert info.value.timeout == timedelta(seconds=900 + ops.DEFER_TIMEOUT_GRACE)
    assert pushed["return_value"] == "wp-1"
    pool_fake.shutdown.assert_not_called()


def test_provision_deferrable_ready_on_first_check(pool_fake: PoolFake) -> None:
    op = _provision(wait_until_ready=True, deferrable=True)
    with patch.object(ProvisionWorkerPool, "defer") as defer:
        assert _exec(op) == "wp-1"
    defer.assert_not_called()


def test_provision_deferrable_failure_on_first_check_terminates(
    pool_fake: PoolFake,
) -> None:
    pool_fake.pool_state.return_value = WP.SHUTDOWN
    with pytest.raises(YellowDogStateError):
        _exec(_provision(wait_until_ready=True, deferrable=True))
    pool_fake.shutdown.assert_called_once_with("wp-1", terminate_immediately=True)


def _pool_event(status: str, state: str | None, **extra: Any) -> dict[str, Any]:
    return {
        "status": status,
        "kind": "worker_pool",
        "resource_id": "wp-1",
        "state": state,
        "message": f"message for {status}",
        **extra,
    }


def test_provision_execute_complete_target(pool_fake: PoolFake) -> None:
    op = _provision(wait_until_ready=True)
    assert (
        op.execute_complete(
            make_context(), _pool_event("target", "IDLE"), worker_pool_id="wp-1"
        )
        == "wp-1"
    )
    pool_fake.shutdown.assert_not_called()


@pytest.mark.parametrize(
    "event, error",
    [
        (_pool_event("failure", "TERMINATED"), YellowDogStateError),
        (_pool_event("timeout", "EMPTY"), WaitTimeoutError),
        (_pool_event("error", None, error_type="HTTPError"), YellowDogException),
        (_pool_event("weird", None), YellowDogException),
    ],
)
def test_provision_execute_complete_failures_terminate(
    pool_fake: PoolFake, event: dict[str, Any], error: type[Exception]
) -> None:
    pool_fake.shutdown.side_effect = RuntimeError("cleanup failed")  # Not masking
    op = _provision(wait_until_ready=True, timeout=900)
    with pytest.raises(error) as info:
        op.execute_complete(make_context(), event, worker_pool_id="wp-1")
    if isinstance(info.value, WaitTimeoutError):
        assert info.value.timeout == 900
    pool_fake.shutdown.assert_called_once_with("wp-1", terminate_immediately=True)


# -- teardown ----------------------------------------------------------------


@pytest.fixture
def platform(mock_client: MagicMock) -> FakePlatform:
    mock_client.worker_pool_client.get_worker_pool_by_id.return_value = _pool(
        computeRequirementId="cr-1"
    )
    return FakePlatform(mock_client)


def _shutdown(**kw: Any) -> ShutdownProvisionedWorkerPool:
    kw.setdefault("task_id", "t")
    kw.setdefault("worker_pool_id", "wp-1")
    kw.setdefault("poke_interval", 0)
    kw.setdefault("deferrable", False)
    return ShutdownProvisionedWorkerPool(**kw)


def _cancelled(platform: FakePlatform) -> list[Any]:
    return platform.work.cancel_work_requirement_by_id.call_args_list


def test_shutdown_defaults() -> None:
    with patch.object(airflow_compat.conf, "getboolean", return_value=False):
        op = ShutdownProvisionedWorkerPool(task_id="t", worker_pool_id="wp-1")
    assert (op.wait, op.cancel_work_requirements) == (False, "scope")
    assert (op.poke_interval, op.timeout, op.deferrable) == (30, 3600, False)


def test_shutdown_cancels_unfinished_run_work_requirements(
    platform: FakePlatform, mock_client: MagicMock
) -> None:
    order: list[str] = []
    platform.work.cancel_work_requirement_by_id.side_effect = lambda i, a: (
        order.append(f"cancel {i}") or platform._cancel(i, a)
    )
    mock_client.worker_pool_client.shutdown_worker_pool_by_id.side_effect = lambda i: (
        order.append(f"shutdown {i}")
    )
    running = platform.existing("a", STAMP, WorkRequirementStatus.RUNNING)
    held = platform.existing("b", STAMP, WorkRequirementStatus.HELD, cycle=3)
    platform.existing("c", STAMP, WorkRequirementStatus.COMPLETED)
    assert _exec(_shutdown(cancel_work_requirements="run")) == "wp-1"
    assert sorted(_cancelled(platform)) == sorted(
        [call(running.id, True), call(held.id, True)]
    )
    assert order[-1] == "shutdown wp-1"
    assert platform.searches[-1].namespaces is None  # Every namespace


def test_teardown_cancels_only_this_runs_work(platform: FakePlatform) -> None:
    mine = platform.existing("mine", STAMP, WorkRequirementStatus.RUNNING)
    other_run = RunStamp("test_dag", "other_run", "run", -1)
    other_dag = RunStamp("other_dag", "test_run", "run", -1)
    theirs = platform.existing("theirs", other_run, WorkRequirementStatus.RUNNING)
    other = platform.existing("other", other_dag, WorkRequirementStatus.RUNNING)
    untagged = platform.existing("untagged", None, WorkRequirementStatus.RUNNING)
    named = platform.existing(
        STAMP.run_tag_prefix(), None, WorkRequirementStatus.RUNNING
    )
    mine_elsewhere = platform.existing("mine2", STAMP, WorkRequirementStatus.HELD)
    mine_elsewhere.namespace = "other-ns"
    theirs_elsewhere = platform.existing(
        "theirs2", other_run, WorkRequirementStatus.RUNNING
    )
    theirs_elsewhere.namespace = "other-ns"
    other_deployment = platform.existing(
        "other-deployment",
        RunStamp("test_dag", "test_run", "run", -1, deployment_id="prod"),
        WorkRequirementStatus.RUNNING,
    )
    _exec(_shutdown(cancel_work_requirements="run"))
    assert sorted(_cancelled(platform)) == sorted(
        [call(mine.id, True), call(mine_elsewhere.id, True)]
    )
    for wr in (theirs, other, untagged, named, theirs_elsewhere, other_deployment):
        assert wr.status == WorkRequirementStatus.RUNNING


def test_shutdown_cancel_none(platform: FakePlatform) -> None:
    platform.existing("a", STAMP, WorkRequirementStatus.RUNNING)
    _exec(_shutdown(cancel_work_requirements="none"))
    platform.work.get_work_requirements.assert_not_called()
    platform.work.cancel_work_requirement_by_id.assert_not_called()


def _scope_exec(op: ShutdownProvisionedWorkerPool) -> Any:
    """Run the teardown ``op`` (in a DAG) as its own task in this DAG run."""
    ctx = make_context(task_id=op.task_id)
    render(op, ctx)
    return op.execute(ctx)


def _two_scopes(
    **kw: Any,
) -> tuple[ShutdownProvisionedWorkerPool, ShutdownProvisionedWorkerPool]:
    """
    A DAG with two parallel setup/teardown scopes, s1 >> w1 >> t1 and
    s2 >> w2 >> t2, plus a task after t1; returns the teardowns (t1, t2).
    """
    with DAG("test_dag", schedule=None):
        s1, w1, s2, w2, after = (
            NoOp(task_id=i) for i in ("s1", "w1", "s2", "w2", "after")
        )
        t1 = _shutdown(task_id="t1", **kw)
        t2 = _shutdown(task_id="t2", **kw)
        _ = s1 >> w1 >> t1.as_teardown(setups=s1)
        _ = s2 >> w2 >> t2.as_teardown(setups=s2)
        _ = t1 >> after
    return t1, t2


def _task(task_id: str, map_index: int = -1) -> RunStamp:
    """The stamp of ``task_id`` in this DAG run."""
    return RunStamp(STAMP.dag_id, STAMP.run_id, task_id, map_index)


def test_teardown_scope_task_ids_is_own_branch() -> None:
    t1, t2 = _two_scopes()
    assert t1._teardown_scope_task_ids() == ["s1", "w1"]
    assert t2._teardown_scope_task_ids() == ["s2", "w2"]


def test_teardown_scope_cancels_only_own_scope(
    platform: FakePlatform, capsys: pytest.CaptureFixture[str]
) -> None:
    t1, _ = _two_scopes()
    setup = platform.existing("s1", _task("s1"), WorkRequirementStatus.RUNNING)
    work = platform.existing("w1", _task("w1"), WorkRequirementStatus.HELD, cycle=2)
    other = platform.existing("w2", _task("w2"), WorkRequirementStatus.RUNNING)
    after = platform.existing("after", _task("after"), WorkRequirementStatus.RUNNING)
    with patch.object(
        YellowDogHook,
        "find_unfinished_run_work_requirements",
        autospec=True,
        side_effect=YellowDogHook.find_unfinished_run_work_requirements,
    ) as find:
        assert _scope_exec(t1) == "wp-1"
    assert find.call_args.args[1:] == (
        None,
        STAMP.run_tag_prefix(),
        [_task("s1").task_tag_prefix(), _task("w1").task_tag_prefix()],
    )
    assert sorted(_cancelled(platform)) == sorted(
        [call(setup.id, True), call(work.id, True)]
    )
    for wr in (other, after):
        assert wr.status == WorkRequirementStatus.RUNNING
    out = capsys.readouterr().out  # Task logs go to stdout
    assert "Setup/teardown scope: 's1', 'w1'" in out
    assert "Cancelling 2 unfinished work requirement(s) of this teardown's scope" in out


def test_teardown_scope_in_task_group_uses_qualified_ids(
    platform: FakePlatform,
) -> None:
    with DAG("test_dag", schedule=None), TaskGroup("grp"):
        s = NoOp(task_id="s")
        w = NoOp(task_id="w")
        t = _shutdown(task_id="t")
        _ = s >> w >> t.as_teardown(setups=s)
    assert t._teardown_scope_task_ids() == ["grp.s", "grp.w"]
    mine = platform.existing("w", _task("grp.w"), WorkRequirementStatus.RUNNING)
    unqualified = platform.existing("u", _task("w"), WorkRequirementStatus.RUNNING)
    _scope_exec(t)
    assert _cancelled(platform) == [call(mine.id, True)]
    assert unqualified.status == WorkRequirementStatus.RUNNING


def test_teardown_scope_covers_every_map_index(platform: FakePlatform) -> None:
    with DAG("test_dag", schedule=None):
        s = NoOp(task_id="s")
        w = NoOp.partial(task_id="w").expand(doc=["a", "b", "c"])
        t = _shutdown(task_id="t")
        _ = s >> w >> t.as_teardown(setups=s)
    assert t._teardown_scope_task_ids() == ["s", "w"]
    mapped = [
        platform.existing(f"w{i}", _task("w", i), WorkRequirementStatus.RUNNING)
        for i in (0, 1, 12)
    ]
    _scope_exec(t)
    assert sorted(_cancelled(platform)) == sorted(call(m.id, True) for m in mapped)


def _mapped_group_teardown() -> ShutdownProvisionedWorkerPool:
    """
    A DAG with a setup ``outer`` and a mapped task group ``grp`` (expanded
    over 2) of s >> w >> t, t a teardown of s and outer; returns ``grp.t``.
    """
    with DAG("test_dag", schedule=None) as dag:
        outer = NoOp(task_id="outer")

        @task_group
        def grp(x: int) -> None:
            s = NoOp(task_id="s", doc=str(x))
            w = NoOp(task_id="w")
            t = _shutdown(task_id="t")
            _ = outer >> s >> w >> t.as_teardown(setups=[outer, s])

        _ = grp.expand(x=[1, 2])
    return cast(ShutdownProvisionedWorkerPool, dag.get_task("grp.t"))


def test_teardown_in_mapped_task_group_scopes_to_own_map_index(
    platform: FakePlatform,
) -> None:
    t = _mapped_group_teardown()
    assert t._teardown_scope_task_ids() == ["grp.s", "grp.w", "outer"]
    outer = platform.existing("o", _task("outer"), WorkRequirementStatus.RUNNING)
    work = {
        i: platform.existing(f"w{i}", _task("grp.w", i), WorkRequirementStatus.RUNNING)
        for i in (0, 1, 12)
    }
    with patch.object(
        YellowDogHook,
        "find_unfinished_run_work_requirements",
        autospec=True,
        side_effect=YellowDogHook.find_unfinished_run_work_requirements,
    ) as find:
        ctx = make_context(task_id=t.task_id, map_index=1)
        render(t, ctx)
        assert t.execute(ctx) == "wp-1"
    assert find.call_args.args[1:] == (
        None,
        STAMP.run_tag_prefix(),
        [
            _task("grp.s", 1).tag(),
            _task("grp.w", 1).tag(),
            _task("outer").task_tag_prefix(),  # Outside the group: every index
        ],
    )
    # m1 does not match m12
    assert sorted(_cancelled(platform)) == sorted(
        [call(outer.id, True), call(work[1].id, True)]
    )
    for i in (0, 12):
        assert work[i].status == WorkRequirementStatus.RUNNING


def test_teardown_run_cancels_whole_run(platform: FakePlatform) -> None:
    t1, _ = _two_scopes(cancel_work_requirements="run")
    wrs = [
        platform.existing(i, _task(i), WorkRequirementStatus.RUNNING)
        for i in ("w1", "w2", "after")
    ]
    with patch.object(
        YellowDogHook,
        "find_unfinished_run_work_requirements",
        autospec=True,
        side_effect=YellowDogHook.find_unfinished_run_work_requirements,
    ) as find:
        _scope_exec(t1)
    assert find.call_args.args[1:] == (None, STAMP.run_tag_prefix(), None)
    assert sorted(_cancelled(platform)) == sorted(call(w.id, True) for w in wrs)


def test_teardown_none_looks_nothing_up(platform: FakePlatform) -> None:
    t1, _ = _two_scopes(cancel_work_requirements="none")
    platform.existing("w1", _task("w1"), WorkRequirementStatus.RUNNING)
    assert _scope_exec(t1) == "wp-1"
    platform.work.get_work_requirements.assert_not_called()
    platform.work.cancel_work_requirement_by_id.assert_not_called()


def _not_a_teardown() -> ShutdownProvisionedWorkerPool:
    with DAG("test_dag", schedule=None):
        s = NoOp(task_id="s")
        t = _shutdown(task_id="t")
        _ = s >> t
    return t


def _teardown_without_setups() -> ShutdownProvisionedWorkerPool:
    with DAG("test_dag", schedule=None):
        w = NoOp(task_id="w")
        t = _shutdown(task_id="t")
        _ = w >> t.as_teardown()
    return t


@pytest.mark.parametrize(
    "build",
    [
        _not_a_teardown,
        _teardown_without_setups,
        lambda: _shutdown(task_id="t"),  # Not in a DAG
    ],
)
def test_scope_without_setups_warns_and_cancels_nothing(
    platform: FakePlatform,
    mock_client: MagicMock,
    capsys: pytest.CaptureFixture[str],
    build: Any,
) -> None:
    op = build()
    assert op._teardown_scope_task_ids() is None
    platform.existing("w", _task("w"), WorkRequirementStatus.RUNNING)
    assert _scope_exec(op) == "wp-1"
    platform.work.get_work_requirements.assert_not_called()
    platform.work.cancel_work_requirement_by_id.assert_not_called()
    mock_client.worker_pool_client.shutdown_worker_pool_by_id.assert_called_once_with(
        "wp-1"
    )
    assert "cancel_work_requirements='run'" in capsys.readouterr().out


@pytest.mark.parametrize("value", ["all", "", None, True, "Scope"])
def test_shutdown_rejects_bad_cancel_work_requirements(value: Any) -> None:
    with pytest.raises(YellowDogArgumentError, match="cancel_work_requirements"):
        _shutdown(cancel_work_requirements=value)


def test_shutdown_cancel_failure_shuts_down_then_fails(
    platform: FakePlatform, pool_fake: PoolFake
) -> None:
    a = platform.existing("a", STAMP, WorkRequirementStatus.RUNNING)
    b = platform.existing("b", STAMP, WorkRequirementStatus.RUNNING)
    platform.work.cancel_work_requirement_by_id.side_effect = RuntimeError("boom")
    with pytest.raises(YellowDogException, match="Failed to cancel 2 of 2") as info:
        _exec(
            _shutdown(
                cancel_work_requirements="run", terminate_immediately=True, wait=True
            )
        )
    assert isinstance(info.value.__cause__, RuntimeError)
    assert sorted(_cancelled(platform)) == sorted([call(a.id, True), call(b.id, True)])
    pool_fake.shutdown.assert_called_once_with("wp-1", terminate_immediately=True)
    pool_fake.pool_state.assert_not_called()  # Fails instead of waiting


def test_shutdown_partial_cancel_failure_names_the_failed(
    platform: FakePlatform, mock_client: MagicMock
) -> None:
    a = platform.existing("a", STAMP, WorkRequirementStatus.RUNNING)
    b = platform.existing("b", STAMP, WorkRequirementStatus.RUNNING)
    platform.work.cancel_work_requirement_by_id.side_effect = lambda wr_id, abort: (
        _raise(RuntimeError("boom")) if wr_id == b.id else None
    )
    with pytest.raises(YellowDogException, match="Failed to cancel 1 of 2") as info:
        _exec(_shutdown(cancel_work_requirements="run"))
    assert f"IDs: {b.id})" in str(info.value)
    assert str(a.id) not in str(info.value)
    mock_client.worker_pool_client.shutdown_worker_pool_by_id.assert_called_once()


def test_shutdown_search_failure_shuts_down_then_fails(
    platform: FakePlatform, mock_client: MagicMock
) -> None:
    platform.work.get_work_requirements.side_effect = RuntimeError("boom")
    with pytest.raises(YellowDogException, match="Could not look up") as info:
        _exec(_shutdown(cancel_work_requirements="run"))
    assert isinstance(info.value.__cause__, RuntimeError)
    mock_client.worker_pool_client.shutdown_worker_pool_by_id.assert_called_once()


def _raise(exc: Exception) -> None:
    raise exc


def test_shutdown_configured_pool_cancels_nothing(
    platform: FakePlatform, mock_client: MagicMock
) -> None:
    platform.existing("a", STAMP, WorkRequirementStatus.RUNNING)
    mock_client.worker_pool_client.get_worker_pool_by_id.return_value = _pool(
        ConfiguredWorkerPool
    )
    with pytest.raises(YellowDogException, match="configured worker pool"):
        _exec(_shutdown(cancel_work_requirements="run"))
    platform.work.cancel_work_requirement_by_id.assert_not_called()


def test_shutdown_without_wait_checks_no_state(
    platform: FakePlatform, pool_fake: PoolFake
) -> None:
    _exec(_shutdown(terminate_immediately=True))
    pool_fake.pool_state.assert_not_called()
    pool_fake.cr_state.assert_not_called()


def test_shutdown_wait_waits_for_pool(
    platform: FakePlatform, pool_fake: PoolFake
) -> None:
    pool_fake.pool_state.side_effect = [WP.RUNNING, WP.SHUTDOWN]
    pushed: dict[str, Any] = {}
    assert _exec(_shutdown(wait=True), pushed) == "wp-1"
    assert pushed["return_value"] == "wp-1"
    assert pool_fake.pool_state.call_count == 2
    pool_fake.cr_state.assert_not_called()


def test_shutdown_wait_terminate_waits_for_compute_requirement(
    platform: FakePlatform, pool_fake: PoolFake, mock_client: MagicMock
) -> None:
    _use_real_shutdown(pool_fake)
    pool_fake.pool_state.side_effect = [WP.RUNNING, WP.TERMINATED]
    pool_fake.cr_state.side_effect = [CR.TERMINATING, CR.TERMINATED]
    assert _exec(_shutdown(wait=True, terminate_immediately=True)) == "wp-1"
    mock_client.compute_client.terminate_compute_requirement_by_id.assert_called_once_with(
        "cr-1"
    )
    assert pool_fake.pool_state.call_count == 2
    assert pool_fake.cr_state.call_count == 2
    pool_fake.cr_state.assert_called_with("cr-1")


def test_shutdown_wait_timeout_raises(
    platform: FakePlatform, pool_fake: PoolFake
) -> None:
    pool_fake.pool_state.return_value = WP.RUNNING
    with pytest.raises(WaitTimeoutError):
        _exec(_shutdown(wait=True, timeout=0))


def test_shutdown_wait_timeout_is_shared_by_both_waits(
    platform: FakePlatform, pool_fake: PoolFake
) -> None:
    pool_fake.pool_state.return_value = WP.SHUTDOWN
    pool_fake.cr_state.return_value = CR.TERMINATING
    clock = chain([0.0], repeat(5.0))
    with (
        patch.object(ops.time, "monotonic", side_effect=clock),
        pytest.raises(WaitTimeoutError) as info,
    ):
        _exec(_shutdown(wait=True, terminate_immediately=True, timeout=3))
    assert info.value.last_state == CR.TERMINATING
    assert (info.value.resource_id, info.value.timeout) == ("cr-1", 3)


def _deferred(op: ShutdownProvisionedWorkerPool) -> Any:
    with patch(f"{ops.__name__}.time") as clock:
        clock.time.return_value = 1000.0
        with pytest.raises(TaskDeferred) as info:
            _exec(op)
    return info.value


def test_shutdown_defers_when_deferrable(
    platform: FakePlatform, pool_fake: PoolFake
) -> None:
    pool_fake.pool_state.return_value = WP.RUNNING
    op = _shutdown(
        wait=True,
        deferrable=True,
        terminate_immediately=True,
        poke_interval=15,
        timeout=900,
        connection_id="my_conn",
    )
    deferred = _deferred(op)
    assert deferred.trigger.serialize()[1] == {
        "connection_id": "my_conn",
        "kind": "worker_pool",
        "resource_id": "wp-1",
        "target_states": ["SHUTDOWN", "TERMINATED"],
        "failure_states": [],
        "poke_interval": 15.0,
        "end_time": 1900.0,
    }
    assert deferred.method_name == "execute_complete"
    assert deferred.kwargs == {
        "worker_pool_id": "wp-1",
        "compute_requirement_id": "cr-1",
        "end_time": 1900.0,
    }
    assert deferred.timeout == timedelta(seconds=900 + ops.DEFER_TIMEOUT_GRACE)

    # The pool event chains a second deferral, for the compute requirement
    pool_fake.cr_state.return_value = CR.TERMINATING
    with patch(f"{ops.__name__}.time") as clock:
        clock.time.return_value = 1100.0
        with pytest.raises(TaskDeferred) as info:
            op.execute_complete(
                make_context(), _pool_event("target", "SHUTDOWN"), **deferred.kwargs
            )
    second: Any = info.value
    assert second.trigger.serialize()[1] == {
        "connection_id": "my_conn",
        "kind": "compute_requirement",
        "resource_id": "cr-1",
        "target_states": ["TERMINATED"],
        "failure_states": [],
        "poke_interval": 15.0,
        "end_time": 1900.0,
    }
    assert second.kwargs == {
        "worker_pool_id": "wp-1",
        "compute_requirement_id": None,
        "end_time": 1900.0,
    }
    assert second.timeout == timedelta(seconds=800 + ops.DEFER_TIMEOUT_GRACE)

    cr_event = {**_pool_event("target", "TERMINATED"), "kind": "compute_requirement"}
    assert op.execute_complete(make_context(), cr_event, **second.kwargs) == "wp-1"


def test_shutdown_deferrable_skips_finished_stages(
    platform: FakePlatform, pool_fake: PoolFake
) -> None:
    pool_fake.pool_state.return_value = WP.TERMINATED
    op = _shutdown(wait=True, deferrable=True, terminate_immediately=True)
    with patch.object(ShutdownProvisionedWorkerPool, "defer") as defer:
        assert _exec(op) == "wp-1"
    defer.assert_not_called()


def test_shutdown_deferrable_pool_only(
    platform: FakePlatform, pool_fake: PoolFake
) -> None:
    pool_fake.pool_state.return_value = WP.RUNNING
    deferred = _deferred(_shutdown(wait=True, deferrable=True))
    assert deferred.kwargs["compute_requirement_id"] is None
    op = _shutdown(wait=True, deferrable=True)
    assert (
        op.execute_complete(
            make_context(), _pool_event("target", "SHUTDOWN"), **deferred.kwargs
        )
        == "wp-1"
    )
    pool_fake.cr_state.assert_not_called()


def test_shutdown_without_wait_does_not_defer(
    platform: FakePlatform, pool_fake: PoolFake
) -> None:
    with patch.object(ShutdownProvisionedWorkerPool, "defer") as defer:
        assert _exec(_shutdown(deferrable=True)) == "wp-1"
    defer.assert_not_called()


@pytest.mark.parametrize(
    "event, error",
    [
        (_pool_event("timeout", "RUNNING"), WaitTimeoutError),
        (_pool_event("error", None, error_type="HTTPError"), YellowDogException),
        (_pool_event("weird", None), YellowDogException),
    ],
)
def test_shutdown_execute_complete_failures(
    pool_fake: PoolFake, event: dict[str, Any], error: type[Exception]
) -> None:
    op = _shutdown(wait=True, timeout=900)
    with pytest.raises(error):
        op.execute_complete(
            make_context(),
            event,
            worker_pool_id="wp-1",
            compute_requirement_id="cr-1",
            end_time=1900.0,
        )
    pool_fake.cr_state.assert_not_called()


def test_shutdown_failure_still_terminates_compute_requirement(
    platform: FakePlatform, mock_client: MagicMock
) -> None:
    pools = mock_client.worker_pool_client
    pools.shutdown_worker_pool_by_id.side_effect = RuntimeError("platform down")
    with pytest.raises(RuntimeError, match="platform down"):
        _exec(_shutdown(terminate_immediately=True))
    mock_client.compute_client.terminate_compute_requirement_by_id.assert_called_once_with(
        "cr-1"
    )


def test_shutdown_failure_without_terminate_raises(
    platform: FakePlatform, mock_client: MagicMock
) -> None:
    pools = mock_client.worker_pool_client
    pools.shutdown_worker_pool_by_id.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="boom"):
        _exec(_shutdown())
    mock_client.compute_client.terminate_compute_requirement_by_id.assert_not_called()


def test_provision_cleanup_terminates_compute_requirement_if_shutdown_rejected(
    mock_client: MagicMock,
) -> None:
    """Uses the real hook: shutdown and terminate are independent."""
    pools = mock_client.worker_pool_client
    pools.provision_worker_pool.return_value = _pool()
    pools.get_worker_pool_by_id.return_value = _pool(
        status=WP.SHUTDOWN, computeRequirementId="cr-1"
    )
    pools.shutdown_worker_pool_by_id.side_effect = InvalidRequestException(
        "Invalid transition"
    )
    with pytest.raises(YellowDogStateError):
        _exec(_provision(wait_until_ready=True))
    mock_client.compute_client.terminate_compute_requirement_by_id.assert_called_once_with(
        "cr-1"
    )


@pytest.mark.parametrize(
    "error",
    [
        InvalidRequestException("Invalid transition: pool is SHUTDOWN"),
        HTTPError("400 Client Error: INVALID TRANSITION"),
    ],
)
def test_teardown_of_already_shut_down_pool_succeeds(
    platform: FakePlatform,
    pool_fake: PoolFake,
    mock_client: MagicMock,
    error: Exception,
) -> None:
    _use_real_shutdown(pool_fake)
    mock_client.worker_pool_client.shutdown_worker_pool_by_id.side_effect = error
    pool_fake.pool_state.return_value = WP.SHUTDOWN
    pool_fake.cr_state.side_effect = [CR.TERMINATING, CR.TERMINATED]
    op = _shutdown(wait=True, terminate_immediately=True)
    assert _exec(op) == "wp-1"
    mock_client.compute_client.terminate_compute_requirement_by_id.assert_called_once_with(
        "cr-1"
    )
    assert pool_fake.cr_state.call_count == 2  # Still waited


def test_teardown_of_already_shut_down_pool_without_terminate(
    platform: FakePlatform, mock_client: MagicMock
) -> None:
    mock_client.worker_pool_client.shutdown_worker_pool_by_id.side_effect = (
        InvalidRequestException("invalid transition")
    )
    assert _exec(_shutdown()) == "wp-1"
    mock_client.compute_client.terminate_compute_requirement_by_id.assert_not_called()


def test_shutdown_other_platform_errors_still_raise(
    platform: FakePlatform, mock_client: MagicMock
) -> None:
    mock_client.worker_pool_client.shutdown_worker_pool_by_id.side_effect = (
        InvalidRequestException("Pool is locked")
    )
    with pytest.raises(InvalidRequestException):
        _exec(_shutdown())


def test_teardown_retry_after_compute_requirement_terminated_succeeds(
    platform: FakePlatform, pool_fake: PoolFake, mock_client: MagicMock
) -> None:
    _use_real_shutdown(pool_fake)
    mock_client.worker_pool_client.shutdown_worker_pool_by_id.side_effect = (
        InvalidRequestException("invalid transition")
    )
    terminate = mock_client.compute_client.terminate_compute_requirement_by_id
    terminate.side_effect = InvalidRequestException("Invalid transition: TERMINATED")
    pool_fake.pool_state.return_value = WP.TERMINATED
    pool_fake.cr_state.return_value = CR.TERMINATED
    assert _exec(_shutdown(terminate_immediately=True, wait=True)) == "wp-1"
    terminate.assert_called_once_with("cr-1")


def test_teardown_other_terminate_errors_raise(
    platform: FakePlatform, mock_client: MagicMock
) -> None:
    terminate = mock_client.compute_client.terminate_compute_requirement_by_id
    terminate.side_effect = InvalidRequestException("Quota exceeded")
    with pytest.raises(InvalidRequestException, match="Quota"):
        _exec(_shutdown(terminate_immediately=True))


def test_teardown_in_mapped_group_gives_nested_mapped_group_plain_prefix() -> None:
    """
    Tasks in a mapped group nested in the teardown's mapped group have their
    own (flat) map indices, so they keep the plain task prefix.
    """
    with DAG("test_dag", schedule=None) as dag:

        @task_group
        def outer(x: int) -> None:
            so = NoOp(task_id="so", doc=str(x))
            t = _shutdown(task_id="t")

            @task_group
            def inner(y: int) -> None:
                NoOp(task_id="w", doc=str(y))

            _ = so >> inner.expand(y=[1, 2]) >> t.as_teardown(setups=so)

        _ = outer.expand(x=[1, 2])
    t = cast(ShutdownProvisionedWorkerPool, dag.get_task("outer.t"))
    assert t._teardown_scope_task_ids() == ["outer.inner.w", "outer.so"]
    stamp = _task("outer.t", 1)
    assert t._scope_tag_prefixes(stamp, ["outer.inner.w", "outer.so"]) == [
        _task("outer.inner.w").task_tag_prefix(),
        _task("outer.so", 1).tag(),
    ]
