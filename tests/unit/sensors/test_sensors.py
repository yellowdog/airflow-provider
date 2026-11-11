import ast
import importlib
import inspect
import pickle
import warnings
from typing import Any, cast
from unittest.mock import MagicMock, patch

import airflow.sdk.bases.sensor as base_sensor
import airflow.sdk.execution_time.task_runner as task_runner
import pytest
from airflow.sdk import Context
from yellowdog_client.model import (
    ComputeRequirement,
    ComputeRequirementStatus,
    ProvisionedWorkerPool,
    WorkerPoolStatus,
    WorkRequirement,
    WorkRequirementStatus,
)

from tests.support import make_context, render
from yellowdog_provider.exceptions.yellowdog_exceptions import (
    YellowDogArgumentError,
    YellowDogException,
    YellowDogStateError,
)
from yellowdog_provider.hooks.yellowdog_hooks import YellowDogHook
from yellowdog_provider.sensors.yellowdog_sensors import (
    ComputeRequirementStateSensor,
    WorkerPoolStateSensor,
    WorkRequirementStateSensor,
)


class Case:
    def __init__(
        self,
        sensor: type,
        prefix: str,
        status: Any,
        reached: Any,
        other: Any,
        model: type,
        client_attr: str,
        getter: str,
        failed: Any,
        default_failures: list[Any],
    ) -> None:
        self.sensor = sensor
        self.prefix = prefix
        self.status = status
        self.reached = reached
        self.other = other
        self.model = model
        self.client_attr = client_attr
        self.getter = getter
        self.failed = failed  # A final state that is not the target
        self.default_failures = default_failures  # Derived for [reached]

    def make(self, **kw: Any) -> Any:
        kw.setdefault("target_states", [self.reached])
        return self.sensor(task_id="s", **kw)

    def obj(self, status: Any) -> Any:
        o = object.__new__(self.model)
        o.id, o.namespace, o.name, o.status = "obj-1", "ns", "nm", status
        return o

    def setup(self, client: MagicMock, status: Any) -> MagicMock:
        api = getattr(client, self.client_attr)
        getattr(api, f"get_{self.getter}_by_id").return_value = self.obj(status)
        getattr(api, f"get_{self.getter}_by_name").return_value = self.obj(status)
        return api


CASES = [
    Case(
        WorkRequirementStateSensor,
        "work_requirement",
        WorkRequirementStatus,
        WorkRequirementStatus.COMPLETED,
        WorkRequirementStatus.RUNNING,
        WorkRequirement,
        "work_client",
        "work_requirement",
        WorkRequirementStatus.FAILED,
        [WorkRequirementStatus.FAILED, WorkRequirementStatus.CANCELLED],
    ),
    Case(
        WorkerPoolStateSensor,
        "worker_pool",
        WorkerPoolStatus,
        WorkerPoolStatus.RUNNING,
        WorkerPoolStatus.PENDING,
        ProvisionedWorkerPool,
        "worker_pool_client",
        "worker_pool",
        WorkerPoolStatus.SHUTDOWN,
        [WorkerPoolStatus.SHUTDOWN, WorkerPoolStatus.TERMINATED],
    ),
    Case(
        ComputeRequirementStateSensor,
        "compute_requirement",
        ComputeRequirementStatus,
        ComputeRequirementStatus.RUNNING,
        ComputeRequirementStatus.NEW,
        ComputeRequirement,
        "compute_client",
        "compute_requirement",
        ComputeRequirementStatus.TERMINATED,
        [ComputeRequirementStatus.TERMINATED],
    ),
]
cases = pytest.mark.parametrize("case", CASES, ids=lambda c: c.prefix)


def _poke(op: Any, ctx: Any = None) -> bool:
    ctx = ctx or _ctx()
    render(op, ctx)
    return op.poke(ctx)


def _ctx() -> Context:
    ctx = make_context()
    ctx["task_instance"] = MagicMock()
    return ctx


@cases
def test_poke_true_when_target_reached(case: Case, mock_client: MagicMock) -> None:
    case.setup(mock_client, case.reached)
    assert _poke(case.make(**{f"{case.prefix}_id": "obj-1"})) is True


@cases
def test_poke_false_otherwise(case: Case, mock_client: MagicMock) -> None:
    case.setup(mock_client, case.other)
    assert _poke(case.make(**{f"{case.prefix}_id": "obj-1"})) is False


@cases
def test_pushes_id_to_xcom_once(case: Case, mock_client: MagicMock) -> None:
    case.setup(mock_client, case.other)
    op = case.make(**{f"{case.prefix}_id": "obj-1"})
    ctx = _ctx()
    _poke(op, ctx)
    _poke(op, ctx)
    ti = cast("MagicMock", ctx.get("task_instance"))
    ti.xcom_push.assert_called_once_with(key="return_value", value="obj-1")


@cases
def test_lookup_by_name(case: Case, mock_client: MagicMock) -> None:
    api = case.setup(mock_client, case.reached)
    op = case.make(namespace="ns", **{f"{case.prefix}_name": "nm"})
    assert _poke(op) is True
    getattr(api, f"get_{case.getter}_by_name").assert_called_once_with("ns", "nm")


@cases
def test_requires_id_or_name_at_init(case: Case) -> None:
    with pytest.raises(YellowDogArgumentError):
        case.make()


@cases
def test_empty_target_states_raises(case: Case) -> None:
    with pytest.raises(YellowDogArgumentError):
        case.make(target_states=[], **{f"{case.prefix}_id": "obj-1"})


@cases
def test_default_poke_interval_60(case: Case) -> None:
    assert case.make(**{f"{case.prefix}_id": "x"}).poke_interval == 60


@cases
def test_default_connection_id(case: Case) -> None:
    op = case.make(**{f"{case.prefix}_id": "x"})
    assert op.connection_id == "yellowdog_default"


@cases
def test_positional_args_rejected(case: Case) -> None:
    with pytest.raises(TypeError):
        case.sensor("s")


@cases
def test_hook_closed_after_poke(case: Case, mock_client: MagicMock) -> None:
    case.setup(mock_client, case.reached)
    with patch.object(YellowDogHook, "close") as close:
        _poke(case.make(**{f"{case.prefix}_id": "obj-1"}))
    close.assert_called_once()


@cases
def test_id_rendering_to_none_raises(case: Case, mock_client: MagicMock) -> None:
    op = case.make(**{f"{case.prefix}_id": "{{ params.missing }}"})
    ctx = _ctx()
    ctx["params"] = {"missing": None}
    render(op, ctx)
    with pytest.raises(YellowDogArgumentError):
        op.poke(ctx)


# Failure states


@cases
def test_default_failure_states_derived(case: Case) -> None:
    op = case.make(**{f"{case.prefix}_id": "x"})
    assert list(op.failure_states) == case.default_failures


@pytest.mark.parametrize(
    ("sensor", "prefix", "targets", "expected"),
    [
        (
            WorkRequirementStateSensor,
            "work_requirement",
            [WorkRequirementStatus.COMPLETED, WorkRequirementStatus.FAILED],
            [WorkRequirementStatus.CANCELLED],
        ),
        (
            WorkerPoolStateSensor,
            "worker_pool",
            [WorkerPoolStatus.SHUTDOWN, WorkerPoolStatus.TERMINATED],
            [],
        ),
        (
            ComputeRequirementStateSensor,
            "compute_requirement",
            [ComputeRequirementStatus.TERMINATED],
            [],
        ),
        (
            ComputeRequirementStateSensor,
            "compute_requirement",
            [ComputeRequirementStatus.STOPPED],
            [ComputeRequirementStatus.TERMINATED],
        ),
    ],
)
def test_default_failure_states_exclude_targets(
    sensor: type, prefix: str, targets: list[Any], expected: list[Any]
) -> None:
    op = sensor(task_id="s", target_states=targets, **{f"{prefix}_id": "x"})
    assert list(op.failure_states) == expected


@cases
def test_failure_state_raises_after_xcom_push(
    case: Case, mock_client: MagicMock
) -> None:
    case.setup(mock_client, case.failed)
    op = case.make(**{f"{case.prefix}_id": "obj-1"})
    ctx = _ctx()
    with pytest.raises(YellowDogStateError) as exc_info:
        _poke(op, ctx)
    ti = cast("MagicMock", ctx.get("task_instance"))
    ti.xcom_push.assert_called_once_with(key="return_value", value="obj-1")
    err = exc_info.value
    assert isinstance(err, YellowDogException)
    message = str(err)
    assert "obj-1" in message
    assert "ns/nm" in message
    assert f"'{case.failed.value}'" in message
    assert f"'{case.reached.value}'" in message
    assert case.prefix.replace("_", " ") in message.lower()
    assert err.resource_id == "obj-1"
    assert err.current_state == case.failed
    assert list(err.target_states) == [case.reached]


@cases
def test_explicit_failure_states_override(case: Case, mock_client: MagicMock) -> None:
    case.setup(mock_client, case.other)
    op = case.make(failure_states=[case.other], **{f"{case.prefix}_id": "obj-1"})
    assert list(op.failure_states) == [case.other]
    with pytest.raises(YellowDogStateError):
        _poke(op)


@cases
def test_explicit_failure_states_replace_default(
    case: Case, mock_client: MagicMock
) -> None:
    case.setup(mock_client, case.failed)
    op = case.make(failure_states=[case.other], **{f"{case.prefix}_id": "obj-1"})
    assert _poke(op) is False


@cases
def test_empty_failure_states_disables(case: Case, mock_client: MagicMock) -> None:
    case.setup(mock_client, case.failed)
    op = case.make(failure_states=[], **{f"{case.prefix}_id": "obj-1"})
    assert list(op.failure_states) == []
    assert _poke(op) is False


@cases
def test_target_still_succeeds_with_failure_states(
    case: Case, mock_client: MagicMock
) -> None:
    case.setup(mock_client, case.reached)
    op = case.make(failure_states=[case.failed], **{f"{case.prefix}_id": "obj-1"})
    assert _poke(op) is True


@cases
def test_overlapping_failure_states_rejected(case: Case) -> None:
    with pytest.raises(YellowDogArgumentError, match=case.reached.value):
        case.make(
            failure_states=[case.failed, case.reached],
            **{f"{case.prefix}_id": "x"},
        )


@cases
def test_failure_states_wrong_type_rejected(case: Case) -> None:
    with pytest.raises(YellowDogArgumentError, match="failure_states"):
        case.make(failure_states=[case.failed.value], **{f"{case.prefix}_id": "x"})


@cases
def test_target_states_wrong_type_rejected(case: Case) -> None:
    with pytest.raises(YellowDogArgumentError, match="target_states"):
        case.make(target_states=[case.reached.value], **{f"{case.prefix}_id": "x"})


def test_state_error_without_name() -> None:
    err = YellowDogStateError(
        kind="work requirement",
        resource_id="wr-1",
        current_state=WorkRequirementStatus.FAILED,
        target_states=[WorkRequirementStatus.COMPLETED],
    )
    assert str(err) == (
        "Work requirement ID 'wr-1' reached status 'FAILED', which is a failure "
        "state (target states: 'COMPLETED')"
    )
    assert err.namespace is None and err.name is None


def test_state_error_pickles() -> None:
    err = YellowDogStateError(
        kind="worker pool",
        resource_id="wp-1",
        current_state=WorkerPoolStatus.SHUTDOWN,
        target_states=[WorkerPoolStatus.IDLE, WorkerPoolStatus.RUNNING],
        namespace="ns",
        name="nm",
    )
    copy = pickle.loads(pickle.dumps(err))
    assert isinstance(copy, YellowDogStateError)
    assert str(copy) == str(err)
    assert copy.namespace == "ns" and copy.name == "nm"


_FAIL = "AirflowFailException"


def test_state_error_is_airflow_fail_exception() -> None:
    """Airflow fails the task without retrying, and silent_fail does not hide it."""
    err = YellowDogStateError(
        kind="work requirement",
        resource_id="wr-1",
        current_state=WorkRequirementStatus.CANCELLED,
        target_states=[WorkRequirementStatus.COMPLETED],
    )
    assert isinstance(err, YellowDogException)
    # BaseSensorOperator (not hidden by silent_fail) imports the name at module
    # level on every supported Airflow (from a different module on each), so
    # look it up by name
    sensor_caught: type[BaseException] = getattr(base_sensor, _FAIL)
    assert isinstance(err, sensor_caught)
    # The task runner (no retry) imports it at module level (Airflow 3.4) or
    # inside run() (Airflow 3.1), so find where its source imports it from
    runner_caught = _runner_fail_exceptions()
    assert runner_caught
    for caught in runner_caught:
        assert isinstance(err, caught), caught


def _runner_fail_exceptions() -> list[type[BaseException]]:
    """The AirflowFailException classes the installed task runner imports."""
    tree = ast.parse(inspect.getsource(task_runner))
    modules = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and node.module is not None
        and any(alias.name == _FAIL for alias in node.names)
    }
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        return [
            getattr(importlib.import_module(module), _FAIL)
            for module in sorted(modules)
        ]


@cases
def test_failure_state_not_swallowed_by_silent_fail(
    case: Case, mock_client: MagicMock
) -> None:
    case.setup(mock_client, case.failed)
    op = case.make(
        silent_fail=True,
        poke_interval=0.01,
        timeout=0.05,
        **{f"{case.prefix}_id": "obj-1"},
    )
    ctx = _ctx()
    render(op, ctx)
    with pytest.raises(YellowDogStateError):
        op.execute(ctx)


@cases
def test_poke_log_lists_failure_states(case: Case, mock_client: MagicMock) -> None:
    case.setup(mock_client, case.other)
    op = case.make(**{f"{case.prefix}_id": "obj-1"})
    with patch.object(type(op), "log", new_callable=MagicMock) as log:
        _poke(op)
    messages = " ".join(str(c.args[0]) for c in log.info.call_args_list)
    assert f"failure states {[s.value for s in case.default_failures]}" in messages
