from typing import Any, cast
from unittest.mock import MagicMock, patch

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
from yellowdog_provider.exceptions.yellowdog_exceptions import YellowDogArgumentError
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
    ) -> None:
        self.sensor = sensor
        self.prefix = prefix
        self.status = status
        self.reached = reached
        self.other = other
        self.model = model
        self.client_attr = client_attr
        self.getter = getter

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
