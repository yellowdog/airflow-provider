from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
from jinja2 import Environment
from yellowdog_client.model import (
    RunSpecification,
    Task,
    TaskGroup,
    WorkRequirement,
    WorkRequirementStatus,
)

from tests.support import make_context, render
from tests.unit.hooks.test_hook_operations import FakePlatform
from tests.unit.test_sdk_xcom import SDK_OBJECTS_VIA_XCOM
from yellowdog_provider.exceptions.yellowdog_exceptions import (
    WaitTimeoutError,
    YellowDogArgumentError,
    YellowDogException,
    YellowDogStateError,
)
from yellowdog_provider.hooks.yellowdog_hooks import SubmitResult, YellowDogHook
from yellowdog_provider.operators import yellowdog_operators as ops
from yellowdog_provider.operators.yellowdog_operators import RunWorkRequirement
from yellowdog_provider.triggers.yellowdog_triggers import YellowDogStateTrigger
from yellowdog_provider.utils.airflow_compat import TaskDeferred
from yellowdog_provider.utils.run_stamp import RunStamp, try_name

S = WorkRequirementStatus
WR_ID = "ydid:workreq:1"


def _wr() -> WorkRequirement:
    return WorkRequirement(namespace="ns", name="wr")


def _groups() -> list[tuple[TaskGroup, list[Task]]]:
    group = TaskGroup(name="g", runSpecification=RunSpecification(taskTypes=["t"]))
    return [(group, [Task(), Task()])]


@dataclass
class Fake:
    """Stand-in hook methods, patched onto YellowDogHook."""

    submit: MagicMock
    cancel: MagicMock
    state: MagicMock
    pushed: dict[str, Any] = field(default_factory=dict[str, Any])

    def states(self, *states: Any) -> None:
        self.state.side_effect = list(states)


@pytest.fixture
def fake(mock_client: MagicMock) -> Iterator[Fake]:
    submit = MagicMock(return_value=SubmitResult(WR_ID, "wr", "submitted"))
    cancel = MagicMock(return_value=None)
    state = MagicMock(return_value=S.COMPLETED)
    with (
        patch.object(YellowDogHook, "submit_work_requirement", submit),
        patch.object(YellowDogHook, "cancel_work_requirement", cancel),
        patch.object(YellowDogHook, "get_work_requirement_state", state),
    ):
        yield Fake(submit, cancel, state)


def _op(**kw: Any) -> RunWorkRequirement:
    kw.setdefault("work_requirement", _wr())
    kw.setdefault("task_groups_and_tasks", _groups())
    kw.setdefault("poke_interval", 0)
    kw.setdefault("deferrable", False)
    return RunWorkRequirement(task_id="run", **kw)


def _execute(op: RunWorkRequirement, fake: Fake, **ctx_kw: Any) -> Any:
    ctx = make_context(pushed=fake.pushed, **ctx_kw)
    render(op, ctx)
    return op.execute(ctx)


# -- construction ------------------------------------------------------------


def test_run_template_fields() -> None:
    assert RunWorkRequirement.template_fields == (
        "connection_id",
        "work_requirement",
        "task_groups_and_tasks",
    )


def test_run_default_failure_states_are_other_final_states() -> None:
    op = _op()
    assert op.target_states == [S.COMPLETED]
    assert set(op.failure_states) == {S.FAILED, S.CANCELLED}


def test_run_rejects_overlapping_states() -> None:
    with pytest.raises(YellowDogArgumentError):
        _op(failure_states=[S.COMPLETED])


def test_run_rejects_empty_targets() -> None:
    with pytest.raises(YellowDogArgumentError):
        _op(target_states=[])


@pytest.mark.parametrize("configured", [True, False])
def test_run_deferrable_defaults_to_airflow_setting(configured: bool) -> None:
    with patch.object(ops.conf, "getboolean", return_value=configured) as get:
        op = RunWorkRequirement(
            task_id="run", work_requirement=_wr(), task_groups_and_tasks=_groups()
        )
    assert op.deferrable is configured
    get.assert_called_once_with("operators", "default_deferrable", fallback=False)


# -- synchronous path --------------------------------------------------------


def test_run_submits_and_waits_to_completed(fake: Fake) -> None:
    fake.states(S.RUNNING, S.COMPLETED)
    wr, groups = _wr(), _groups()
    op = _op(work_requirement=wr, task_groups_and_tasks=groups)
    assert _execute(op, fake) == WR_ID
    fake.submit.assert_called_once_with(
        wr,
        groups,
        stamp=RunStamp("test_dag", "test_run", "test_task", -1),
        try_number=1,
        cycle=0,
    )
    assert fake.state.call_count == 2
    fake.state.assert_called_with(WR_ID)
    fake.cancel.assert_not_called()
    assert fake.pushed["return_value"] == WR_ID


def test_run_resolves_callables(fake: Fake) -> None:
    wr, groups = _wr(), _groups()

    def make_wr(*, context: Any, jinja_env: Environment) -> WorkRequirement:
        return wr

    def make_groups(
        *, context: Any, jinja_env: Environment
    ) -> list[tuple[TaskGroup, list[Task]]]:
        return groups

    op = _op(work_requirement=make_wr, task_groups_and_tasks=make_groups)
    assert _execute(op, fake) == WR_ID
    assert fake.submit.call_args.args == (wr, groups)


def test_run_pushes_id_before_waiting(fake: Fake) -> None:
    seen: list[Any] = []

    def state(_: str) -> WorkRequirementStatus:
        seen.append(fake.pushed.get("return_value"))
        return S.COMPLETED

    fake.state.side_effect = state
    _execute(_op(), fake)
    assert seen == [WR_ID]


def test_run_raises_state_error_on_failed(fake: Fake) -> None:
    fake.states(S.RUNNING, S.FAILED)
    with pytest.raises(YellowDogStateError) as info:
        _execute(_op(), fake)
    assert info.value.current_state == S.FAILED
    assert info.value.resource_id == WR_ID
    fake.cancel.assert_not_called()


def test_run_cancels_on_unfinished_failure_state(fake: Fake) -> None:
    # A user-chosen failure state the work requirement does not stop in
    fake.states(S.HELD)
    with pytest.raises(YellowDogStateError):
        _execute(_op(failure_states=[S.HELD]), fake)
    fake.cancel.assert_called_once_with(WR_ID, abort=True)


def test_run_own_timeout_cancels_with_abort(fake: Fake) -> None:
    fake.state.return_value = S.RUNNING
    with pytest.raises(WaitTimeoutError) as info:
        _execute(_op(timeout=0), fake)
    assert info.value.resource_id == WR_ID
    fake.cancel.assert_called_once_with(WR_ID, abort=True)


def test_run_wait_error_cancels_and_reraises(fake: Fake) -> None:
    error = YellowDogException("boom")
    fake.state.side_effect = error
    with pytest.raises(YellowDogException) as info:
        _execute(_op(), fake)
    assert info.value is error
    fake.cancel.assert_called_once_with(WR_ID, abort=True)


def test_run_cancel_failure_does_not_mask_original_error(fake: Fake) -> None:
    fake.state.return_value = S.RUNNING
    fake.cancel.side_effect = RuntimeError("cancel failed")
    with pytest.raises(WaitTimeoutError):
        _execute(_op(timeout=0), fake)
    fake.cancel.assert_called_once()


def test_run_resumes_unfinished_attempt_without_resubmit(fake: Fake) -> None:
    fake.submit.return_value = SubmitResult(WR_ID, "wr", "resumed")
    fake.states(S.RUNNING, S.COMPLETED)
    assert _execute(_op(), fake, try_number=2, max_tries=3) == WR_ID
    fake.submit.assert_called_once()
    assert fake.submit.call_args.kwargs["cycle"] == 3
    assert fake.state.call_count == 2
    assert fake.pushed["return_value"] == WR_ID


def test_run_already_completed_returns_id(fake: Fake) -> None:
    fake.submit.return_value = SubmitResult(WR_ID, "wr", "already_completed")
    assert _execute(_op(), fake, try_number=2, max_tries=3) == WR_ID
    fake.state.assert_not_called()
    fake.cancel.assert_not_called()
    assert fake.pushed["return_value"] == WR_ID


def test_run_already_completed_checks_state_if_completed_not_a_target(
    fake: Fake,
) -> None:
    fake.submit.return_value = SubmitResult(WR_ID, "wr", "already_completed")
    fake.states(S.COMPLETED)
    with pytest.raises(YellowDogStateError):
        _execute(_op(target_states=[S.RUNNING]), fake, try_number=2, max_tries=2)
    fake.cancel.assert_not_called()


def test_run_passes_max_tries_as_cycle(fake: Fake) -> None:
    # retries=2; try 1 succeeded; the clear sets max_tries = 1 + 2
    _execute(_op(retries=2), fake, try_number=2, max_tries=3)
    assert fake.submit.call_args.kwargs == {
        "stamp": RunStamp("test_dag", "test_run", "test_task", -1),
        "try_number": 2,
        "cycle": 3,
    }


@pytest.mark.parametrize("max_tries", [None, -1, "3"])
def test_run_cycle_zero_if_max_tries_unavailable(fake: Fake, max_tries: Any) -> None:
    ctx = make_context(pushed=fake.pushed, try_number=2, max_tries=1)
    ti: Any = cast(Any, ctx)["ti"]
    if max_tries is None:
        del ti.max_tries
    else:
        ti.max_tries = max_tries
    op = _op()
    render(op, ctx)
    op.execute(ctx)
    assert fake.submit.call_args.kwargs["cycle"] == 0


def test_run_clear_then_failed_submit_then_retry_resubmits(
    mock_client: MagicMock,
) -> None:
    """
    Cleared after success: the first try after the clear fails at submit (a
    transient error); the retry must still redo the work, not return the
    attempt that completed before the clear.
    """
    platform = FakePlatform(mock_client)
    stamp = RunStamp("test_dag", "test_run", "test_task", -1)
    # retries=2: try 1 ran in cycle 2 (max_tries = retries) and completed
    before = platform.existing("wr", stamp, S.COMPLETED, cycle=2)
    # Cleared: max_tries = 1 + 2 = 3; try 2 fails at submit
    search = platform.work.get_work_requirements.side_effect
    platform.work.get_work_requirements.side_effect = [ConnectionError("down")]
    with pytest.raises(ConnectionError):
        _execute_on(_op(retries=2), try_number=2, max_tries=3)
    platform.work.add_work_requirement.assert_not_called()
    # Retry: try 3, max_tries still 3
    platform.work.get_work_requirements.side_effect = search
    with patch.object(
        YellowDogHook, "get_work_requirement_state", return_value=S.COMPLETED
    ):
        wr_id = _execute_on(_op(retries=2), try_number=3, max_tries=3)
    assert wr_id != before.id
    added = platform.work.add_work_requirement.call_args.args[0]
    assert added.name == try_name("wr", 3)
    assert added.tag == f"{stamp.tag()}:c3"
    # A further retry in the same cycle finds that one completed
    platform.by_id[wr_id].status = S.COMPLETED
    platform.work.add_work_requirement.reset_mock()
    assert _execute_on(_op(retries=2), try_number=4, max_tries=3) == wr_id
    platform.work.add_work_requirement.assert_not_called()


def _execute_on(op: RunWorkRequirement, **ctx_kw: Any) -> Any:
    ctx = make_context(**ctx_kw)
    render(op, ctx)
    return op.execute(ctx)


# -- on_kill -----------------------------------------------------------------


class _Killed(BaseException):
    """Stands in for Airflow interrupting a running task (not an Exception)."""


def test_run_killed_mid_wait_on_kill_cancels_with_abort(fake: Fake) -> None:
    fake.states(S.RUNNING, _Killed())
    op = _op()
    with pytest.raises(_Killed):
        _execute(op, fake)
    fake.cancel.assert_not_called()  # BaseException: no cleanup in execute
    op.on_kill()
    fake.cancel.assert_called_once_with(WR_ID, abort=True)


def test_run_on_kill_noop_before_submit(fake: Fake) -> None:
    with patch.object(RunWorkRequirement, "get_hook") as get_hook:
        _op().on_kill()
    get_hook.assert_not_called()
    fake.cancel.assert_not_called()


def test_run_on_kill_never_raises(fake: Fake) -> None:
    fake.states(S.RUNNING, _Killed())
    fake.cancel.side_effect = RuntimeError("platform down")
    op = _op()
    with pytest.raises(_Killed):
        _execute(op, fake)
    op.on_kill()  # Logged, not raised
    fake.cancel.assert_called_once_with(WR_ID, abort=True)


def test_run_deferral_trigger_cancels_on_kill(fake: Fake) -> None:
    fake.state.return_value = S.RUNNING
    with pytest.raises(TaskDeferred) as info:
        _execute(_op(deferrable=True), fake)
    trigger = info.value.trigger
    assert isinstance(trigger, YellowDogStateTrigger)
    assert trigger.kill_action == "cancel_work_requirement"
    assert trigger.serialize()[1]["kill_action"] == "cancel_work_requirement"


# -- deferral ----------------------------------------------------------------


def test_run_defers_with_trigger(fake: Fake) -> None:
    fake.state.return_value = S.RUNNING
    op = _op(deferrable=True, poke_interval=30, timeout=600, connection_id="my_conn")
    with patch(f"{ops.__name__}.time") as clock:
        clock.time.return_value = 1000.0
        with pytest.raises(TaskDeferred) as info:
            _execute(op, fake)
    trigger = info.value.trigger
    assert isinstance(trigger, YellowDogStateTrigger)
    assert trigger.serialize()[1] == {
        "connection_id": "my_conn",
        "kind": "work_requirement",
        "resource_id": WR_ID,
        "target_states": ["COMPLETED"],
        "failure_states": ["FAILED", "CANCELLED"],
        "poke_interval": 30.0,
        "end_time": 1600.0,
        "kill_action": "cancel_work_requirement",
    }
    assert trigger.kill_action == "cancel_work_requirement"
    assert info.value.method_name == "execute_complete"
    assert info.value.kwargs == {
        "work_requirement_id": WR_ID,
        "namespace": "ns",
        "name": "wr",
    }
    assert info.value.timeout == timedelta(seconds=600 + ops.DEFER_TIMEOUT_GRACE)
    assert fake.pushed["return_value"] == WR_ID
    fake.cancel.assert_not_called()


def test_run_already_completed_deferrable_does_not_defer(fake: Fake) -> None:
    fake.submit.return_value = SubmitResult(WR_ID, "wr", "already_completed")
    op = _op(deferrable=True)
    with patch.object(RunWorkRequirement, "defer") as defer:
        assert _execute(op, fake, try_number=2, max_tries=2) == WR_ID
    defer.assert_not_called()
    fake.state.assert_not_called()


def test_run_deferrable_returns_if_target_on_first_check(fake: Fake) -> None:
    fake.states(S.COMPLETED)
    assert _execute(_op(deferrable=True), fake) == WR_ID


def test_run_deferrable_raises_on_failure_state_first_check(fake: Fake) -> None:
    fake.states(S.FAILED)
    with pytest.raises(YellowDogStateError):
        _execute(_op(deferrable=True), fake)
    fake.cancel.assert_not_called()


def test_run_deferrable_first_check_error_cancels(fake: Fake) -> None:
    fake.state.side_effect = YellowDogException("boom")
    with pytest.raises(YellowDogException):
        _execute(_op(deferrable=True), fake)
    fake.cancel.assert_called_once_with(WR_ID, abort=True)


def _event(status: str, state: str | None, **extra: Any) -> dict[str, Any]:
    return {
        "status": status,
        "kind": "work_requirement",
        "resource_id": WR_ID,
        "state": state,
        "message": f"message for {status}",
        **extra,
    }


def _complete(op: RunWorkRequirement, event: dict[str, Any]) -> Any:
    return op.execute_complete(
        make_context(), event, work_requirement_id=WR_ID, namespace="ns", name="wr"
    )


def test_run_execute_complete_target_returns_id(fake: Fake) -> None:
    assert _complete(_op(), _event("target", "COMPLETED")) == WR_ID
    assert _complete(_op(), _event("target", "NEW_STATE")) == WR_ID
    fake.cancel.assert_not_called()


def test_run_execute_complete_failure_raises_state_error(fake: Fake) -> None:
    with pytest.raises(YellowDogStateError) as info:
        _complete(_op(), _event("failure", "CANCELLED"))
    assert info.value.current_state == S.CANCELLED
    assert (info.value.namespace, info.value.name) == ("ns", "wr")
    fake.cancel.assert_not_called()


def test_run_execute_complete_timeout_cancels(fake: Fake) -> None:
    op = _op(timeout=600)
    with pytest.raises(WaitTimeoutError) as info:
        _complete(op, _event("timeout", "RUNNING"))
    assert info.value.last_state == S.RUNNING
    assert info.value.timeout == 600
    fake.cancel.assert_called_once_with(WR_ID, abort=True)


def test_run_execute_complete_error_cancels(fake: Fake) -> None:
    with pytest.raises(YellowDogException, match="message for error") as info:
        _complete(_op(), _event("error", None, error_type="HTTPError"))
    assert "HTTPError" in str(info.value)
    fake.cancel.assert_called_once_with(WR_ID, abort=True)


def test_run_execute_complete_error_cancel_failure_not_masked(fake: Fake) -> None:
    fake.cancel.side_effect = RuntimeError("cancel failed")
    with pytest.raises(YellowDogException, match="message for error"):
        _complete(_op(), _event("error", None))


@pytest.mark.parametrize("status", ["failure", "timeout"])
def test_run_execute_complete_unknown_state_cancels_and_reports(
    fake: Fake, status: str
) -> None:
    event = _event(status, "NEW_STATE")
    with pytest.raises(YellowDogException, match="NEW_STATE") as info:
        _complete(_op(), event)
    assert not isinstance(info.value, YellowDogStateError | WaitTimeoutError)
    fake.cancel.assert_called_once_with(WR_ID, abort=True)


def test_run_execute_complete_unexpected_status_cancels(fake: Fake) -> None:
    with pytest.raises(YellowDogException, match="Unexpected"):
        _complete(_op(), _event("weird", None))
    fake.cancel.assert_called_once_with(WR_ID, abort=True)


def test_run_execute_complete_records_id_for_on_kill(fake: Fake) -> None:
    op = _op()
    with pytest.raises(YellowDogException):
        _complete(op, _event("error", None))
    fake.cancel.reset_mock()
    op.on_kill()
    fake.cancel.assert_called_once_with(WR_ID, abort=True)


@pytest.mark.skipif(
    not SDK_OBJECTS_VIA_XCOM,
    reason="SDK objects cannot travel through XCom (Task 1 decision False); "
    "work_requirement and task_groups_and_tasks accept SDK objects or "
    "(context, jinja_env) callables only",
)
def test_run_accepts_sdk_objects_from_xcom() -> None:
    raise AssertionError("not applicable")  # pragma: no cover
