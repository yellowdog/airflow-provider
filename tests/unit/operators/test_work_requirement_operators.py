from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from airflow.sdk import Connection
from requests.exceptions import HTTPError
from yellowdog_client.model import RunSpecification, Task, TaskGroup, WorkRequirement

from tests.support import make_context, render
from yellowdog_provider.exceptions.yellowdog_exceptions import YellowDogArgumentError
from yellowdog_provider.hooks.yellowdog_hooks import YellowDogHook
from yellowdog_provider.operators import yellowdog_operators as ops
from yellowdog_provider.operators.yellowdog_operators import (
    AddPopulatedWorkRequirement,
    AddTaskGroupsToWorkRequirement,
    AddTasksToTaskGroup,
    AddWorkRequirement,
    CancelWorkRequirement,
)


def _with(obj: Any, **attrs: Any) -> Any:
    """Set server-populated attributes (e.g. id) that aren't constructor args."""
    for key, value in attrs.items():
        setattr(obj, key, value)
    return obj


def _wr(**kw: Any) -> WorkRequirement:
    return _with(WorkRequirement(namespace="ns", name="wr"), **kw)


def _tg(name: str, id: str | None = None) -> TaskGroup:
    return _with(
        TaskGroup(name=name, runSpecification=RunSpecification(taskTypes=["t"])), id=id
    )


def _task(id: str | None = None) -> Task:
    return _with(Task(), id=id)


def _run(op: Any, **ctx_kw: Any) -> Any:
    ctx = make_context(**ctx_kw)
    render(op, ctx)
    return op.execute(ctx)


def test_default_connection_id() -> None:
    op = AddWorkRequirement(task_id="t", work_requirement=_wr())
    assert op.connection_id == "yellowdog_default"


def test_positional_args_rejected() -> None:
    with pytest.raises(TypeError):
        AddWorkRequirement("t", _wr())  # type: ignore[misc]


def test_add_work_requirement_returns_id(mock_client: MagicMock) -> None:
    mock_client.work_client.add_work_requirement.return_value = _wr(id="wr-1")
    op = AddWorkRequirement(task_id="t", work_requirement=_wr())
    assert _run(op) == "wr-1"


def test_add_work_requirement_accepts_callable(mock_client: MagicMock) -> None:
    seen: dict[str, Any] = {}

    def gen(*, context: Any, jinja_env: Any) -> WorkRequirement:
        seen.update(context=context, jinja_env=jinja_env)
        return _wr()

    mock_client.work_client.add_work_requirement.return_value = _wr(id="wr-2")
    op = AddWorkRequirement(task_id="t", work_requirement=gen)
    assert _run(op) == "wr-2"
    assert seen["context"] is not None and seen["jinja_env"] is not None


def test_connection_id_templated(mock_client: MagicMock) -> None:
    mock_client.work_client.add_work_requirement.return_value = _wr(id="x")
    op = AddWorkRequirement(
        task_id="t", connection_id="{{ params.conn }}", work_requirement=_wr()
    )
    with patch.object(ops, "YellowDogHook", wraps=YellowDogHook) as hook_cls:
        _run(op, params={"conn": "my_conn"})
    hook_cls.assert_called_once_with("my_conn")


def test_add_task_groups_requires_id_or_name_at_init() -> None:
    with pytest.raises(YellowDogArgumentError):
        AddTaskGroupsToWorkRequirement(task_id="t", task_groups=[])


def test_add_task_groups_returns_ids_in_input_order(mock_client: MagicMock) -> None:
    existing = _tg("old", "id-old")
    mock_client.work_client.get_work_requirement_by_id.return_value = _wr(
        id="wr-1", taskGroups=[existing]
    )
    mock_client.work_client.update_work_requirement.return_value = _wr(
        id="wr-1",
        taskGroups=[
            _tg("b", "id-b"),
            existing,
            _tg("a", "id-a"),
        ],
    )
    op = AddTaskGroupsToWorkRequirement(
        task_id="t",
        work_requirement_id="wr-1",
        task_groups=[_tg("a"), _tg("b")],
    )
    assert _run(op) == ["id-a", "id-b"]


def test_add_tasks_by_id(mock_client: MagicMock) -> None:
    mock_client.work_client.add_tasks_to_task_group_by_id.return_value = [
        _task("t1"),
        _task("t2"),
    ]
    op = AddTasksToTaskGroup(
        task_id="t", task_group_id="tg-1", tasks=[_task(), _task()]
    )
    assert _run(op) == ["t1", "t2"]
    mock_client.work_client.add_tasks_to_task_group_by_id.assert_called_once()


def test_add_tasks_by_name(mock_client: MagicMock) -> None:
    mock_client.work_client.add_tasks_to_task_group_by_name.return_value = [_task("t1")]
    op = AddTasksToTaskGroup(
        task_id="t",
        namespace="ns",
        work_requirement_name="wr",
        task_group_name="tg",
        tasks=[_task()],
    )
    assert _run(op) == ["t1"]
    args = mock_client.work_client.add_tasks_to_task_group_by_name.call_args.args
    assert args[:3] == ("ns", "wr", "tg")


def test_add_tasks_missing_args_message() -> None:
    with pytest.raises(YellowDogArgumentError, match=" 'task_group_name'"):
        AddTasksToTaskGroup(task_id="t", tasks=[], namespace="ns")


def test_add_populated_rejects_preset_task_groups(mock_client: MagicMock) -> None:
    op = AddPopulatedWorkRequirement(
        task_id="t",
        work_requirement=_wr(taskGroups=[_tg("x")]),
        task_groups_and_tasks=[],
    )
    with pytest.raises(YellowDogArgumentError, match="task groups"):
        _run(op)
    mock_client.work_client.add_work_requirement.assert_not_called()


def test_add_populated_adds_groups_then_tasks(mock_client: MagicMock) -> None:
    mock_client.work_client.add_work_requirement.side_effect = lambda wr: wr
    mock_client.work_client.update_work_requirement.side_effect = lambda wr: wr
    mock_client.work_client.get_work_requirement_by_id.return_value = None
    g1, g2 = _tg("g1"), _tg("g2")
    t1, t2 = [_task()], [_task(), _task()]
    pairs = [(g1, t1), (g2, t2)]
    wr = _wr(id="wr-9")
    op = AddPopulatedWorkRequirement(
        task_id="t", work_requirement=wr, task_groups_and_tasks=pairs
    )
    assert _run(op) == "wr-9"

    update = mock_client.work_client.update_work_requirement
    update.assert_called_once()
    sent = update.call_args.args[0].taskGroups
    expected = [g2, g1] if ops._TASK_GROUP_SUBMISSION_REVERSED else [g1, g2]
    assert sent == expected

    calls = mock_client.work_client.add_tasks_to_task_group_by_name.call_args_list
    assert [c.args for c in calls] == [
        ("ns", "wr", "g1", t1),
        ("ns", "wr", "g2", t2),
    ]
    assert pairs == [(g1, t1), (g2, t2)]  # input list not mutated


def test_cancel_tolerates_invalid_transition(mock_client: MagicMock) -> None:
    mock_client.work_client.get_work_requirement_by_id.return_value = _wr(
        id="wr-1", status=ops.WorkRequirementStatus.COMPLETED
    )
    mock_client.work_client.cancel_work_requirement_by_id.side_effect = HTTPError(
        "400: Invalid Transition from COMPLETED"
    )
    op = CancelWorkRequirement(task_id="t", work_requirement_id="wr-1")
    assert _run(op) == "wr-1"


def test_cancel_reraises_other_http_errors(mock_client: MagicMock) -> None:
    mock_client.work_client.get_work_requirement_by_id.return_value = _wr(id="wr-1")
    mock_client.work_client.cancel_work_requirement_by_id.side_effect = HTTPError(
        "500: boom"
    )
    op = CancelWorkRequirement(task_id="t", work_requirement_id="wr-1")
    with pytest.raises(HTTPError):
        _run(op)


def test_id_rendering_to_None_raises(mock_client: MagicMock) -> None:
    op = CancelWorkRequirement(
        task_id="t",
        work_requirement_id="{{ ti.xcom_pull(task_ids='missing') }}",
    )
    with pytest.raises(YellowDogArgumentError, match="upstream XCom"):
        _run(op)
    mock_client.work_client.get_work_requirement_by_id.assert_not_called()


def test_hook_closed_after_execute(yd_connection: Connection) -> None:
    client = MagicMock(name="client")
    client.work_client.add_work_requirement.side_effect = RuntimeError("boom")
    op = AddWorkRequirement(
        task_id="t", connection_id="yd_test", work_requirement=_wr()
    )
    with (
        patch(
            "yellowdog_provider.hooks.yellowdog_hooks.PlatformClient.create",
            return_value=client,
        ),
        pytest.raises(RuntimeError),
    ):
        _run(op)
    client.close.assert_called_once()
