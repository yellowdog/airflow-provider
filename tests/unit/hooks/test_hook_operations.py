"""Typed operations on YellowDogHook, including the run-stamped submit."""

import copy
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock

import pytest
from requests import HTTPError, Response
from yellowdog_client.model import (
    ComputeRequirementStatus,
    ComputeRequirementTemplateUsage,
    ConfiguredWorkerPool,
    ProvisionedWorkerPool,
    RunSpecification,
    Task,
    TaskGroup,
    TaskSummary,
    WorkerPoolStatus,
    WorkRequirement,
    WorkRequirementSearch,
    WorkRequirementStatus,
    WorkRequirementSummary,
)
from yellowdog_client.model.exceptions import InvalidRequestException

from yellowdog_provider.exceptions.yellowdog_exceptions import (
    YellowDogArgumentError,
    YellowDogException,
)
from yellowdog_provider.hooks.yellowdog_hooks import (
    SubmitResult,
    YellowDogHook,
    _owned_cycle,
)
from yellowdog_provider.utils.run_stamp import (
    RunStamp,
    derive_name,
    task_tag_prefix_for,
    try_name,
)

NS = "ns"
STAMP = RunStamp("dag", "run-1", "task", -1)
OTHER = RunStamp("dag", "run-0", "task", -1)
MAPPED_1 = RunStamp("dag", "run-1", "task", 1)
MAPPED_12 = RunStamp("dag", "run-1", "task", 12)
T0 = datetime(2026, 1, 1, tzinfo=UTC)


def _tag(stamp: RunStamp, cycle: int) -> str:
    """The tag of an attempt submitted in clear cycle ``cycle``."""
    return f"{stamp.tag()}:c{cycle}"


def _http(status: int) -> HTTPError:
    response = Response()
    response.status_code = status
    return HTTPError(f"{status} Client Error", response=response)


def _groups(
    spec: Sequence[tuple[str, int]] = (("g1", 2), ("g2", 1)),
) -> list[tuple[TaskGroup, list[Task]]]:
    return [
        (
            TaskGroup(name=name, runSpecification=RunSpecification(taskTypes=["t"])),
            [Task(name=f"{name}-{i}", taskType="t") for i in range(count)],
        )
        for name, count in spec
    ]


class FakePlatform:
    """A tiny in-memory work service behind a MagicMock PlatformClient."""

    def __init__(self, client: MagicMock) -> None:
        self.client = client
        self.by_id: dict[str, WorkRequirement] = {}
        self.counter = 0
        self.searches: list[WorkRequirementSearch] = []
        self.tag_search_works = True
        work = client.work_client
        work.get_work_requirement_by_name.side_effect = self._get_by_name
        work.get_work_requirement_by_id.side_effect = lambda i: self.by_id[i]
        work.get_work_requirements.side_effect = self._search
        work.add_work_requirement.side_effect = self._add
        work.update_work_requirement.side_effect = self._update
        work.add_tasks_to_task_group_by_name.side_effect = self._add_tasks
        work.cancel_work_requirement_by_id.side_effect = self._cancel

    def existing(
        self,
        name: str,
        stamp: RunStamp | None,
        status: WorkRequirementStatus,
        groups: Sequence[tuple[str, int]] = (("g1", 2), ("g2", 1)),
        age: int = 0,
        cycle: int | None = 1,
        tag: str | None = None,
    ) -> WorkRequirement:
        if tag is None and stamp is not None:
            tag = stamp.tag() if cycle is None else _tag(stamp, cycle)
        wr = WorkRequirement(namespace=NS, name=name, tag=tag)
        self.counter += 1
        wr.id = f"old-{self.counter}"
        wr.status = status
        wr.createdTime = T0 + timedelta(minutes=age)
        task_groups: list[TaskGroup] = []
        for group_name, count in groups:
            tg = TaskGroup(
                name=group_name, runSpecification=RunSpecification(taskTypes=["t"])
            )
            tg.taskSummary = TaskSummary(taskCount=count)
            task_groups.append(tg)
        wr.taskGroups = task_groups
        self.by_id[wr.id] = wr
        return wr

    def _get_by_name(self, namespace: str, name: str) -> WorkRequirement:
        for wr in self.by_id.values():
            if wr.namespace == namespace and wr.name == name:
                return wr
        raise _http(404)

    def _search(self, search: WorkRequirementSearch) -> MagicMock:
        self.searches.append(search)
        found = [
            WorkRequirementSummary(
                id=wr.id,
                namespace=wr.namespace,
                name=wr.name,
                tag=wr.tag,
                createdTime=wr.createdTime,
                status=wr.status,
            )
            for wr in self.by_id.values()
            if (search.namespaces is None or wr.namespace in search.namespaces)
            # deliberately loose (prefix) so the hook's exact filter is tested
            and (
                search.tag is None
                or (
                    self.tag_search_works and (wr.tag or "").startswith(search.tag[:20])
                )
            )
            and (search.statuses is None or wr.status in search.statuses)
        ]
        result = MagicMock()
        result.list_all.return_value = found
        result.iterate.return_value = iter(found)
        return result

    def _add(self, wr: WorkRequirement) -> WorkRequirement:
        added = copy.deepcopy(wr)
        self.counter += 1
        added.id = f"new-{self.counter}"
        added.status = WorkRequirementStatus.RUNNING
        added.createdTime = T0 + timedelta(days=1)
        self.by_id[added.id] = added
        return copy.deepcopy(added)

    def _update(self, wr: WorkRequirement) -> WorkRequirement:
        assert wr.id is not None
        self.by_id[wr.id].taskGroups = copy.deepcopy(wr.taskGroups)
        return copy.deepcopy(self.by_id[wr.id])

    def _add_tasks(
        self, namespace: str, name: str, group: str, tasks: list[Task]
    ) -> list[Task]:
        wr = self._get_by_name(namespace, name)
        for tg in wr.taskGroups or []:
            if tg.name == group:
                before = 0 if tg.taskSummary is None else tg.taskSummary.taskCount
                tg.taskSummary = TaskSummary(taskCount=before + len(tasks))
        return [copy.deepcopy(t) for t in tasks]

    def _cancel(self, wr_id: str, abort: bool) -> WorkRequirement:
        wr = self.by_id[wr_id]
        wr.status = WorkRequirementStatus.CANCELLING
        return wr

    @property
    def work(self) -> Any:
        return self.client.work_client


@pytest.fixture
def platform(mock_client: MagicMock) -> FakePlatform:
    return FakePlatform(mock_client)


def _submit(
    *,
    name: str = "wr",
    try_number: int = 1,
    cycle: int = 1,
    groups: list[tuple[TaskGroup, list[Task]]] | None = None,
) -> SubmitResult:
    return YellowDogHook("yd_test").submit_work_requirement(
        WorkRequirement(namespace=NS, name=name),
        _groups() if groups is None else groups,
        stamp=STAMP,
        try_number=try_number,
        cycle=cycle,
    )


def _assert_built(platform: FakePlatform, name: str, cycle: int = 1) -> None:
    work = platform.work
    added = work.add_work_requirement.call_args.args[0]
    assert added.name == name
    assert added.tag == _tag(STAMP, cycle)
    assert not added.taskGroups
    updated = work.update_work_requirement.call_args.args[0]
    assert [tg.name for tg in updated.taskGroups] == ["g1", "g2"]
    calls = work.add_tasks_to_task_group_by_name.call_args_list
    assert [(c.args[0], c.args[1], c.args[2], len(c.args[3])) for c in calls] == [
        (NS, name, "g1", 2),
        (NS, name, "g2", 1),
    ]


# --- failure partway through a submission -----------------------------------


def test_failure_adding_tasks_cancels_partial_work_requirement(
    platform: FakePlatform,
) -> None:
    error = RuntimeError("bad task in group 2")
    platform.work.add_tasks_to_task_group_by_name.side_effect = [[], error]
    with pytest.raises(RuntimeError) as info:
        _submit()
    assert info.value is error
    platform.work.cancel_work_requirement_by_id.assert_called_once_with("new-1", True)


def test_failure_updating_task_groups_cancels_partial_work_requirement(
    platform: FakePlatform,
) -> None:
    error = _http(503)
    platform.work.update_work_requirement.side_effect = error
    with pytest.raises(HTTPError) as info:
        _submit()
    assert info.value is error
    platform.work.cancel_work_requirement_by_id.assert_called_once_with("new-1", True)
    platform.work.add_tasks_to_task_group_by_name.assert_not_called()


def test_failure_adding_work_requirement_cancels_nothing(
    platform: FakePlatform,
) -> None:
    platform.work.add_work_requirement.side_effect = RuntimeError("down")
    with pytest.raises(RuntimeError, match="down"):
        _submit()
    platform.work.cancel_work_requirement_by_id.assert_not_called()


def test_failed_cancel_does_not_mask_submission_error(
    platform: FakePlatform,
) -> None:
    error = RuntimeError("bad task in group 2")
    platform.work.add_tasks_to_task_group_by_name.side_effect = [[], error]
    platform.work.cancel_work_requirement_by_id.side_effect = RuntimeError("no cancel")
    with pytest.raises(RuntimeError) as info:
        _submit()
    assert info.value is error
    platform.work.cancel_work_requirement_by_id.assert_called_once_with("new-1", True)


def test_interrupt_during_submission_cancels_partial_work_requirement(
    platform: FakePlatform,
) -> None:
    platform.work.add_tasks_to_task_group_by_name.side_effect = KeyboardInterrupt
    with pytest.raises(KeyboardInterrupt):
        _submit()
    platform.work.cancel_work_requirement_by_id.assert_called_once_with("new-1", True)


# --- decision table ---------------------------------------------------------


def test_submit_new(platform: FakePlatform) -> None:
    result = _submit()
    assert result.action == "submitted"
    assert result.name == "wr"
    assert result.work_requirement_id == "new-1"
    _assert_built(platform, "wr")
    platform.work.cancel_work_requirement_by_id.assert_not_called()


def test_submit_new_derives_name_when_empty(platform: FakePlatform) -> None:
    result = _submit(name="")
    assert result.name == derive_name(STAMP)
    _assert_built(platform, derive_name(STAMP))


def test_submit_searches_own_stamp_tag(platform: FakePlatform) -> None:
    _submit()
    assert platform.searches[0].namespaces == [NS]
    assert platform.searches[0].tag == _tag(STAMP, 1)


def test_submit_name_owned_by_other_run_gets_fresh_copy(
    platform: FakePlatform,
) -> None:
    other = platform.existing("wr", OTHER, WorkRequirementStatus.RUNNING)
    result = _submit(try_number=1)
    assert result.action == "submitted"
    assert result.name == try_name("wr", 1)
    _assert_built(platform, try_name("wr", 1))
    work = platform.work
    work.cancel_work_requirement_by_id.assert_not_called()
    for call in work.update_work_requirement.call_args_list:
        assert call.args[0].id != other.id
    assert other.status == WorkRequirementStatus.RUNNING


def test_submit_name_owned_by_untagged_wr_gets_fresh_copy(
    platform: FakePlatform,
) -> None:
    platform.existing("wr", None, WorkRequirementStatus.COMPLETED)
    result = _submit(try_number=2)
    assert (result.action, result.name) == ("submitted", try_name("wr", 2))


def test_submit_try_name_taken_by_other_run_gets_further_suffix(
    platform: FakePlatform,
) -> None:
    platform.existing("wr", OTHER, WorkRequirementStatus.RUNNING)
    platform.existing(try_name("wr", 1), OTHER, WorkRequirementStatus.RUNNING)
    result = _submit(try_number=1)
    assert result.action == "submitted"
    assert result.name not in ("wr", try_name("wr", 1))
    assert result.name.startswith("wr-") and result.name.endswith("-try1")
    assert len(result.name) <= 60
    _assert_built(platform, result.name)
    platform.work.cancel_work_requirement_by_id.assert_not_called()
    # deterministic for the same stamp: forget the new WR and submit again
    del platform.by_id[result.work_requirement_id]
    assert _submit(try_number=1).name == result.name


@pytest.mark.parametrize(
    "status",
    [
        WorkRequirementStatus.RUNNING,
        WorkRequirementStatus.HELD,
        WorkRequirementStatus.FINISHING,
    ],
)
def test_submit_resumes_unfinished_own_attempt(
    platform: FakePlatform, status: WorkRequirementStatus
) -> None:
    own = platform.existing("wr", STAMP, status)
    result = _submit(try_number=2)
    assert result == SubmitResult(
        work_requirement_id=str(own.id), name="wr", action="resumed"
    )
    work = platform.work
    work.add_work_requirement.assert_not_called()
    work.update_work_requirement.assert_not_called()
    work.add_tasks_to_task_group_by_name.assert_not_called()
    work.cancel_work_requirement_by_id.assert_not_called()


def test_submit_resumes_latest_own_try_attempt(platform: FakePlatform) -> None:
    platform.existing("wr", STAMP, WorkRequirementStatus.FAILED, age=0)
    latest = platform.existing(
        try_name("wr", 2), STAMP, WorkRequirementStatus.RUNNING, age=5
    )
    result = _submit(try_number=3)
    assert (result.action, result.work_requirement_id, result.name) == (
        "resumed",
        latest.id,
        try_name("wr", 2),
    )
    platform.work.add_work_requirement.assert_not_called()


def test_submit_completed_same_cycle_already_completed(platform: FakePlatform) -> None:
    own = platform.existing("wr", STAMP, WorkRequirementStatus.COMPLETED, cycle=2)
    result = _submit(try_number=2, cycle=2)
    assert result == SubmitResult(
        work_requirement_id=str(own.id), name="wr", action="already_completed"
    )
    platform.work.add_work_requirement.assert_not_called()
    platform.work.cancel_work_requirement_by_id.assert_not_called()


def test_submit_completed_earlier_cycle_resubmits(platform: FakePlatform) -> None:
    # Completed in cycle 2; the task instance was cleared since (cycle 4)
    own = platform.existing("wr", STAMP, WorkRequirementStatus.COMPLETED, cycle=2)
    result = _submit(try_number=2, cycle=4)
    assert (result.action, result.name) == ("submitted", try_name("wr", 2))
    assert result.work_requirement_id != own.id
    _assert_built(platform, try_name("wr", 2), cycle=4)
    platform.work.cancel_work_requirement_by_id.assert_not_called()


def test_submit_completed_without_cycle_resubmits(platform: FakePlatform) -> None:
    # A tag with no cycle (exactly the stamp): the cycle is unknown
    platform.existing("wr", STAMP, WorkRequirementStatus.COMPLETED, cycle=None)
    result = _submit(try_number=2, cycle=1)
    assert (result.action, result.name) == ("submitted", try_name("wr", 2))


@pytest.mark.parametrize("cycle", [None, 0, 1])
def test_submit_resumes_unfinished_attempt_from_any_cycle(
    platform: FakePlatform, cycle: int | None
) -> None:
    own = platform.existing("wr", STAMP, WorkRequirementStatus.RUNNING, cycle=cycle)
    result = _submit(try_number=3, cycle=5)
    assert (result.action, result.work_requirement_id) == ("resumed", own.id)
    platform.work.add_work_requirement.assert_not_called()


def test_submit_finds_earlier_cycle_attempt_by_name_when_tag_search_misses(
    platform: FakePlatform,
) -> None:
    # A suffixed name (the plain and -try1 names belong to another run) from
    # cycle 1, completed; after a clear (cycle 3) it is found by name and redone
    platform.existing("wr", OTHER, WorkRequirementStatus.RUNNING)
    platform.existing(try_name("wr", 1), OTHER, WorkRequirementStatus.RUNNING)
    first = _submit(try_number=1, cycle=1)
    platform.by_id[first.work_requirement_id].status = WorkRequirementStatus.COMPLETED
    platform.tag_search_works = False
    assert _submit(try_number=1, cycle=1).action == "already_completed"
    platform.work.add_tasks_to_task_group_by_name.reset_mock()
    result = _submit(try_number=2, cycle=3)
    assert result.action == "submitted"
    assert result.work_requirement_id != first.work_requirement_id
    _assert_built(platform, result.name, cycle=3)


@pytest.mark.parametrize(
    ("tag", "owned"),
    [
        (STAMP.tag(), True),
        (_tag(STAMP, 0), True),
        (_tag(STAMP, 3), True),
        (_tag(OTHER, 3), False),
        (f"{STAMP.tag()}:c", False),
        (f"{STAMP.tag()}:cx", False),
        (f"{STAMP.tag()}:c3:x", False),
        (f"{STAMP.tag()}:c-1", False),
        (f"{STAMP.tag()}0", False),
        (f"x{STAMP.tag()}:c3", False),
    ],
)
def test_submit_owns_cycle_suffixed_tags_only_for_same_stamp(
    platform: FakePlatform, tag: str, owned: bool
) -> None:
    other = platform.existing("wr", None, WorkRequirementStatus.RUNNING, tag=tag)
    result = _submit(try_number=1, cycle=3)
    if owned:
        assert (result.action, result.work_requirement_id) == ("resumed", other.id)
    else:
        assert (result.action, result.name) == ("submitted", try_name("wr", 1))
        assert other.status == WorkRequirementStatus.RUNNING
        platform.work.cancel_work_requirement_by_id.assert_not_called()


def test_owned_cycle_never_confuses_map_index_prefixes() -> None:
    assert _owned_cycle(f"{MAPPED_1.tag()}:c3", MAPPED_1.tag()) == (True, 3)
    assert _owned_cycle(MAPPED_1.tag(), MAPPED_1.tag()) == (True, None)
    assert _owned_cycle(f"{MAPPED_12.tag()}:c3", MAPPED_1.tag()) == (False, None)
    assert _owned_cycle(MAPPED_12.tag(), MAPPED_1.tag()) == (False, None)


def test_submit_rejects_negative_cycle(platform: FakePlatform) -> None:
    with pytest.raises(YellowDogArgumentError):
        _submit(cycle=-1)
    platform.work.add_work_requirement.assert_not_called()


@pytest.mark.parametrize(
    "status",
    [
        WorkRequirementStatus.FAILED,
        WorkRequirementStatus.CANCELLED,
        # on its way to CANCELLED, e.g. cancelled by the previous try's timeout
        WorkRequirementStatus.CANCELLING,
    ],
)
def test_submit_failed_own_attempt_fresh_try_name(
    platform: FakePlatform, status: WorkRequirementStatus
) -> None:
    platform.existing("wr", STAMP, status)
    result = _submit(try_number=3)
    assert (result.action, result.name) == ("submitted", try_name("wr", 3))
    _assert_built(platform, try_name("wr", 3))
    platform.work.cancel_work_requirement_by_id.assert_not_called()


def test_submit_failed_own_attempt_same_try_name_taken_gets_suffix(
    platform: FakePlatform,
) -> None:
    # e.g. the same try number ran twice: wr-try2 is this stamp's, failed
    platform.existing("wr", STAMP, WorkRequirementStatus.FAILED, age=0)
    platform.existing(try_name("wr", 2), STAMP, WorkRequirementStatus.FAILED, age=5)
    result = _submit(try_number=2)
    assert result.action == "submitted"
    assert result.name not in ("wr", try_name("wr", 2))
    assert result.name.endswith("-try2")


@pytest.mark.parametrize(
    "built",
    [
        (),  # crashed after adding the WR
        (("g1", 0), ("g2", 0)),  # after setting the task groups
        (("g1", 2), ("g2", 0)),  # part-way through adding tasks
        (("g1", 2),),  # fewer groups
        (("g2", 1), ("g1", 2)),  # groups not as supplied
    ],
)
def test_submit_partly_built_cancelled_and_resubmitted(
    platform: FakePlatform, built: tuple[tuple[str, int], ...]
) -> None:
    own = platform.existing("wr", STAMP, WorkRequirementStatus.RUNNING, groups=built)
    result = _submit(try_number=2)
    platform.work.cancel_work_requirement_by_id.assert_called_once_with(own.id, True)
    assert (result.action, result.name) == ("submitted", try_name("wr", 2))
    _assert_built(platform, try_name("wr", 2))


def test_submit_completed_own_attempt_not_checked_for_build(
    platform: FakePlatform,
) -> None:
    # partly built, but the platform marked it COMPLETED: completed is completed
    own = platform.existing(
        "wr", STAMP, WorkRequirementStatus.COMPLETED, groups=(("g1", 2),)
    )
    result = _submit(try_number=2)
    assert (result.action, result.work_requirement_id) == (
        "already_completed",
        own.id,
    )
    platform.work.get_work_requirement_by_id.assert_not_called()
    platform.work.get_tasks.assert_not_called()
    platform.work.add_work_requirement.assert_not_called()
    platform.work.cancel_work_requirement_by_id.assert_not_called()


def test_submit_finds_own_try_attempt_by_name_when_tag_search_misses(
    platform: FakePlatform,
) -> None:
    platform.existing("wr", STAMP, WorkRequirementStatus.FAILED, age=0)
    latest = platform.existing(
        try_name("wr", 2), STAMP, WorkRequirementStatus.RUNNING, age=5
    )
    platform.tag_search_works = False
    result = _submit(try_number=3)
    assert (result.action, result.work_requirement_id) == ("resumed", latest.id)
    platform.work.add_work_requirement.assert_not_called()


def test_submit_finds_own_suffixed_attempt_by_name_when_tag_search_misses(
    platform: FakePlatform,
) -> None:
    platform.existing("wr", OTHER, WorkRequirementStatus.RUNNING)
    platform.existing(try_name("wr", 1), OTHER, WorkRequirementStatus.RUNNING)
    first = _submit(try_number=1)  # gets a suffixed name
    platform.tag_search_works = False
    result = _submit(try_number=2)
    assert (result.action, result.work_requirement_id, result.name) == (
        "resumed",
        first.work_requirement_id,
        first.name,
    )


def test_submit_does_not_look_up_the_same_name_twice(platform: FakePlatform) -> None:
    _submit(try_number=3)
    names = [
        c.args[1] for c in platform.work.get_work_requirement_by_name.call_args_list
    ]
    assert len(names) == len(set(names))


def test_submit_counts_tasks_by_search_when_no_task_summary(
    platform: FakePlatform,
) -> None:
    own = platform.existing("wr", STAMP, WorkRequirementStatus.RUNNING)
    assert own.taskGroups is not None
    for tg in own.taskGroups:
        tg.taskSummary = None
        tg.id = f"tg-{tg.name}"
    counts = {"tg-g1": 2, "tg-g2": 1}

    def get_tasks(search: Any) -> MagicMock:
        result = MagicMock()
        result.iterate.return_value = iter([Task()] * counts[search.taskGroupId])
        return result

    platform.work.get_tasks.side_effect = get_tasks
    result = _submit(try_number=2)
    assert result.action == "resumed"
    searches = [c.args[0] for c in platform.work.get_tasks.call_args_list]
    assert [(s.workRequirementId, s.taskGroupId) for s in searches] == [
        (own.id, "tg-g1"),
        (own.id, "tg-g2"),
    ]


def test_submit_reraises_non_404_lookup_errors(platform: FakePlatform) -> None:
    platform.work.get_work_requirement_by_name.side_effect = _http(500)
    with pytest.raises(HTTPError):
        _submit()
    platform.work.add_work_requirement.assert_not_called()


def test_submit_rejects_work_requirement_with_task_groups(
    platform: FakePlatform,
) -> None:
    wr = WorkRequirement(namespace=NS, name="wr", taskGroups=[_groups()[0][0]])
    with pytest.raises(YellowDogArgumentError):
        YellowDogHook("yd_test").submit_work_requirement(
            wr, _groups(), stamp=STAMP, try_number=1, cycle=1
        )
    platform.work.add_work_requirement.assert_not_called()


def test_submit_does_not_mutate_input(platform: FakePlatform) -> None:
    platform.existing("wr", STAMP, WorkRequirementStatus.FAILED)
    wr = WorkRequirement(namespace=NS, name="", tag="user-tag")
    groups = _groups()
    wr_before = copy.deepcopy(wr)
    groups_before = copy.deepcopy(groups)
    YellowDogHook("yd_test").submit_work_requirement(
        wr, groups, stamp=STAMP, try_number=2, cycle=1
    )
    assert wr == wr_before
    assert groups == groups_before
    work = platform.work
    assert work.add_work_requirement.call_args.args[0] is not wr
    sent_groups = work.update_work_requirement.call_args.args[0].taskGroups
    assert all(a is not b for a, (b, _) in zip(sent_groups, groups, strict=True))
    for call, (_, tasks) in zip(
        work.add_tasks_to_task_group_by_name.call_args_list, groups, strict=True
    ):
        assert all(a is not b for a, b in zip(call.args[3], tasks, strict=True))


# --- cancel -----------------------------------------------------------------


@pytest.mark.parametrize(
    "error",
    [
        HTTPError("400 Client Error: Invalid Transition from COMPLETED"),
        InvalidRequestException("invalid transition from CANCELLED to CANCELLING"),
    ],
)
def test_cancel_tolerates_invalid_transition(
    mock_client: MagicMock, error: Exception
) -> None:
    mock_client.work_client.cancel_work_requirement_by_id.side_effect = error
    YellowDogHook("yd_test").cancel_work_requirement("wr-id", abort=True)
    mock_client.work_client.cancel_work_requirement_by_id.assert_called_once_with(
        "wr-id", True
    )


@pytest.mark.parametrize(
    "error",
    [
        _http(500),
        InvalidRequestException("namespace not permitted"),
        RuntimeError("invalid transition"),
    ],
)
def test_cancel_reraises_other_errors(mock_client: MagicMock, error: Exception) -> None:
    mock_client.work_client.cancel_work_requirement_by_id.side_effect = error
    with pytest.raises(type(error)):
        YellowDogHook("yd_test").cancel_work_requirement("wr-id", abort=False)


# --- run sweep --------------------------------------------------------------


def test_find_unfinished_run_work_requirements_filters_by_tag_prefix_and_status(
    platform: FakePlatform,
) -> None:
    other_task = RunStamp("dag", "run-1", "other-task", -1)
    a = platform.existing("a", STAMP, WorkRequirementStatus.RUNNING)
    a_legacy = platform.existing("a0", STAMP, WorkRequirementStatus.RUNNING, cycle=None)
    b = platform.existing("b", other_task, WorkRequirementStatus.HELD, cycle=7)
    platform.existing("c", STAMP, WorkRequirementStatus.COMPLETED)
    platform.existing("d", OTHER, WorkRequirementStatus.RUNNING)
    platform.existing("e", None, WorkRequirementStatus.RUNNING)
    # a name that looks like the run is never matched without the tag
    platform.existing(STAMP.run_tag_prefix(), None, WorkRequirementStatus.RUNNING)
    ids = YellowDogHook("yd_test").find_unfinished_run_work_requirements(
        NS, STAMP.run_tag_prefix()
    )
    assert sorted(ids) == sorted([str(a.id), str(a_legacy.id), str(b.id)])
    search = platform.searches[-1]
    assert search.namespaces == [NS]
    assert search.statuses is not None
    assert set(search.statuses) == {s for s in WorkRequirementStatus if not s.finished}


def test_find_unfinished_run_work_requirements_all_namespaces(
    platform: FakePlatform,
) -> None:
    here = platform.existing("a", STAMP, WorkRequirementStatus.RUNNING)
    elsewhere = platform.existing("b", STAMP, WorkRequirementStatus.RUNNING)
    elsewhere.namespace = "other-ns"
    other_run = platform.existing("c", OTHER, WorkRequirementStatus.RUNNING)
    other_run.namespace = "other-ns"
    ids = YellowDogHook("yd_test").find_unfinished_run_work_requirements(
        None, STAMP.run_tag_prefix()
    )
    assert sorted(ids) == sorted([str(here.id), str(elsewhere.id)])
    assert platform.searches[-1].namespaces is None


@pytest.mark.parametrize("prefix", ["", "airflow:", "airflow", "airflow:XYZ"])
def test_find_unfinished_run_work_requirements_rejects_non_run_prefix(
    platform: FakePlatform, prefix: str
) -> None:
    platform.existing("a", STAMP, WorkRequirementStatus.RUNNING)
    with pytest.raises(YellowDogArgumentError):
        YellowDogHook("yd_test").find_unfinished_run_work_requirements(NS, prefix)
    platform.work.get_work_requirements.assert_not_called()


def test_find_unfinished_run_work_requirements_scoped_to_task_prefixes(
    platform: FakePlatform,
) -> None:
    other_task = RunStamp("dag", "run-1", "other-task", -1)
    third_task = RunStamp("dag", "run-1", "third", -1)
    mapped = [RunStamp("dag", "run-1", "mapped", i) for i in (1, 12)]
    a = platform.existing("a", STAMP, WorkRequirementStatus.RUNNING)
    b = platform.existing("b", other_task, WorkRequirementStatus.HELD, cycle=7)
    m = [
        platform.existing(f"m{s.map_index}", s, WorkRequirementStatus.RUNNING)
        for s in mapped
    ]
    platform.existing("c", third_task, WorkRequirementStatus.RUNNING)
    platform.existing("d", STAMP, WorkRequirementStatus.COMPLETED)
    platform.existing("e", OTHER, WorkRequirementStatus.RUNNING)
    ids = YellowDogHook("yd_test").find_unfinished_run_work_requirements(
        NS,
        STAMP.run_tag_prefix(),
        [
            STAMP.task_tag_prefix(),
            other_task.task_tag_prefix(),
            mapped[0].task_tag_prefix(),
        ],
    )
    assert sorted(ids) == sorted([str(a.id), str(b.id), *(str(w.id) for w in m)])
    assert task_tag_prefix_for(STAMP, "other-task") == other_task.task_tag_prefix()


def test_find_unfinished_run_work_requirements_scoped_to_map_index(
    platform: FakePlatform,
) -> None:
    one = platform.existing("m1", MAPPED_1, WorkRequirementStatus.RUNNING, cycle=2)
    one_legacy = platform.existing(
        "m1-legacy", MAPPED_1, WorkRequirementStatus.HELD, cycle=None
    )
    twelve = platform.existing("m12", MAPPED_12, WorkRequirementStatus.RUNNING)
    unmapped = platform.existing("a", STAMP, WorkRequirementStatus.RUNNING)
    other_task = RunStamp("dag", "run-1", "other-task", -1)
    b = platform.existing("b", other_task, WorkRequirementStatus.RUNNING)
    ids = YellowDogHook("yd_test").find_unfinished_run_work_requirements(
        NS, STAMP.run_tag_prefix(), [MAPPED_1.tag(), other_task.task_tag_prefix()]
    )
    # m1 does not match m12; the legacy tag (no cycle) is the prefix itself
    assert sorted(ids) == sorted([str(one.id), str(one_legacy.id), str(b.id)])
    assert twelve.status == unmapped.status == WorkRequirementStatus.RUNNING


def test_find_unfinished_run_work_requirements_empty_scope_makes_no_call(
    platform: FakePlatform,
) -> None:
    platform.existing("a", STAMP, WorkRequirementStatus.RUNNING)
    assert (
        YellowDogHook("yd_test").find_unfinished_run_work_requirements(
            NS, STAMP.run_tag_prefix(), []
        )
        == []
    )
    platform.work.get_work_requirements.assert_not_called()


@pytest.mark.parametrize(
    "prefix",
    [
        "airflow:abc",
        STAMP.run_tag_prefix(),
        f"{STAMP.task_tag_prefix()}:m",
        f"{STAMP.task_tag_prefix()}:mx",
        f"{STAMP.task_tag_prefix()}:m1:",
        _tag(STAMP, 0),
        OTHER.task_tag_prefix(),
        OTHER.tag(),
    ],
)
def test_find_unfinished_run_work_requirements_rejects_bad_task_prefix(
    platform: FakePlatform, prefix: str
) -> None:
    with pytest.raises(YellowDogArgumentError):
        YellowDogHook("yd_test").find_unfinished_run_work_requirements(
            NS, STAMP.run_tag_prefix(), [STAMP.task_tag_prefix(), prefix]
        )
    platform.work.get_work_requirements.assert_not_called()


# --- states, pools ----------------------------------------------------------


def test_get_states(mock_client: MagicMock) -> None:
    mock_client.work_client.get_work_requirement_by_id.return_value.status = (
        WorkRequirementStatus.RUNNING
    )
    mock_client.worker_pool_client.get_worker_pool_by_id.return_value.status = (
        WorkerPoolStatus.IDLE
    )
    mock_client.compute_client.get_compute_requirement_by_id.return_value.status = (
        ComputeRequirementStatus.RUNNING
    )
    hook = YellowDogHook("yd_test")
    assert hook.get_work_requirement_state("w") == WorkRequirementStatus.RUNNING
    assert hook.get_worker_pool_state("p") == WorkerPoolStatus.IDLE
    assert hook.get_compute_requirement_state("c") == ComputeRequirementStatus.RUNNING
    mock_client.work_client.get_work_requirement_by_id.assert_called_once_with("w")
    mock_client.worker_pool_client.get_worker_pool_by_id.assert_called_once_with("p")
    mock_client.compute_client.get_compute_requirement_by_id.assert_called_once_with(
        "c"
    )


def test_get_state_without_status_raises(mock_client: MagicMock) -> None:
    mock_client.work_client.get_work_requirement_by_id.return_value.status = None
    with pytest.raises(YellowDogException):
        YellowDogHook("yd_test").get_work_requirement_state("w")


def test_provision_worker_pool_returns_id(mock_client: MagicMock) -> None:
    usage = ComputeRequirementTemplateUsage(
        templateId="tpl", requirementNamespace=NS, requirementName="pool"
    )
    mock_client.worker_pool_client.provision_worker_pool.return_value = (
        ProvisionedWorkerPool(id="wp-1")
    )
    assert YellowDogHook("yd_test").provision_worker_pool(usage, None) == "wp-1"
    mock_client.worker_pool_client.provision_worker_pool.assert_called_once_with(
        usage, None
    )


def test_shutdown_terminate_returns_compute_requirement_id(
    mock_client: MagicMock,
) -> None:
    pools = mock_client.worker_pool_client
    pools.get_worker_pool_by_id.return_value = ProvisionedWorkerPool(
        id="wp-1", computeRequirementId="cr-1"
    )
    result = YellowDogHook("yd_test").shutdown_worker_pool(
        "wp-1", terminate_immediately=True
    )
    assert result == "cr-1"
    pools.shutdown_worker_pool_by_id.assert_called_once_with("wp-1")
    mock_client.compute_client.terminate_compute_requirement_by_id.assert_called_once_with(
        "cr-1"
    )


def test_shutdown_without_terminate_returns_none(mock_client: MagicMock) -> None:
    pools = mock_client.worker_pool_client
    pools.get_worker_pool_by_id.return_value = ProvisionedWorkerPool(
        id="wp-1", computeRequirementId="cr-1"
    )
    hook = YellowDogHook("yd_test")
    assert hook.shutdown_worker_pool("wp-1", terminate_immediately=False) is None
    pools.shutdown_worker_pool_by_id.assert_called_once_with("wp-1")
    mock_client.compute_client.terminate_compute_requirement_by_id.assert_not_called()


def test_shutdown_failure_still_terminates_then_raises(
    mock_client: MagicMock,
) -> None:
    pools = mock_client.worker_pool_client
    pools.get_worker_pool_by_id.return_value = ProvisionedWorkerPool(
        id="wp-1", computeRequirementId="cr-1"
    )
    pools.shutdown_worker_pool_by_id.side_effect = InvalidRequestException(
        "Pool is busy"
    )
    with pytest.raises(InvalidRequestException):
        YellowDogHook("yd_test").shutdown_worker_pool(
            "wp-1", terminate_immediately=True
        )
    mock_client.compute_client.terminate_compute_requirement_by_id.assert_called_once_with(
        "cr-1"
    )


@pytest.mark.parametrize(
    "error",
    [
        InvalidRequestException("Invalid Transition from SHUTDOWN"),
        HTTPError("400 Client Error: invalid transition"),
    ],
)
@pytest.mark.parametrize("terminate", [True, False])
def test_shutdown_already_shut_down_pool_tolerated(
    mock_client: MagicMock, error: Exception, terminate: bool
) -> None:
    pools = mock_client.worker_pool_client
    pools.get_worker_pool_by_id.return_value = ProvisionedWorkerPool(
        id="wp-1", computeRequirementId="cr-1", status=WorkerPoolStatus.SHUTDOWN
    )
    pools.shutdown_worker_pool_by_id.side_effect = error
    result = YellowDogHook("yd_test").shutdown_worker_pool(
        "wp-1", terminate_immediately=terminate
    )
    terminate_cr = mock_client.compute_client.terminate_compute_requirement_by_id
    if terminate:
        assert result == "cr-1"
        terminate_cr.assert_called_once_with("cr-1")
    else:
        assert result is None
        terminate_cr.assert_not_called()


def test_shutdown_invalid_transition_text_in_other_error_type_raises(
    mock_client: MagicMock,
) -> None:
    pools = mock_client.worker_pool_client
    pools.get_worker_pool_by_id.return_value = ProvisionedWorkerPool(
        id="wp-1", computeRequirementId="cr-1"
    )
    pools.shutdown_worker_pool_by_id.side_effect = RuntimeError("invalid transition")
    with pytest.raises(RuntimeError):
        YellowDogHook("yd_test").shutdown_worker_pool(
            "wp-1", terminate_immediately=False
        )


def test_shutdown_failure_without_terminate_raises(mock_client: MagicMock) -> None:
    pools = mock_client.worker_pool_client
    pools.get_worker_pool_by_id.return_value = ProvisionedWorkerPool(
        id="wp-1", computeRequirementId="cr-1"
    )
    pools.shutdown_worker_pool_by_id.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError):
        YellowDogHook("yd_test").shutdown_worker_pool(
            "wp-1", terminate_immediately=False
        )
    mock_client.compute_client.terminate_compute_requirement_by_id.assert_not_called()


def test_shutdown_configured_pool_raises(mock_client: MagicMock) -> None:
    mock_client.worker_pool_client.get_worker_pool_by_id.return_value = (
        ConfiguredWorkerPool(id="wp-1", namespace=NS, name="cfg")
    )
    with pytest.raises(YellowDogException):
        YellowDogHook("yd_test").shutdown_worker_pool(
            "wp-1", terminate_immediately=True
        )
    mock_client.worker_pool_client.shutdown_worker_pool_by_id.assert_not_called()


# --- add_work_requirement (no task groups) ----------------------------------


def _add(
    *, name: str = "wr", try_number: int = 1, cycle: int = 1
) -> tuple[SubmitResult, WorkRequirement]:
    wr = WorkRequirement(namespace=NS, name=name)
    result = YellowDogHook("yd_test").add_work_requirement(
        wr, stamp=STAMP, try_number=try_number, cycle=cycle
    )
    return result, wr


def test_add_new(platform: FakePlatform) -> None:
    result, wr = _add()
    assert (result.action, result.name) == ("submitted", "wr")
    added = platform.work.add_work_requirement.call_args.args[0]
    assert added.tag == _tag(STAMP, 1)
    platform.work.update_work_requirement.assert_not_called()
    assert wr.tag is None  # input not mutated


def test_add_empty_name_derived(platform: FakePlatform) -> None:
    result, wr = _add(name="")
    assert result.name == derive_name(STAMP)
    assert wr.name == ""


def test_add_completed_same_cycle_already_completed(platform: FakePlatform) -> None:
    own = platform.existing("wr", STAMP, WorkRequirementStatus.COMPLETED, cycle=2)
    result, _ = _add(try_number=2, cycle=2)
    assert result == SubmitResult(str(own.id), "wr", "already_completed")
    platform.work.add_work_requirement.assert_not_called()


def test_add_completed_earlier_cycle_fresh_name(platform: FakePlatform) -> None:
    platform.existing("wr", STAMP, WorkRequirementStatus.COMPLETED, cycle=2)
    result, _ = _add(try_number=2, cycle=4)
    assert (result.action, result.name) == ("submitted", try_name("wr", 2))


def test_add_cancelling_latest_fresh_name(platform: FakePlatform) -> None:
    platform.existing("wr", STAMP, WorkRequirementStatus.CANCELLING, cycle=1)
    result, _ = _add(try_number=2, cycle=1)
    assert (result.action, result.name) == ("submitted", try_name("wr", 2))
    platform.work.cancel_work_requirement_by_id.assert_not_called()


def test_add_unfinished_earlier_cycle_resumed(platform: FakePlatform) -> None:
    own = platform.existing("wr", STAMP, WorkRequirementStatus.RUNNING, cycle=1)
    result, _ = _add(try_number=2, cycle=3)
    assert result == SubmitResult(str(own.id), "wr", "resumed")
    platform.work.add_work_requirement.assert_not_called()


def test_add_name_held_by_other_stamp_fresh_suffixed_name(
    platform: FakePlatform,
) -> None:
    other = platform.existing("wr", OTHER, WorkRequirementStatus.RUNNING, cycle=1)
    result, _ = _add()
    assert result.action == "submitted"
    assert result.name not in ("wr", "")
    assert result.name.startswith("wr")
    assert platform.by_id[str(other.id)].status == WorkRequirementStatus.RUNNING
    platform.work.cancel_work_requirement_by_id.assert_not_called()


def test_add_rejects_negative_cycle(platform: FakePlatform) -> None:
    with pytest.raises(YellowDogArgumentError):
        _add(cycle=-1)
    platform.work.add_work_requirement.assert_not_called()


@pytest.mark.parametrize(
    "error",
    [
        InvalidRequestException("Invalid transition from TERMINATED"),
        HTTPError("400 Client Error: invalid transition"),
    ],
)
def test_shutdown_already_terminated_compute_requirement_tolerated(
    mock_client: MagicMock, error: Exception
) -> None:
    pools = mock_client.worker_pool_client
    pools.get_worker_pool_by_id.return_value = ProvisionedWorkerPool(
        id="wp-1", computeRequirementId="cr-1"
    )
    pools.shutdown_worker_pool_by_id.side_effect = InvalidRequestException(
        "invalid transition"
    )
    terminate = mock_client.compute_client.terminate_compute_requirement_by_id
    terminate.side_effect = error
    hook = YellowDogHook("yd_test")
    assert hook.shutdown_worker_pool("wp-1", terminate_immediately=True) == "cr-1"
    terminate.assert_called_once_with("cr-1")


def test_terminate_compute_requirement_other_errors_raise(
    mock_client: MagicMock,
) -> None:
    terminate = mock_client.compute_client.terminate_compute_requirement_by_id
    terminate.side_effect = InvalidRequestException("Quota exceeded")
    with pytest.raises(InvalidRequestException):
        YellowDogHook("yd_test").terminate_compute_requirement("cr-1")
    terminate.side_effect = RuntimeError("invalid transition")  # Not an SDK error
    with pytest.raises(RuntimeError):
        YellowDogHook("yd_test").terminate_compute_requirement("cr-1")
