"""
YellowDog Airflow Operators for managing work requirements and worker pools.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Collection, Sequence
from datetime import timedelta
from enum import Enum
from typing import Any, Literal, NoReturn, cast, get_args

from airflow.sdk import BaseOperator, BaseOperatorLink, Context
from yellowdog_client.model import (
    ComputeRequirementStatus,
    ComputeRequirementTemplateUsage,
    ConfiguredWorkerPool,
    ProvisionedWorkerPool,
    ProvisionedWorkerPoolProperties,
    Task,
    TaskGroup,
    WorkerPoolStatus,
    WorkRequirement,
    WorkRequirementStatus,
)

from yellowdog_provider.exceptions.yellowdog_exceptions import (
    WaitTimeoutError,
    YellowDogArgumentError,
    YellowDogException,
    YellowDogStateError,
)
from yellowdog_provider.hooks.yellowdog_hooks import (
    YellowDogHook,
)
from yellowdog_provider.links.yellowdog_links import (
    WorkerPoolPortalLink,
    WorkRequirementPortalLink,
)
from yellowdog_provider.sensors.yellowdog_sensors import (
    COMPUTE_REQUIREMENT_FINAL_STATES,
    WORK_REQUIREMENT_FINAL_STATES,
    WORKER_POOL_FINAL_STATES,
    XCOM_KEY,
)
from yellowdog_provider.triggers.yellowdog_triggers import (
    ResourceKind,
    YellowDogStateTrigger,
    event_error_message,
)
from yellowdog_provider.utils.airflow_compat import default_deferrable
from yellowdog_provider.utils.run_stamp import RunStamp, task_tag_prefix_for
from yellowdog_provider.utils.templating import (
    Templated,
    resolve,
    resolve_optional_str,
)
from yellowdog_provider.utils.waiting import resolve_states, wait_for_state
from yellowdog_provider.utils.yellowdog_utils import (
    get_work_requirement_by_id_or_name,
    get_worker_pool_by_id_or_name,
    require_id_or_name,
)


class YellowDogOperator(BaseOperator):
    """
    Base class for YellowDog operators.

    :param connection_id: connection to run the operator with (templated) or a
        Callable that generates the connection ID; defaults to
        ``yellowdog_default``
    :type connection_id: str | Callable
    """

    template_fields: Collection[str] = ("connection_id",)

    def __init__(
        self,
        *,
        connection_id: Templated[str] = YellowDogHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        # Airflow's BaseOperator.__init__ has partially-unknown default_args typing
        super().__init__(**kwargs)  # pyright: ignore[reportUnknownMemberType]
        self.connection_id = connection_id

    def get_hook(self) -> YellowDogHook:
        """Return a hook for the (rendered) connection ID."""
        return YellowDogHook(resolve(self.connection_id, "connection_id"))

    def _reached_before_deferring[S: Enum](
        self,
        state: S,
        targets: Collection[S],
        failures: Collection[S],
        describe: Callable[[S], Exception],
    ) -> bool:
        """
        The check a deferrable wait makes before deferring: true if ``state``
        is a target state; raises ``describe(state)`` if it is a failure state.
        """
        if state in targets:
            self.log.info("Reached status '%s'", state.value)
            return True
        if state in failures:
            raise describe(state)
        return False


class _WorkRequirementSubmitter(YellowDogOperator):
    """
    Base class for operators that submit a work requirement: records its ID
    (for :meth:`on_kill`) and derives the clear cycle.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._work_requirement_id: str | None = None

    def _cycle(self, max_tries: object) -> int:
        """The clear cycle: the task instance's ``max_tries`` (see above)."""
        if (
            isinstance(max_tries, int)
            and not isinstance(max_tries, bool)
            and max_tries >= 0
        ):
            return max_tries
        self.log.warning(
            "The task instance has no usable 'max_tries' (%r); using clear "
            "cycle 0, so a completed earlier attempt from cycle 0 will not be "
            "re-run after a clear",
            max_tries,
        )
        return 0

    def on_kill(self) -> None:
        """
        Cancels the work requirement this operator submitted or resumed,
        aborting its running tasks (best-effort: errors are logged, not
        raised). Does nothing if no work requirement has been submitted yet.
        """
        wr_id = self._work_requirement_id
        if wr_id is None:
            self.log.info("No work requirement submitted yet; nothing to cancel")
            return
        self._cancel(wr_id, "task killed")

    def _cancel(self, wr_id: str, reason: str) -> None:
        """Best-effort cancel with abort: log failures, never raise."""
        self.log.warning(
            "Cancelling work requirement ID '%s', aborting running tasks (%s)",
            wr_id,
            reason,
        )
        try:
            with self.get_hook() as hook:
                hook.cancel_work_requirement(wr_id, abort=True)
        except Exception:
            self.log.exception("Failed to cancel work requirement ID '%s'", wr_id)

    def _context_ti(self, context: Context) -> tuple[RunStamp, Any, int]:
        """The run stamp, task instance and clear cycle of the running task."""
        stamp = RunStamp.from_context(context)
        if "ti" not in context:  # Defensive: RunStamp.from_context checks this
            raise YellowDogException("Task context has no task instance ('ti')")
        ti = context["ti"]
        return stamp, ti, self._cycle(getattr(ti, "max_tries", None))


class AddWorkRequirement(_WorkRequirementSubmitter):
    """
    Add a YellowDog work requirement to the platform. All arguments are
    keyword-only.

    :param task_id: the Airflow task ID
    :type task_id: str
    :param connection_id: connection to run the operator with (templated) or a
        Callable that generates the connection ID; defaults to
        ``yellowdog_default``
    :type connection_id: str | Callable
    :param work_requirement: a YellowDog WorkRequirement object or a
        Callable that generates a WorkRequirement object (templated)
    :type work_requirement: WorkRequirement | Callable

    The work requirement is tagged with the task's run stamp, so a retry (or
    a re-run after the task is cleared) returns this task's earlier work
    requirement instead of failing on a duplicate name, provided that is
    unfinished (or completed in the current clear cycle); otherwise a new one
    is added, under a suffixed name if the name is held by another work
    requirement. If the task is killed, its work requirement is cancelled
    (best effort), aborting any running tasks.

    :return: the YellowDog ID of the added work requirement
    :rtype: str
    """

    operator_extra_links: Collection[BaseOperatorLink] = (WorkRequirementPortalLink(),)

    template_fields: Collection[str] = (
        *YellowDogOperator.template_fields,
        "work_requirement",
    )

    def __init__(
        self,
        *,
        work_requirement: Templated[WorkRequirement],
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.work_requirement = work_requirement

    def execute(self, context: Context) -> str:
        """
        Adds the work requirement.
        """

        wr = resolve(self.work_requirement, "work_requirement")
        stamp, ti, cycle = self._context_ti(context)
        with self.get_hook() as hook:
            result = hook.add_work_requirement(
                wr, stamp=stamp, try_number=ti.try_number, cycle=cycle
            )
            self._work_requirement_id = result.work_requirement_id
            self.log.info(
                "Work requirement '%s/%s' (ID '%s'): %s",
                wr.namespace,
                result.name,
                result.work_requirement_id,
                result.action.replace("_", " "),
            )
            return result.work_requirement_id


class AddTaskGroupsToWorkRequirement(YellowDogOperator):
    """
    Add task groups to a YellowDog work requirement. Either the work_requirement_id
    or both the namespace and work_requirement_name must be supplied, otherwise
    a :exc:`.YellowDogArgumentError` is raised. A field that renders to an empty string
    is treated as not supplied; one that renders to ``"None"`` raises a
    :exc:`.YellowDogArgumentError` (usually a missing upstream XCom value). All
    arguments are keyword-only.

    Retries: the groups are appended to the work requirement's current ones in
    a single update, so a retry after that update succeeded (but the task still
    failed) fails on the duplicate group names. The operator does not cancel
    anything if the task is killed.

    :param task_id: the Airflow task ID
    :type task_id: str
    :param connection_id: connection to run the operator with (templated) or a
        Callable that generates the connection ID; defaults to
        ``yellowdog_default``
    :type connection_id: str | Callable
    :param task_groups: a list of YellowDog TaskGroup objects or a
        Callable that generates a list of TaskGroup objects (templated)
    :type task_groups: list[TaskGroup] | Callable
    :param work_requirement_id: the ID of the work requirement (templated) or
        a Callable that generates the ID
    :type work_requirement_id: str | Callable | None
    :param namespace: the namespace of the work requirement (templated) or
        a Callable that generates the namespace
    :type namespace: str | Callable | None
    :param work_requirement_name: the name of a work requirement (templated) or a
        Callable that generates a work requirement name
    :type work_requirement_name: str | Callable | None

    :return: the YellowDog IDs of the added task groups, in the order of the
        supplied task_groups (pre-existing task groups are excluded)
    :rtype: list[str]
    """

    template_fields: Collection[str] = (
        *YellowDogOperator.template_fields,
        "work_requirement_id",
        "namespace",
        "work_requirement_name",
        "task_groups",
    )

    def __init__(
        self,
        *,
        task_groups: Templated[list[TaskGroup]],
        work_requirement_id: Templated[str] | None = None,
        namespace: Templated[str] | None = None,
        work_requirement_name: Templated[str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        require_id_or_name(
            "work requirement",
            "work_requirement_id",
            work_requirement_id,
            {"namespace": namespace, "work_requirement_name": work_requirement_name},
        )
        self.work_requirement_id = work_requirement_id
        self.namespace = namespace
        self.work_requirement_name = work_requirement_name
        self.task_groups = task_groups

    def execute(self, context: Context) -> list[str]:
        """
        Adds the task groups to the work requirement.
        """

        wr_id = resolve_optional_str(self.work_requirement_id, "work_requirement_id")
        namespace = resolve_optional_str(self.namespace, "namespace")
        wr_name = resolve_optional_str(
            self.work_requirement_name, "work_requirement_name"
        )
        task_groups = resolve(self.task_groups, "task_groups")

        with self.get_hook() as hook:
            client = hook.get_conn()
            work_requirement = get_work_requirement_by_id_or_name(
                client, self.log, wr_id, namespace, wr_name
            )

            existing = list(work_requirement.taskGroups or [])
            existing_ids = {tg.id for tg in existing}
            work_requirement.taskGroups = [*existing, *task_groups]

            self.log.info(
                f"Adding {len(task_groups)} task group(s) to work requirement"
            )
            updated = client.work_client.update_work_requirement(work_requirement)

            new_groups = [
                tg for tg in (updated.taskGroups or []) if tg.id not in existing_ids
            ]
            added_ids: list[str] = []
            for task_group in task_groups:
                match = next(
                    (tg for tg in new_groups if tg.name == task_group.name), None
                )
                if match is None:
                    continue
                new_groups.remove(match)
                self.log.info(f"Added task group '{match.name}' ID '{match.id}'")
                added_ids.append(cast(str, match.id))
            return added_ids


class AddTasksToTaskGroup(YellowDogOperator):
    """
    Add a list of tasks to a task group. Either the task_group_id,
    or all of: namespace, work_requirement_name and task_group_name,
    must be supplied, otherwise a :exc:`.YellowDogArgumentError` is raised. If both
    are supplied, the task_group_id is used. A field
    that renders to an empty string is treated as not supplied; one that
    renders to ``"None"`` raises a :exc:`.YellowDogArgumentError` (usually a missing
    upstream XCom value). All arguments are keyword-only.

    Retries: tasks are added without checking for earlier ones, so a retry
    after a partial failure may add some tasks twice; use
    :class:`AddPopulatedWorkRequirement` or :class:`RunWorkRequirement` for
    retry-safe submission. The operator does not cancel anything if the task
    is killed.

    :param task_id: the Airflow task ID
    :type task_id: str
    :param connection_id: connection to run the operator with (templated) or a
        Callable that generates the connection ID; defaults to
        ``yellowdog_default``
    :type connection_id: str | Callable
    :param tasks: a list of YellowDog Task objects or a
        Callable that generates a list of Task objects (templated)
    :type tasks: list[Task] | Callable
    :param task_group_id: the ID of the task_group (templated) or
        a Callable that generates the ID
    :type task_group_id: str | Callable | None
    :param namespace: the namespace of the work requirement (templated) or
        a Callable that generates the namespace
    :type namespace: str | Callable | None
    :param work_requirement_name: the name of a work requirement (templated) or a
        Callable that generates a work requirement name
    :type work_requirement_name: str | Callable | None
    :param task_group_name: the name of a task group (templated) or a
        Callable that generates a task group name
    :type task_group_name: str | Callable | None

    :return: the YellowDog IDs of the added tasks
    :rtype: list[str]
    """

    template_fields: Collection[str] = (
        *YellowDogOperator.template_fields,
        "namespace",
        "task_group_id",
        "work_requirement_name",
        "task_group_name",
        "tasks",
    )

    def __init__(
        self,
        *,
        tasks: Templated[list[Task]],
        task_group_id: Templated[str] | None = None,
        namespace: Templated[str] | None = None,
        work_requirement_name: Templated[str] | None = None,
        task_group_name: Templated[str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        require_id_or_name(
            "task group",
            "task_group_id",
            task_group_id,
            {
                "namespace": namespace,
                "work_requirement_name": work_requirement_name,
                "task_group_name": task_group_name,
            },
        )
        self.task_group_id = task_group_id
        self.namespace = namespace
        self.work_requirement_name = work_requirement_name
        self.task_group_name = task_group_name
        self.tasks = tasks

    def execute(self, context: Context) -> list[str]:
        """
        Adds the tasks to the task group.
        """

        task_group_id = resolve_optional_str(self.task_group_id, "task_group_id")
        namespace = resolve_optional_str(self.namespace, "namespace")
        wr_name = resolve_optional_str(
            self.work_requirement_name, "work_requirement_name"
        )
        task_group_name = resolve_optional_str(self.task_group_name, "task_group_name")
        tasks = resolve(self.tasks, "tasks")

        # Re-check after rendering (fields may have rendered to empty)
        require_id_or_name(
            "task group",
            "task_group_id",
            task_group_id,
            {
                "namespace": namespace,
                "work_requirement_name": wr_name,
                "task_group_name": task_group_name,
            },
        )

        with self.get_hook() as hook:
            client = hook.get_conn()
            if task_group_id is not None:
                self.log.info(
                    f"Adding {len(tasks)} task(s) to task group ID '{task_group_id}'"
                )
                added_tasks = client.work_client.add_tasks_to_task_group_by_id(
                    task_group_id=task_group_id,
                    tasks=tasks,
                )
            else:
                assert (
                    namespace is not None
                    and wr_name is not None
                    and task_group_name is not None
                )  # for type checkers
                self.log.info(
                    f"Adding {len(tasks)} task(s) to task group '{task_group_name}' "
                    f"in work requirement '{namespace}/{wr_name}'"
                )
                added_tasks = client.work_client.add_tasks_to_task_group_by_name(
                    namespace,
                    wr_name,
                    task_group_name,
                    tasks,
                )
            return [cast(str, task_.id) for task_ in added_tasks]


class AddPopulatedWorkRequirement(_WorkRequirementSubmitter):
    """
    Add a 'one-shot', populated YellowDog work requirement to the platform,
    including its task groups and tasks. The supplied work requirement must not
    already contain task groups (a :exc:`.YellowDogArgumentError` is raised if it does);
    supply them via task_groups_and_tasks instead. All arguments are
    keyword-only.

    Submission is retry-safe, with the same semantics as
    :class:`RunWorkRequirement` (see :meth:`.YellowDogHook.submit_work_requirement`):
    a retry resumes this task's earlier, fully built work requirement or
    submits a fresh copy named ``<name>-try<N>``. If submission fails
    partway, the partly built work requirement is cancelled, with abort. If the
    task is killed, its work requirement is cancelled (best effort), aborting
    any running tasks.

    :param task_id: the Airflow task ID
    :type task_id: str
    :param connection_id: connection to run the operator with (templated) or a
        Callable that generates the connection ID; defaults to
        ``yellowdog_default``
    :type connection_id: str | Callable
    :param work_requirement: a YellowDog WorkRequirement object or a
        Callable that generates a WorkRequirement object (templated)
    :type work_requirement: WorkRequirement | Callable
    :param task_groups_and_tasks: a list of (TaskGroup, list of Task) pairs,
        one per task group, or a Callable that generates the list (templated)
    :type task_groups_and_tasks: list[tuple[TaskGroup, list[Task]]] | Callable

    :return: the YellowDog ID of the added work requirement
    :rtype: str
    """

    operator_extra_links: Collection[BaseOperatorLink] = (WorkRequirementPortalLink(),)

    template_fields: Collection[str] = (
        *YellowDogOperator.template_fields,
        "work_requirement",
        "task_groups_and_tasks",
    )

    def __init__(
        self,
        *,
        work_requirement: Templated[WorkRequirement],
        task_groups_and_tasks: Templated[list[tuple[TaskGroup, list[Task]]]],
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.work_requirement = work_requirement
        self.task_groups_and_tasks = task_groups_and_tasks

    def execute(self, context: Context) -> str:
        """
        Adds the work requirement, task groups and tasks.
        """

        wr = resolve(self.work_requirement, "work_requirement")
        task_groups_and_tasks = resolve(
            self.task_groups_and_tasks, "task_groups_and_tasks"
        )

        if wr.taskGroups:
            raise YellowDogArgumentError(
                "The work requirement passed to AddPopulatedWorkRequirement must "
                "not already contain task groups; supply them via "
                "'task_groups_and_tasks'"
            )

        stamp, ti, cycle = self._context_ti(context)
        with self.get_hook() as hook:
            result = hook.submit_work_requirement(
                wr,
                task_groups_and_tasks,
                stamp=stamp,
                try_number=ti.try_number,
                cycle=cycle,
            )
            self._work_requirement_id = result.work_requirement_id
            self.log.info(
                "Work requirement '%s/%s' (ID '%s'): %s",
                wr.namespace,
                result.name,
                result.work_requirement_id,
                result.action.replace("_", " "),
            )
            return result.work_requirement_id


_WORK_REQUIREMENT = "work requirement"

DEFER_TIMEOUT_GRACE: float = 300
"""
Seconds that the deferrable operators (:class:`RunWorkRequirement`,
:class:`ProvisionWorkerPool`, :class:`ShutdownProvisionedWorkerPool`) add to
their remaining ``timeout`` for Airflow's own deferral timeout, so that the
trigger's timeout event (on which the operator cleans up, e.g. cancels its
work requirement or terminates its unready pool) arrives first. Airflow's
deferral timeout is only a backstop, e.g. if no triggerer is running; when it
fires, Airflow fails the task without calling the operator, so the operator
does not clean up.
"""


class RunWorkRequirement(_WorkRequirementSubmitter):
    """
    Run a populated YellowDog work requirement: submit it with its task groups
    and tasks, wait until it reaches a target state (by default COMPLETED),
    and fail if it reaches a failure state first. All arguments are
    keyword-only.

    **Submission is safe across retries.** The work requirement is stamped
    with this DAG run's and task's identity (in its ``tag``, replacing any tag
    supplied) and named as supplied or, if the name is empty, after the DAG,
    task and run. An earlier attempt of this task in this DAG run is resumed
    (waited on) if it is unfinished, and is not run again if it completed; a
    failed or cancelled attempt is replaced by a fresh copy named
    ``<name>-try<N>``. See
    :meth:`.YellowDogHook.submit_work_requirement` for the full rules. For a
    mapped task (``expand``), leave ``name`` unset: the derived names are
    unique per map index, whereas concurrent map indices with the same fixed
    name can all see it free, and all but one then fail on the duplicate.

    **Clearing re-runs the work.** A completed earlier attempt is submitted
    again as a fresh copy when the task instance has been *cleared* since it
    was submitted, but not on an automatic retry. Each submission records the
    task instance's *clear cycle* in its tag (``<stamp.tag()>:c<cycle>``), and the
    cycle is the task instance's Airflow ``max_tries``: Airflow raises
    ``max_tries`` (to ``<last try> + retries``) whenever it clears a task
    instance and leaves it unchanged on retries (verified for Airflow 3.1 and
    3.4). A completed attempt counts as done only if it was submitted in the
    current cycle, so work requested by a clear is redone even if the first
    try after the clear fails before submitting. An unfinished attempt is
    resumed whatever its cycle. Limitations: Airflow does not tell the task
    whether it had succeeded, so a clear re-runs a completed work requirement
    whatever the task instance's earlier outcome was; and on Airflow 3.1, a
    task instance cleared while running in non-deferrable mode can, in a
    narrow race, be recorded as failed without ``max_tries`` being raised, in
    which case its next try is treated as a retry (a completed attempt is not
    re-run). If the task instance does not expose a usable ``max_tries``,
    cycle 0 is used (a completed attempt from cycle 0 is then never re-run).

    **The work requirement ID** is pushed to XCom (key ``return_value``) as
    soon as it is known, before waiting, and is also the operator's return
    value.

    **Nothing is left running.** If the wait times out (``timeout``) or fails
    with an error, the work requirement is cancelled, aborting its running
    tasks, before the task fails; so is it if the task is killed (e.g. marked
    failed, or by ``execution_timeout``) while this operator runs on a
    worker. A failure state that the work requirement can leave (one that is
    not final, if supplied in ``failure_states``) also cancels it.
    Cancellation is best-effort: a failure to cancel is logged and never
    hides the original error. A task killed by a user (marked failed or
    succeeded, or cleared) while deferred is cleaned up by the trigger on
    Airflow 3.3 and later (verified on 3.4); **on Airflow 3.1 and 3.2,
    Airflow has no kill hook for deferred tasks, so it is not cleaned up by
    the provider**. An error while the work requirement is being submitted
    (after it is created, before all its tasks are added) cancels the partly
    built work requirement, aborting its tasks, and then fails the task; the
    only gap is a task killed in the moment between the work requirement's
    creation and the operator recording its ID (inside the submission),
    which is not cleaned up. For these cases, use a
    teardown such as :class:`ShutdownProvisionedWorkerPool` whose
    setup/teardown scope includes this task: it cancels the unfinished work
    requirements of the tasks in its scope (``cancel_work_requirements``).

    :param task_id: the Airflow task ID
    :type task_id: str
    :param connection_id: connection to run the operator with (templated) or a
        Callable that generates the connection ID; defaults to
        ``yellowdog_default``
    :type connection_id: str | Callable
    :param work_requirement: a YellowDog WorkRequirement object, without task
        groups, or a Callable that generates one (templated)
    :type work_requirement: WorkRequirement | Callable
    :param task_groups_and_tasks: a list of (TaskGroup, list of Task) pairs,
        one per task group, in order, or a Callable that generates the list
        (templated)
    :type task_groups_and_tasks: list[tuple[TaskGroup, list[Task]]] | Callable
    :param target_states: the (non-empty) WorkRequirementStatus states that
        end the wait successfully; defaults to COMPLETED
    :type target_states: Sequence[WorkRequirementStatus]
    :param failure_states: the WorkRequirementStatus states that fail the task
        at once with :exc:`.YellowDogStateError`. Defaults to the final states
        (COMPLETED, FAILED and CANCELLED) that are not in ``target_states``;
        an empty list disables failure detection
    :type failure_states: Sequence[WorkRequirementStatus] | None
    :param poke_interval: the time between status checks in seconds
        (default = 60)
    :type poke_interval: float
    :param timeout: the longest time to wait, in seconds, from the start of the
        wait (default = 7 days); on expiry the work requirement is cancelled
        and :exc:`.WaitTimeoutError` is raised (Airflow may retry the task,
        which then submits a fresh copy)
    :type timeout: float
    :param deferrable: if true, the operator checks the status once, then
        defers to the triggerer, which polls every ``poke_interval`` seconds
        without occupying a worker slot. Defaults to Airflow's ``[operators]
        default_deferrable`` setting (false unless configured), read when the
        operator is constructed
    :type deferrable: bool | None

    :return: the YellowDog ID of the work requirement
    :rtype: str
    """

    operator_extra_links: Collection[BaseOperatorLink] = (WorkRequirementPortalLink(),)

    template_fields: Collection[str] = (
        *YellowDogOperator.template_fields,
        "work_requirement",
        "task_groups_and_tasks",
    )

    def __init__(
        self,
        *,
        work_requirement: Templated[WorkRequirement],
        task_groups_and_tasks: Templated[list[tuple[TaskGroup, list[Task]]]],
        target_states: Sequence[WorkRequirementStatus] = (
            WorkRequirementStatus.COMPLETED,
        ),
        failure_states: Sequence[WorkRequirementStatus] | None = None,
        poke_interval: float = 60,  # Seconds
        timeout: float = 7 * 24 * 3600,  # Seconds
        deferrable: bool | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        targets, failures = resolve_states(
            target_states,
            failure_states,
            WORK_REQUIREMENT_FINAL_STATES,
            _WORK_REQUIREMENT,
        )
        self.work_requirement = work_requirement
        self.task_groups_and_tasks = task_groups_and_tasks
        self.target_states: list[WorkRequirementStatus] = list(targets)
        self.failure_states: list[WorkRequirementStatus] = list(failures)
        self.poke_interval: float = float(poke_interval)
        self.timeout: float = float(timeout)
        self.deferrable: bool = default_deferrable(deferrable)

    def execute(self, context: Context) -> str:
        """
        Submits (or resumes) the work requirement, pushes its ID to XCom, then
        waits for it, polling or deferring.
        """

        wr = resolve(self.work_requirement, "work_requirement")
        task_groups_and_tasks = resolve(
            self.task_groups_and_tasks, "task_groups_and_tasks"
        )
        stamp, ti, cycle = self._context_ti(context)

        with self.get_hook() as hook:
            result = hook.submit_work_requirement(
                wr,
                task_groups_and_tasks,
                stamp=stamp,
                try_number=ti.try_number,
                cycle=cycle,
            )
            wr_id = result.work_requirement_id
            self._work_requirement_id = wr_id
            self.log.info(
                "Work requirement '%s/%s' (ID '%s'): %s",
                wr.namespace,
                result.name,
                wr_id,
                result.action.replace("_", " "),
            )
            try:
                ti.xcom_push(key=XCOM_KEY, value=wr_id)
                if (
                    result.action == "already_completed"
                    and WorkRequirementStatus.COMPLETED in self.target_states
                ):
                    return wr_id
                if not self.deferrable:
                    wait_for_state(
                        lambda: hook.get_work_requirement_state(wr_id),
                        self.target_states,
                        self.failure_states,
                        poke_interval=self.poke_interval,
                        timeout=self.timeout,
                        describe=lambda state: self._state_error(
                            wr_id, state, wr.namespace, result.name
                        ),
                        log=self.log,
                        kind=_WORK_REQUIREMENT,
                        resource_id=wr_id,
                    )
                    return wr_id
                if self._reached_before_deferring(
                    hook.get_work_requirement_state(wr_id),
                    self.target_states,
                    self.failure_states,
                    lambda state: self._state_error(
                        wr_id, state, wr.namespace, result.name
                    ),
                ):
                    return wr_id
                trigger = YellowDogStateTrigger(
                    connection_id=resolve(self.connection_id, "connection_id"),
                    kind=ResourceKind.WORK_REQUIREMENT,
                    resource_id=wr_id,
                    target_states=self.target_states,
                    failure_states=self.failure_states,
                    poke_interval=self.poke_interval,
                    end_time=time.time() + self.timeout,
                    kill_action="cancel_work_requirement",
                )
            except YellowDogStateError as exc:
                if not cast(WorkRequirementStatus, exc.current_state).finished:
                    self._cancel(wr_id, "unfinished failure state")
                raise
            except Exception:
                self._cancel(wr_id, "error while waiting")
                raise

        self.defer(
            trigger=trigger,
            method_name="execute_complete",
            kwargs={
                "work_requirement_id": wr_id,
                "namespace": wr.namespace,
                "name": result.name,
            },
            timeout=timedelta(seconds=self.timeout + DEFER_TIMEOUT_GRACE),
        )

    def execute_complete(
        self,
        context: Context,
        event: dict[str, Any],
        work_requirement_id: str | None = None,
        namespace: str | None = None,
        name: str | None = None,
    ) -> str:
        """
        Resume after the trigger fires. Returns the work requirement ID if a
        target state was reached; raises :exc:`.YellowDogStateError` for a
        failure state, and otherwise cancels the work requirement (aborting
        running tasks) and raises :exc:`.WaitTimeoutError` on timeout or
        :exc:`.YellowDogException` for an error in the triggerer.

        :param event: the :class:`.YellowDogStateTrigger` event
        :param work_requirement_id: the work requirement's ID (set by
            ``execute``); defaults to the event's resource ID
        :param namespace: the work requirement's namespace (set by ``execute``)
        :param name: the work requirement's name (set by ``execute``)
        """
        wr_id = work_requirement_id or str(event.get("resource_id", ""))
        self._work_requirement_id = wr_id
        status = event.get("status")
        if status == "target":
            self.log.info(str(event.get("message", "")))
            return wr_id
        _, state = _event_state(event)
        if status == "failure" and isinstance(state, WorkRequirementStatus):
            if not state.finished:
                self._cancel(wr_id, "unfinished failure state")
            raise self._state_error(wr_id, state, namespace, name)
        self._cancel(wr_id, f"trigger event '{status}'")
        raise _event_error(event, self.timeout)

    def _state_error(
        self,
        wr_id: str,
        state: WorkRequirementStatus,
        namespace: str | None,
        name: str | None,
    ) -> YellowDogStateError:
        return YellowDogStateError(
            kind=_WORK_REQUIREMENT,
            resource_id=wr_id,
            current_state=state,
            target_states=self.target_states,
            namespace=namespace,
            name=name,
        )


class CancelWorkRequirement(YellowDogOperator):
    """
    Cancel a YellowDog work requirement. Either the work_requirement_id or both
    namespace and work_requirement_name must be supplied, otherwise a
    :exc:`.YellowDogArgumentError` is raised. A field that renders to an empty string
    is treated as not supplied; one that renders to ``"None"`` raises a
    :exc:`.YellowDogArgumentError` (usually a missing upstream XCom value). All
    arguments are keyword-only.

    If the platform rejects the cancellation as an invalid state transition
    (e.g., the work requirement has already finished), a warning is logged
    and the operator succeeds.

    :param task_id: the Airflow task ID
    :type task_id: str
    :param connection_id: connection to run the operator with (templated) or a
        Callable that generates the connection ID; defaults to
        ``yellowdog_default``
    :type connection_id: str | Callable
    :param work_requirement_id: the ID of the work requirement (templated) or
        a Callable that generates the ID
    :type work_requirement_id: str | Callable | None
    :param namespace: the namespace of the work requirement (templated) or
        a Callable that generates the namespace
    :type namespace: str | Callable | None
    :param work_requirement_name: the name of a work requirement (templated) or a
        Callable that generates a work requirement name
    :type work_requirement_name: str | Callable | None
    :param abort_running_tasks: abort currently running tasks rather than
        letting them finish; defaults to ``False``
    :type abort_running_tasks: bool

    :return: the YellowDog ID of the cancelled work requirement
    :rtype: str
    """

    template_fields: Collection[str] = (
        *YellowDogOperator.template_fields,
        "work_requirement_id",
        "namespace",
        "work_requirement_name",
    )

    def __init__(
        self,
        *,
        work_requirement_id: Templated[str] | None = None,
        namespace: Templated[str] | None = None,
        work_requirement_name: Templated[str] | None = None,
        abort_running_tasks: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        require_id_or_name(
            "work requirement",
            "work_requirement_id",
            work_requirement_id,
            {"namespace": namespace, "work_requirement_name": work_requirement_name},
        )
        self.work_requirement_id = work_requirement_id
        self.namespace = namespace
        self.work_requirement_name = work_requirement_name
        self.abort_running_tasks = abort_running_tasks

    def execute(self, context: Context) -> str:
        """
        Cancels the work requirement, optionally aborting running tasks.
        """

        wr_id = resolve_optional_str(self.work_requirement_id, "work_requirement_id")
        namespace = resolve_optional_str(self.namespace, "namespace")
        wr_name = resolve_optional_str(
            self.work_requirement_name, "work_requirement_name"
        )

        with self.get_hook() as hook:
            client = hook.get_conn()
            work_requirement = get_work_requirement_by_id_or_name(
                client, self.log, wr_id, namespace, wr_name
            )

            hook.cancel_work_requirement(
                cast(str, work_requirement.id), abort=self.abort_running_tasks
            )
            return cast(str, work_requirement.id)


_WORKER_POOL = "worker pool"
_COMPUTE_REQUIREMENT = "compute requirement"

POOL_READY_STATES: tuple[WorkerPoolStatus, ...] = (
    WorkerPoolStatus.IDLE,
    WorkerPoolStatus.RUNNING,
)
"""
The worker pool states in which :class:`ProvisionWorkerPool` considers a pool
ready (its workers have registered). EMPTY, PENDING and CONFIGURING are not
ready.
"""

_EVENT_KINDS: dict[str, tuple[str, type[Enum]]] = {
    ResourceKind.WORK_REQUIREMENT.value: (_WORK_REQUIREMENT, WorkRequirementStatus),
    ResourceKind.WORKER_POOL.value: (_WORKER_POOL, WorkerPoolStatus),
    ResourceKind.COMPUTE_REQUIREMENT.value: (
        _COMPUTE_REQUIREMENT,
        ComputeRequirementStatus,
    ),
}


def _push_id(context: Context, resource_id: str) -> None:
    """Push a resource ID to XCom (key ``return_value``) before waiting."""
    if "ti" not in context:
        raise YellowDogException("Task context has no task instance ('ti')")
    context["ti"].xcom_push(key=XCOM_KEY, value=resource_id)


def _event_state(event: dict[str, Any]) -> tuple[str, Enum | None]:
    """
    The resource kind label and the state of a trigger event (``None`` if
    absent or not a known status).
    """
    label, status_type = _EVENT_KINDS.get(
        str(event.get("kind")), (_WORKER_POOL, WorkerPoolStatus)
    )
    raw = event.get("state")
    try:
        return label, None if raw is None else status_type(raw)
    except ValueError:
        return label, None


def _event_error(event: dict[str, Any], timeout: float) -> YellowDogException:
    """
    The error for a trigger event that is neither a target nor a failure
    event: :exc:`.WaitTimeoutError` for a timeout with a known state,
    otherwise :exc:`.YellowDogException`.
    """
    status = event.get("status")
    label, state = _event_state(event)
    if status == "timeout" and state is not None:
        return WaitTimeoutError(
            label, str(event.get("resource_id", "")), state, timeout
        )
    if status == "error":
        return YellowDogException(event_error_message(event))
    return YellowDogException(f"Unexpected trigger event: {event}")


class ProvisionWorkerPool(YellowDogOperator):
    """
    Provision a YellowDog worker pool and, optionally, wait until it is
    ready. All arguments are keyword-only.

    With ``wait_until_ready``, the worker pool ID is pushed to XCom (key
    ``return_value``) as soon as the pool is provisioned, then the operator
    waits until the pool is IDLE or RUNNING (its workers have registered).
    EMPTY, PENDING and CONFIGURING are not ready, so a pool whose instances
    never come up fails at the ``timeout``; a pool that reaches SHUTDOWN or
    TERMINATED first fails the task with :exc:`.YellowDogStateError`.

    **The pool cleans up after itself.** If the wait fails (failure state,
    ``timeout``, or an error), or the task is killed (e.g. marked failed, or
    by ``execution_timeout``) while this operator waits on a worker, the pool
    is shut down and its compute requirement terminated before the task
    fails, so a retry never orphans a pool. Cleanup is best-effort: a failure
    to clean up is logged and never hides the original error. Once the pool
    is ready (or if not waiting), killing the task leaves the pool alone: use
    :class:`ShutdownProvisionedWorkerPool` as the matching teardown.

    A task killed by a user (marked failed or succeeded, or cleared) while
    deferred is cleaned up by the trigger on Airflow 3.3 and later
    (verified on 3.4). **On Airflow 3.1 and 3.2, Airflow has no kill hook for
    deferred tasks, so such a pool is not cleaned up by the provider**, and
    the teardown does not run either (Airflow skips a teardown whose setup
    failed). On those versions,
    give the pool an idle shutdown (``ProvisionedWorkerPoolProperties``) as
    a backstop, or use ``deferrable=False`` for this task. Likewise, if the
    worker running this task crashes while waiting, nothing cleans up the
    pool, and a retry provisions a second one; the pool's idle shutdown is
    the backstop.

    :param task_id: the Airflow task ID
    :type task_id: str
    :param connection_id: connection to run the operator with (templated) or a
        Callable that generates the connection ID; defaults to
        ``yellowdog_default``
    :type connection_id: str | Callable
    :param compute_requirement_template_usage: a ComputeRequirementTemplateUsage
        object or a Callable that generates the object (templated)
    :type compute_requirement_template_usage: ComputeRequirementTemplateUsage | Callable
    :param provisioned_worker_pool_properties: a ProvisionedWorkerPoolProperties object
        or a Callable that generates the object (templated); optional
    :type provisioned_worker_pool_properties: ProvisionedWorkerPoolProperties |
        Callable | None
    :param wait_until_ready: wait until the pool is IDLE or RUNNING (default
        ``False``: return as soon as the pool is provisioned)
    :type wait_until_ready: bool
    :param poke_interval: the time between status checks in seconds
        (default = 30)
    :type poke_interval: float
    :param timeout: the longest time to wait for the pool to be ready, in
        seconds (default = 3600); on expiry the pool is terminated and
        :exc:`.WaitTimeoutError` is raised
    :type timeout: float
    :param deferrable: if true, the operator checks the status once, then
        defers to the triggerer, which polls every ``poke_interval`` seconds
        without occupying a worker slot. Defaults to Airflow's ``[operators]
        default_deferrable`` setting (false unless configured), read when the
        operator is constructed. Only used with ``wait_until_ready``
    :type deferrable: bool | None

    :return: the YellowDog ID of the provisioned worker pool
    :rtype: str
    """

    operator_extra_links: Collection[BaseOperatorLink] = (WorkerPoolPortalLink(),)

    template_fields: Collection[str] = (
        *YellowDogOperator.template_fields,
        "compute_requirement_template_usage",
        "provisioned_worker_pool_properties",
    )

    def __init__(
        self,
        *,
        compute_requirement_template_usage: Templated[ComputeRequirementTemplateUsage],
        provisioned_worker_pool_properties: (
            Templated[ProvisionedWorkerPoolProperties] | None
        ) = None,
        wait_until_ready: bool = False,
        poke_interval: float = 30,  # Seconds
        timeout: float = 3600,  # Seconds
        deferrable: bool | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.compute_requirement_template_usage = compute_requirement_template_usage
        self.provisioned_worker_pool_properties = provisioned_worker_pool_properties
        self.wait_until_ready: bool = wait_until_ready
        self.poke_interval: float = float(poke_interval)
        self.timeout: float = float(timeout)
        self.deferrable: bool = default_deferrable(deferrable)
        self._worker_pool_id: str | None = None
        self._ready: bool = False  # The pool is handed over: on_kill leaves it

    def execute(self, context: Context) -> str:
        """
        Provisions the worker pool and, with ``wait_until_ready``, pushes its
        ID to XCom and waits until it is ready, polling or deferring.
        """

        crtu = resolve(
            self.compute_requirement_template_usage,
            "compute_requirement_template_usage",
        )
        pwpp = (
            None
            if self.provisioned_worker_pool_properties is None
            else resolve(
                self.provisioned_worker_pool_properties,
                "provisioned_worker_pool_properties",
            )
        )

        with self.get_hook() as hook:
            pool_id = hook.provision_worker_pool(crtu, pwpp)
            self._worker_pool_id = pool_id
            if not self.wait_until_ready:
                self._ready = True
                return pool_id
            try:
                _push_id(context, pool_id)
                if not self.deferrable:
                    wait_for_state(
                        lambda: hook.get_worker_pool_state(pool_id),
                        POOL_READY_STATES,
                        WORKER_POOL_FINAL_STATES,
                        poke_interval=self.poke_interval,
                        timeout=self.timeout,
                        describe=lambda state: self._state_error(pool_id, state),
                        log=self.log,
                        kind=_WORKER_POOL,
                        resource_id=pool_id,
                    )
                    self._ready = True
                    return pool_id
                if self._reached_before_deferring(
                    hook.get_worker_pool_state(pool_id),
                    POOL_READY_STATES,
                    WORKER_POOL_FINAL_STATES,
                    lambda state: self._state_error(pool_id, state),
                ):
                    self._ready = True
                    return pool_id
                trigger = YellowDogStateTrigger(
                    connection_id=resolve(self.connection_id, "connection_id"),
                    kind=ResourceKind.WORKER_POOL,
                    resource_id=pool_id,
                    target_states=POOL_READY_STATES,
                    failure_states=WORKER_POOL_FINAL_STATES,
                    poke_interval=self.poke_interval,
                    end_time=time.time() + self.timeout,
                    kill_action="terminate_worker_pool",
                )
            except Exception:
                self._terminate(pool_id, "not ready")
                raise

        self.defer(
            trigger=trigger,
            method_name="execute_complete",
            kwargs={"worker_pool_id": pool_id},
            timeout=timedelta(seconds=self.timeout + DEFER_TIMEOUT_GRACE),
        )

    def execute_complete(
        self,
        context: Context,
        event: dict[str, Any],
        worker_pool_id: str | None = None,
    ) -> str:
        """
        Resume after the trigger fires. Returns the worker pool ID if the pool
        is ready; otherwise terminates the pool (best-effort) and raises
        :exc:`.YellowDogStateError` for a failure state,
        :exc:`.WaitTimeoutError` on timeout or :exc:`.YellowDogException` for
        an error in the triggerer.

        :param event: the :class:`.YellowDogStateTrigger` event
        :param worker_pool_id: the worker pool's ID (set by ``execute``);
            defaults to the event's resource ID
        """
        pool_id = worker_pool_id or str(event.get("resource_id", ""))
        self._worker_pool_id = pool_id
        status = event.get("status")
        if status == "target":
            self.log.info(str(event.get("message", "")))
            self._ready = True
            return pool_id
        self._terminate(pool_id, f"trigger event '{status}'")
        _, state = _event_state(event)
        if status == "failure" and isinstance(state, WorkerPoolStatus):
            raise self._state_error(pool_id, state)
        raise _event_error(event, self.timeout)

    def on_kill(self) -> None:
        """
        Terminates the worker pool if it was provisioned but is not yet ready
        (best-effort: errors are logged, not raised). A ready pool, or one
        provisioned without ``wait_until_ready``, is left alone.
        """
        pool_id = self._worker_pool_id
        if pool_id is None or self._ready:
            self.log.info("No unready worker pool to terminate")
            return
        self._terminate(pool_id, "task killed")

    def _terminate(self, pool_id: str, reason: str) -> None:
        """Best-effort shutdown and terminate: log failures, never raise."""
        self.log.warning(
            "Shutting down worker pool ID '%s' and terminating its compute "
            "requirement (%s)",
            pool_id,
            reason,
        )
        try:
            with self.get_hook() as hook:
                hook.shutdown_worker_pool(pool_id, terminate_immediately=True)
        except Exception:
            self.log.exception("Failed to terminate worker pool ID '%s'", pool_id)

    def _state_error(
        self, pool_id: str, state: WorkerPoolStatus
    ) -> YellowDogStateError:
        return YellowDogStateError(
            kind=_WORKER_POOL,
            resource_id=pool_id,
            current_state=state,
            target_states=POOL_READY_STATES,
        )


CancelWorkRequirements = Literal["scope", "run", "none"]
"""The ``cancel_work_requirements`` values of :class:`ShutdownProvisionedWorkerPool`."""

_CANCEL_WORK_REQUIREMENTS: tuple[str, ...] = get_args(CancelWorkRequirements)


class ShutdownProvisionedWorkerPool(YellowDogOperator):
    """
    Shuts down a YellowDog provisioned worker pool, and optionally
    immediately terminates its associated compute requirement. Either the
    worker_pool_id or both the namespace and worker_pool_name must be
    supplied, otherwise a :exc:`.YellowDogArgumentError` is raised. A field that
    renders to an empty string is treated as not supplied; one that renders
    to ``"None"`` raises a :exc:`.YellowDogArgumentError` (usually a missing upstream
    XCom value). A configured (or otherwise non-provisioned) worker pool
    raises a
    :exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogException`
    (and nothing is cancelled). A pool that is already shut down (the
    platform rejects the shutdown as an "invalid transition") is tolerated:
    the operator still terminates the compute requirement (with
    ``terminate_immediately``), waits (with ``wait``) and succeeds; so is a
    compute requirement that is already terminating or terminated (e.g. when
    the teardown is retried). If the
    shutdown fails for another reason, the compute requirement is still
    terminated (with ``terminate_immediately``) and the task then fails.
    All arguments are keyword-only.

    **Scoped cancellation.** Before shutting the pool down, the operator
    cancels, aborting running tasks, unfinished work requirements, in any
    namespace visible to the connection's credentials, that were submitted
    by tasks of *this DAG run* (identified by the run stamp in their
    ``tag``; see :meth:`~yellowdog_provider.utils.run_stamp.RunStamp.tag`).
    ``cancel_work_requirements`` chooses which: with ``"scope"`` (the
    default), those of the tasks in this teardown's setup/teardown scope
    (its setups, and the tasks downstream of a setup and upstream of the
    teardown; every map index of a mapped task, except that in a mapped
    task group each teardown instance covers only its own map instance of
    the tasks in that group); with ``"run"``, those of
    every task of the DAG run; with ``"none"``, none. Under ``"scope"``, an
    operator that is not a teardown (``as_teardown``) or has no setups
    cancels nothing and logs a warning. Work requirements of other DAG runs,
    or not submitted by this provider, are never touched. If several
    Airflow deployments share the same YellowDog credentials, give each a
    distinct ``[yellowdog] deployment_id`` setting, so that runs with the
    same DAG and run IDs in different deployments are told apart. Used as
    the teardown of the work, this cleans up work left running by killed
    deferred tasks or crashed workers. A task outside every scope
    (for example a :class:`RunWorkRequirement` on an existing pool) still
    cancels its own work on failure, timeout or kill, but its work left
    running after a worker crash is only cleaned up with ``"run"``.
    If the lookup or a cancellation fails, the failure is logged and the
    shutdown goes ahead (including ``terminate_immediately``), then the task
    fails with a :exc:`.YellowDogException` instead of waiting, as work
    requirements may still be running; a retry cancels and shuts down again.

    **Waiting.** With ``wait``, the operator pushes the pool ID to XCom (key
    ``return_value``), waits until the pool is SHUTDOWN or TERMINATED and,
    with ``terminate_immediately``, then until its compute requirement is
    TERMINATED. ``timeout`` covers both waits together. When deferrable, each
    wait is a separate deferral (the compute requirement's deferral starts
    after the pool's ends) with the same overall deadline.

    :param task_id: the Airflow task ID
    :type task_id: str
    :param connection_id: connection to run the operator with (templated) or a
        Callable that generates the connection ID; defaults to
        ``yellowdog_default``
    :type connection_id: str | Callable
    :param worker_pool_id: the ID of the worker pool (templated) or
        a Callable that generates the ID
    :type worker_pool_id: str | Callable | None
    :param namespace: the namespace of the worker pool (templated) or a
        Callable that generates the namespace
    :type namespace: str | Callable | None
    :param worker_pool_name: the name of the worker pool (templated) or a
        Callable that generates a worker pool name
    :type worker_pool_name: str | Callable | None
    :param terminate_immediately: immediately terminate the associated
        compute requirement, failing any running tasks; defaults to ``False``
    :type terminate_immediately: bool
    :param wait: wait until the pool (and, with ``terminate_immediately``,
        its compute requirement) has finished shutting down (default
        ``False``)
    :type wait: bool
    :param poke_interval: the time between status checks in seconds
        (default = 30)
    :type poke_interval: float
    :param timeout: the longest time to wait, in seconds, for both waits
        together (default = 3600); on expiry :exc:`.WaitTimeoutError` is
        raised
    :type timeout: float
    :param deferrable: if true, each wait checks the status once, then defers
        to the triggerer. Defaults to Airflow's ``[operators]
        default_deferrable`` setting (false unless configured), read when the
        operator is constructed. Only used with ``wait``
    :type deferrable: bool | None
    :param cancel_work_requirements: which of this DAG run's unfinished
        work requirements to cancel first, in any namespace: ``"scope"``
        (the default) for those of the tasks in this teardown's
        setup/teardown scope, ``"run"`` for the whole DAG run, or ``"none"``.
        Any other value raises :exc:`.YellowDogArgumentError` (at DAG parse
        time)
    :type cancel_work_requirements: Literal["scope", "run", "none"]

    :return: the YellowDog ID of the worker pool that was shut down
    :rtype: str
    """

    operator_extra_links: Collection[BaseOperatorLink] = (WorkerPoolPortalLink(),)

    template_fields: Collection[str] = (
        *YellowDogOperator.template_fields,
        "worker_pool_id",
        "namespace",
        "worker_pool_name",
    )

    def __init__(
        self,
        *,
        worker_pool_id: Templated[str] | None = None,
        namespace: Templated[str] | None = None,
        worker_pool_name: Templated[str] | None = None,
        terminate_immediately: bool = False,
        wait: bool = False,
        poke_interval: float = 30,  # Seconds
        timeout: float = 3600,  # Seconds
        deferrable: bool | None = None,
        cancel_work_requirements: CancelWorkRequirements = "scope",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        require_id_or_name(
            "worker pool",
            "worker_pool_id",
            worker_pool_id,
            {"namespace": namespace, "worker_pool_name": worker_pool_name},
        )
        if cancel_work_requirements not in _CANCEL_WORK_REQUIREMENTS:
            raise YellowDogArgumentError(
                f"cancel_work_requirements must be one of "
                f"{', '.join(repr(v) for v in _CANCEL_WORK_REQUIREMENTS)}, "
                f"not {cancel_work_requirements!r}"
            )
        self.worker_pool_id = worker_pool_id
        self.namespace = namespace
        self.worker_pool_name = worker_pool_name
        self.terminate_immediately = terminate_immediately
        self.wait: bool = wait
        self.poke_interval: float = float(poke_interval)
        self.timeout: float = float(timeout)
        self.deferrable: bool = default_deferrable(deferrable)
        self.cancel_work_requirements: CancelWorkRequirements = cancel_work_requirements

    def execute(self, context: Context) -> str:
        """
        Cancels unfinished work requirements of this DAG run (see
        ``cancel_work_requirements``),
        shuts down the worker pool, optionally terminates its compute
        requirement, and optionally waits for both.
        """

        wp_id = resolve_optional_str(self.worker_pool_id, "worker_pool_id")
        namespace = resolve_optional_str(self.namespace, "namespace")
        wp_name = resolve_optional_str(self.worker_pool_name, "worker_pool_name")

        with self.get_hook() as hook:
            client = hook.get_conn()
            worker_pool = get_worker_pool_by_id_or_name(
                client, self.log, wp_id, namespace, wp_name
            )

            if isinstance(worker_pool, ConfiguredWorkerPool):
                raise YellowDogException(
                    f"Worker pool '{worker_pool.namespace}/{worker_pool.name}' "
                    f"({worker_pool.id}) is a configured worker pool"
                )
            if not isinstance(worker_pool, ProvisionedWorkerPool):
                raise YellowDogException(
                    f"Worker pool '{worker_pool.namespace}/{worker_pool.name}' "
                    f"({worker_pool.id}) is of unsupported type "
                    f"'{type(worker_pool).__name__}'; only provisioned worker pools "
                    "can be shut down"
                )

            pool_id = cast(str, worker_pool.id)
            cancel_error = (
                None
                if self.cancel_work_requirements == "none"
                else self._cancel_work_requirements(hook, context)
            )

            cr_id = hook.shutdown_worker_pool(
                pool_id, terminate_immediately=self.terminate_immediately
            )
            if cancel_error is not None:
                raise cancel_error

            if not self.wait:
                return pool_id

            _push_id(context, pool_id)
            if self.deferrable:
                return self._await_deferred(
                    hook, pool_id, cr_id, time.time() + self.timeout
                )
            deadline = time.monotonic() + self.timeout
            self._wait_sync(
                lambda: hook.get_worker_pool_state(pool_id),
                cast(Sequence[Enum], WORKER_POOL_FINAL_STATES),
                self.timeout,
                _WORKER_POOL,
                pool_id,
            )
            if cr_id is not None:
                try:
                    self._wait_sync(
                        lambda: hook.get_compute_requirement_state(cr_id),
                        cast(Sequence[Enum], COMPUTE_REQUIREMENT_FINAL_STATES),
                        max(0.0, deadline - time.monotonic()),
                        _COMPUTE_REQUIREMENT,
                        cr_id,
                    )
                except WaitTimeoutError as exc:  # Report the overall timeout
                    raise WaitTimeoutError(
                        exc.kind, exc.resource_id, exc.last_state, self.timeout
                    ) from exc
            return pool_id

    def execute_complete(
        self,
        context: Context,
        event: dict[str, Any],
        worker_pool_id: str | None = None,
        compute_requirement_id: str | None = None,
        end_time: float | None = None,
    ) -> str:
        """
        Resume after a trigger fires. After the worker pool's wait, if there
        is a compute requirement to wait for, checks it and (unless already
        TERMINATED) defers again; otherwise returns the worker pool ID.
        Raises :exc:`.WaitTimeoutError` on timeout or
        :exc:`.YellowDogException` for an error in the triggerer.

        :param event: the :class:`.YellowDogStateTrigger` event
        :param worker_pool_id: the worker pool's ID (set by ``execute``)
        :param compute_requirement_id: the compute requirement still to wait
            for, if any (set by ``execute``)
        :param end_time: the overall deadline, in seconds since the epoch
            (set by ``execute``)
        """
        pool_id = worker_pool_id or str(event.get("resource_id", ""))
        if event.get("status") != "target":
            raise _event_error(event, self.timeout)
        self.log.info(str(event.get("message", "")))
        if compute_requirement_id is None:
            return pool_id
        deadline = time.time() + self.timeout if end_time is None else end_time
        with self.get_hook() as hook:
            return self._await_deferred(
                hook, pool_id, compute_requirement_id, deadline, pool_done=True
            )

    def _teardown_scope_task_ids(self) -> list[str] | None:
        """
        The (sorted) task IDs in this teardown's setup/teardown scope: its
        setups (direct upstream tasks that are setups) and every task both
        downstream of one of them and upstream of this task, not including
        this task. ``None`` if this task is not a teardown or has no setups.
        """
        if not self.is_teardown:
            return None
        relatives = self.get_flat_relatives(upstream=True)
        setups = [
            task
            for task in relatives
            if task.is_setup and task.task_id in self.upstream_task_ids
        ]
        if not setups:
            return None
        upstream = {task.task_id for task in relatives}
        # Nested setups and teardowns fall in the scope too; harmless, as they
        # submit no work requirements.
        scope = {setup.task_id for setup in setups}
        for setup in setups:
            scope.update(
                task.task_id
                for task in setup.get_flat_relatives(upstream=False)
                if task.task_id in upstream
            )
        scope.discard(self.task_id)
        return sorted(scope)

    def _scope_tag_prefixes(self, stamp: RunStamp, task_ids: list[str]) -> list[str]:
        """
        The tag prefixes of the scope tasks ``task_ids``: each task's
        :func:`.task_tag_prefix_for` (every map index), except that, when this
        teardown is in a mapped task group, the tasks in that same group get
        ``<task prefix>:m<map index>`` for this teardown's own map index, as
        Airflow runs each instance of the teardown once its own group
        instance's tasks are done, while the others may still be running.
        Tasks in a mapped group nested in that group keep the plain prefix.
        """
        group = self.get_closest_mapped_task_group()
        # Only tasks whose closest mapped group is this one share its map
        # index; a task in a nested mapped group has its own (flat) map
        # indices, so it keeps the plain prefix (over-cancels, never misses).
        in_group = (
            set[str]()
            if group is None
            else {
                task.task_id
                for task in group.iter_tasks()
                if (closest := task.get_closest_mapped_task_group()) is not None
                and closest.group_id == group.group_id
            }
        )
        return [
            task_tag_prefix_for(stamp, task_id)
            + (f":m{stamp.map_index}" if task_id in in_group else "")
            for task_id in task_ids
        ]

    def _cancel_work_requirements(
        self, hook: YellowDogHook, context: Context
    ) -> YellowDogException | None:
        """
        Cancel (with abort) this DAG run's unfinished work requirements in
        every visible namespace, those of the tasks in this teardown's scope
        or of the whole run (``cancel_work_requirements``). Failures are
        logged and not raised, so that the pool is still shut down; the error
        to raise afterwards (chained to the first failure) is returned, or
        ``None`` if there was none.
        """
        mode = self.cancel_work_requirements
        try:
            stamp = RunStamp.from_context(context)
            task_prefixes: list[str] | None = None
            if mode == "scope":
                task_ids = self._teardown_scope_task_ids()
                if task_ids is None:
                    self.log.warning(
                        "Task '%s' is not a teardown with setups, so it has no "
                        "setup/teardown scope; no work requirements cancelled. "
                        "Use cancel_work_requirements='run' to cancel the whole "
                        "DAG run's, or make it a teardown "
                        "(as_teardown(setups=...))",
                        self.task_id,
                    )
                    return None
                self.log.info(
                    "Setup/teardown scope: %s", ", ".join(repr(t) for t in task_ids)
                )
                task_prefixes = self._scope_tag_prefixes(stamp, task_ids)
            wr_ids = hook.find_unfinished_run_work_requirements(
                None, stamp.run_tag_prefix(), task_prefixes
            )
        except Exception as exc:
            self.log.exception(
                "Could not look up this DAG run's unfinished work requirements; "
                "none cancelled"
            )
            error = YellowDogException(
                "Could not look up this DAG run's unfinished work requirements, "
                "so none were cancelled and some may still be running; the "
                "worker pool was still shut down"
            )
            error.__cause__ = exc
            return error
        where = "this DAG run" if mode == "run" else "this teardown's scope"
        if not wr_ids:
            self.log.info(
                "No unfinished work requirements of %s (cancel_work_requirements='%s')",
                where,
                mode,
            )
            return None
        self.log.warning(
            "Cancelling %d unfinished work requirement(s) of %s "
            "(cancel_work_requirements='%s'), aborting running tasks",
            len(wr_ids),
            where,
            mode,
        )
        failed: list[str] = []
        first_failure: Exception | None = None
        for wr_id in wr_ids:
            try:
                hook.cancel_work_requirement(wr_id, abort=True)
            except Exception as exc:
                self.log.exception("Failed to cancel work requirement ID '%s'", wr_id)
                failed.append(wr_id)
                first_failure = first_failure or exc
        if not failed:
            return None
        error = YellowDogException(
            f"Failed to cancel {len(failed)} of {len(wr_ids)} unfinished work "
            f"requirement(s) of {where} (IDs: {', '.join(failed)}); they may "
            "still be running. The worker pool was still shut down"
        )
        error.__cause__ = first_failure
        return error

    def _wait_sync(
        self,
        get_state: Callable[[], Enum],
        targets: Sequence[Enum],
        timeout: float,
        kind: str,
        resource_id: str,
    ) -> None:
        """Poll until a target state (no failure states) or the timeout."""
        wait_for_state(
            get_state,
            targets,
            (),
            poke_interval=self.poke_interval,
            timeout=timeout,
            describe=lambda state: YellowDogStateError(  # Unused: no failures
                kind=kind,
                resource_id=resource_id,
                current_state=state,
                target_states=targets,
            ),
            log=self.log,
            kind=kind,
            resource_id=resource_id,
        )

    def _await_deferred(
        self,
        hook: YellowDogHook,
        pool_id: str,
        cr_id: str | None,
        end_time: float,
        *,
        pool_done: bool = False,
    ) -> str:
        """
        Check the worker pool (unless ``pool_done``) and then the compute
        requirement (if any) once each, deferring on the first that has not
        yet finished; return the worker pool ID if both have.
        """
        if not pool_done:
            pool_state = hook.get_worker_pool_state(pool_id)
            if pool_state not in WORKER_POOL_FINAL_STATES:
                self._defer_wait(
                    ResourceKind.WORKER_POOL,
                    pool_id,
                    WORKER_POOL_FINAL_STATES,
                    end_time,
                    {"worker_pool_id": pool_id, "compute_requirement_id": cr_id},
                )
            self.log.info("Worker pool reached status '%s'", pool_state.value)
        if cr_id is not None:
            cr_state = hook.get_compute_requirement_state(cr_id)
            if cr_state not in COMPUTE_REQUIREMENT_FINAL_STATES:
                self._defer_wait(
                    ResourceKind.COMPUTE_REQUIREMENT,
                    cr_id,
                    COMPUTE_REQUIREMENT_FINAL_STATES,
                    end_time,
                    {"worker_pool_id": pool_id, "compute_requirement_id": None},
                )
            self.log.info("Compute requirement reached status '%s'", cr_state.value)
        return pool_id

    def _defer_wait(
        self,
        kind: ResourceKind,
        resource_id: str,
        targets: Sequence[Enum],
        end_time: float,
        kwargs: dict[str, Any],
    ) -> NoReturn:
        trigger = YellowDogStateTrigger(
            connection_id=resolve(self.connection_id, "connection_id"),
            kind=kind,
            resource_id=resource_id,
            target_states=targets,
            failure_states=[],
            poke_interval=self.poke_interval,
            end_time=end_time,
        )
        self.defer(
            trigger=trigger,
            method_name="execute_complete",
            kwargs={**kwargs, "end_time": end_time},
            timeout=timedelta(
                seconds=max(0.0, end_time - time.time()) + DEFER_TIMEOUT_GRACE
            ),
        )
