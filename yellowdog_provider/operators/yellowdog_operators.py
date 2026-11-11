"""
YellowDog Airflow Operators for managing work requirements and worker pools.
"""

from __future__ import annotations

from collections.abc import Collection
from typing import Any, cast

from airflow.sdk import BaseOperator, Context
from requests.exceptions import HTTPError
from yellowdog_client.model import (
    ComputeRequirementTemplateUsage,
    ConfiguredWorkerPool,
    ProvisionedWorkerPool,
    ProvisionedWorkerPoolProperties,
    Task,
    TaskGroup,
    WorkRequirement,
    WorkRequirementStatus,
)
from yellowdog_client.model.exceptions import BaseCustomException

from yellowdog_provider.exceptions.yellowdog_exceptions import (
    YellowDogArgumentError,
    YellowDogException,
)
from yellowdog_provider.hooks.yellowdog_hooks import YellowDogHook
from yellowdog_provider.utils.templating import (
    Templated,
    resolve,
    resolve_optional_str,
)
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


class AddWorkRequirement(YellowDogOperator):
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

    :return: the YellowDog ID of the added work requirement
    :rtype: str
    """

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
        with self.get_hook() as hook:
            client = hook.get_conn()
            self.log.info(f"Adding work requirement '{wr.namespace}/{wr.name}'")
            added = client.work_client.add_work_requirement(wr)
            self.log.info(f"Added work requirement ID '{added.id}'")
            return cast(str, added.id)


class AddTaskGroupsToWorkRequirement(YellowDogOperator):
    """
    Add task groups to a YellowDog work requirement. Either the work_requirement_id
    or both the namespace and work_requirement_name must be supplied, otherwise
    a YellowDogArgumentError is raised. A field that renders to an empty string
    is treated as not supplied; one that renders to ``"None"`` raises a
    YellowDogArgumentError (usually a missing upstream XCom value). All
    arguments are keyword-only.

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
    must be supplied, otherwise a YellowDogArgumentError is raised. If both
    are supplied, the task_group_id is used. A field
    that renders to an empty string is treated as not supplied; one that
    renders to ``"None"`` raises a YellowDogArgumentError (usually a missing
    upstream XCom value). All arguments are keyword-only.

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


class AddPopulatedWorkRequirement(YellowDogOperator):
    """
    Add a 'one-shot', populated YellowDog work requirement to the platform,
    including its task groups and tasks. The supplied work requirement must not
    already contain task groups (a YellowDogArgumentError is raised if it does);
    supply them via task_groups_and_tasks instead. All arguments are
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
    :param task_groups_and_tasks: a list of (TaskGroup, list of Task) pairs,
        one per task group, or a Callable that generates the list (templated)
    :type task_groups_and_tasks: list[tuple[TaskGroup, list[Task]]] | Callable

    :return: the YellowDog ID of the added work requirement
    :rtype: str
    """

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

        with self.get_hook() as hook:
            client = hook.get_conn()

            self.log.info(f"Adding work requirement '{wr.namespace}/{wr.name}'")
            added = client.work_client.add_work_requirement(wr)
            self.log.info(f"Added work requirement ID '{added.id}'")

            task_groups = [task_group for task_group, _ in task_groups_and_tasks]
            self.log.info(
                f"Adding {len(task_groups)} task group(s) to work requirement: "
                f"{[task_group.name for task_group in task_groups]}"
            )
            # The platform preserves submission order, so no reversal is
            # needed; verified by
            # tests/system/test_components.py::test_add_populated_task_group_order
            added.taskGroups = task_groups
            added = client.work_client.update_work_requirement(added)

            for task_group, tasks in task_groups_and_tasks:
                self.log.info(
                    f"Adding {len(tasks)} task(s) to task group '{task_group.name}'"
                )
                client.work_client.add_tasks_to_task_group_by_name(
                    added.namespace,
                    added.name,
                    task_group.name,
                    tasks,
                )

            return cast(str, added.id)


class CancelWorkRequirement(YellowDogOperator):
    """
    Cancel a YellowDog work requirement. Either the work_requirement_id or both
    namespace and work_requirement_name must be supplied, otherwise a
    YellowDogArgumentError is raised. A field that renders to an empty string
    is treated as not supplied; one that renders to ``"None"`` raises a
    YellowDogArgumentError (usually a missing upstream XCom value). All
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

            self.log.info(f"Cancelling work requirement ID '{work_requirement.id}'")
            try:
                work_requirement = client.work_client.cancel_work_requirement_by_id(
                    cast(str, work_requirement.id), self.abort_running_tasks
                )
            except (HTTPError, BaseCustomException) as e:
                # Tolerate idempotent cancellations (YEL-13327); the SDK raises
                # either HTTPError or a BaseCustomException subclass
                if "invalid transition" in str(e).lower():
                    status = cast(WorkRequirementStatus, work_requirement.status)
                    self.log.warning(
                        "Cancellation invalid for work requirement with status "
                        f"'{status.value}'"
                    )
                else:
                    raise

            return cast(str, work_requirement.id)


class ProvisionWorkerPool(YellowDogOperator):
    """
    Provision a YellowDog worker pool. All arguments are keyword-only.

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

    :return: the YellowDog ID of the provisioned worker pool
    :rtype: str
    """

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
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.compute_requirement_template_usage = compute_requirement_template_usage
        self.provisioned_worker_pool_properties = provisioned_worker_pool_properties

    def execute(self, context: Context) -> str:
        """
        Provisions the worker pool.
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
            client = hook.get_conn()
            self.log.info(
                "Provisioning worker pool "
                f"'{crtu.requirementNamespace}/{crtu.requirementName}'"
            )
            provisioned_worker_pool = client.worker_pool_client.provision_worker_pool(
                crtu, pwpp
            )
            self.log.info(f"Provisioned worker pool ID '{provisioned_worker_pool.id}'")
            return cast(str, provisioned_worker_pool.id)


class ShutdownProvisionedWorkerPool(YellowDogOperator):
    """
    Shuts down a YellowDog provisioned worker pool, and optionally
    immediately terminates its associated compute requirement. Either the
    worker_pool_id or both the namespace and worker_pool_name must be
    supplied, otherwise a YellowDogArgumentError is raised. A field that
    renders to an empty string is treated as not supplied; one that renders
    to ``"None"`` raises a YellowDogArgumentError (usually a missing upstream
    XCom value). A configured (or otherwise non-provisioned) worker pool
    raises a YellowDogException. All arguments are keyword-only.

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

    :return: the YellowDog ID of the worker pool that was shut down
    :rtype: str
    """

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
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        require_id_or_name(
            "worker pool",
            "worker_pool_id",
            worker_pool_id,
            {"namespace": namespace, "worker_pool_name": worker_pool_name},
        )
        self.worker_pool_id = worker_pool_id
        self.namespace = namespace
        self.worker_pool_name = worker_pool_name
        self.terminate_immediately = terminate_immediately

    def execute(self, context: Context) -> str:
        """
        Shuts down a worker pool and optionally terminates its compute
        requirement.
        """

        wp_id = resolve_optional_str(self.worker_pool_id, "worker_pool_id")
        namespace = resolve_optional_str(self.namespace, "namespace")
        wp_name = resolve_optional_str(self.worker_pool_name, "worker_pool_name")

        with self.get_hook() as hook:
            client = hook.get_conn()
            worker_pool = get_worker_pool_by_id_or_name(
                client, self.log, wp_id, namespace, wp_name
            )

            if isinstance(worker_pool, ProvisionedWorkerPool):
                self.log.info(
                    f"Shutting down provisioned worker pool ID '{worker_pool.id}'"
                )
                client.worker_pool_client.shutdown_worker_pool_by_id(
                    cast(str, worker_pool.id)
                )
                self.log.info(f"Shut down worker pool ID '{worker_pool.id}'")

                if self.terminate_immediately:
                    self.log.info(
                        "Immediately terminating compute requirement ID "
                        f"'{worker_pool.computeRequirementId}'"
                    )
                    client.compute_client.terminate_compute_requirement_by_id(
                        cast(str, worker_pool.computeRequirementId)
                    )

                return cast(str, worker_pool.id)

            if isinstance(worker_pool, ConfiguredWorkerPool):
                raise YellowDogException(
                    f"Worker pool '{worker_pool.namespace}/{worker_pool.name}' "
                    f"({worker_pool.id}) is a configured worker pool"
                )

            raise YellowDogException(
                f"Worker pool '{worker_pool.namespace}/{worker_pool.name}' "
                f"({worker_pool.id}) is of unsupported type "
                f"'{type(worker_pool).__name__}'; only provisioned worker pools "
                "can be shut down"
            )
