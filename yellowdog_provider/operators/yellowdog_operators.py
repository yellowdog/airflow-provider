"""
YellowDog Airflow Operators for managing work requirements and worker pools.
"""

from collections.abc import Callable, Sequence

from airflow.models import BaseOperator
from airflow.utils.context import Context
from jinja2 import Environment
from requests.exceptions import HTTPError
from yellowdog_client import PlatformClient
from yellowdog_client.model import (
    ComputeRequirementTemplateUsage,
    ConfiguredWorkerPool,
    ProvisionedWorkerPool,
    ProvisionedWorkerPoolProperties,
    Task,
    TaskGroup,
    WorkRequirement,
)

from yellowdog_provider.exceptions.yellowdog_exceptions import YellowDogException
from yellowdog_provider.hooks.yellowdog_hooks import YellowDogHook
from yellowdog_provider.utils.yellowdog_utils import (
    get_work_requirement_by_id_or_name,
    get_worker_pool_by_id_or_name,
)


class YellowDogOperator(BaseOperator):
    """
    Base class for YellowDog operators.

    :param connection_id: connection to run the operator with (templated) or a
        Callable that generates the connection ID
    :type connection_id: str | Callable
    """

    template_fields: Sequence[str] = ("connection_id",)

    def __init__(
        self,
        connection_id: Callable[[Context, Environment], str] | str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.connection_id = connection_id


class AddWorkRequirement(YellowDogOperator):
    """
    Add a YellowDog work requirement to the platform.

    :param task_id: the Airflow task ID
    :type task_id: str
    :param connection_id: connection to run the operator with (templated) or a
        Callable that generates the connection ID
    :type connection_id: str | Callable
    :param work_requirement: a YellowDog WorkRequirement object or a
        Callable that generates a WorkRequirement object (templated)
    :type work_requirement: WorkRequirement or Callable

    :return: the YellowDog ID of the added work requirement
    :rtype: str
    """

    template_fields: Sequence[str] = (
        *YellowDogOperator.template_fields,
        "work_requirement",
    )

    def __init__(
        self,
        task_id: str,
        connection_id: Callable[[Context, Environment], str] | str,
        work_requirement: (
            Callable[[Context, Environment], WorkRequirement] | WorkRequirement
        ),
        **kwargs,
    ):
        super().__init__(task_id=task_id, connection_id=connection_id, **kwargs)
        self.work_requirement = work_requirement

    def execute(self, context: Context) -> str:
        """
        Adds the work requirement.
        """

        client: PlatformClient = YellowDogHook(self.connection_id).get_conn()

        self.log.info(
            f"Adding work requirement '{self.work_requirement.namespace}/"
            f"{self.work_requirement.name}'"
        )
        self.work_requirement = client.work_client.add_work_requirement(
            self.work_requirement
        )
        self.log.info(f"Added work requirement ID '{self.work_requirement.id}'")

        return self.work_requirement.id


class AddTaskGroupsToWorkRequirement(YellowDogOperator):
    """
    Add task groups to a YellowDog work requirement. Either the work_requirement_id
    or both the namespace and work_requirement_name must be supplied.

    :param task_id: the Airflow task ID
    :type task_id: str
    :param connection_id: connection to run the operator with (templated) or a
        Callable that generates the connection ID
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

    :return: the YellowDog IDs of the added task groups
    :rtype: list[str]
    """

    template_fields: Sequence[str] = (
        *YellowDogOperator.template_fields,
        "work_requirement_id",
        "namespace",
        "work_requirement_name",
        "task_groups",
    )

    def __init__(
        self,
        task_id: str,
        connection_id: Callable[[Context, Environment], str] | str,
        task_groups: (
            Callable[[Context, Environment], list[TaskGroup]] | list[TaskGroup]
        ),
        work_requirement_id: Callable[[Context, Environment], str] | str | None = None,
        namespace: Callable[[Context, Environment], str] | str | None = None,
        work_requirement_name: (
            Callable[[Context, Environment], str] | str | None
        ) = None,
        **kwargs,
    ):
        super().__init__(task_id=task_id, connection_id=connection_id, **kwargs)
        self.work_requirement_id = work_requirement_id
        self.namespace = namespace
        self.work_requirement_name = work_requirement_name
        self.task_groups = task_groups

    def execute(self, context: Context) -> list[str]:
        """
        Adds the task groups to the work requirement.
        """

        client: PlatformClient = YellowDogHook(self.connection_id).get_conn()

        work_requirement = get_work_requirement_by_id_or_name(
            client,
            self.log,
            self.work_requirement_id,
            self.namespace,
            self.work_requirement_name,
        )

        if work_requirement.taskGroups is None:
            work_requirement.taskGroups = self.task_groups
        else:
            work_requirement.taskGroups += self.task_groups

        self.log.info(
            f"Adding {len(self.task_groups)} task group(s) to work requirement"
        )
        work_requirement = client.work_client.update_work_requirement(work_requirement)

        task_group_names = [task_group_.name for task_group_ in self.task_groups]
        added_task_group_ids = []
        for task_group_ in work_requirement.taskGroups:
            if task_group_.name in task_group_names:
                self.log.info(
                    f"Added task group '{task_group_.name}' ID '{task_group_.id}'"
                )
                added_task_group_ids.append(task_group_.id)

        return added_task_group_ids


class AddTasksToTaskGroup(YellowDogOperator):
    """
    Add a list of tasks to a task group. Either the task_group_id,
    or all of: namespace, work_requirement_name and task_group_name,
    must be supplied.

    :param task_id: the Airflow task ID
    :type task_id: str
    :param connection_id: connection to run the operator with (templated) or a
        Callable that generates the connection ID
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

    template_fields: Sequence[str] = (
        *YellowDogOperator.template_fields,
        "namespace",
        "task_group_id",
        "work_requirement_name",
        "task_group_name",
        "tasks",
    )

    def __init__(
        self,
        task_id: str,
        connection_id: Callable[[Context, Environment], str] | str,
        tasks: Callable[[Context, Environment], list[Task]] | list[Task],
        task_group_id: Callable[[Context, Environment], str] | str | None = None,
        namespace: Callable[[Context, Environment], str] | str | None = None,
        work_requirement_name: (
            Callable[[Context, Environment], str] | str | None
        ) = None,
        task_group_name: Callable[[Context, Environment], str] | str | None = None,
        **kwargs,
    ):
        super().__init__(task_id=task_id, connection_id=connection_id, **kwargs)
        self.task_group_id = task_group_id
        self.namespace = namespace
        self.work_requirement_name = work_requirement_name
        self.task_group_name = task_group_name
        self.tasks = tasks

    def execute(self, context: Context) -> list[str]:
        """
        Adds the tasks to the task group.
        """

        client: PlatformClient = YellowDogHook(self.connection_id).get_conn()

        if self.task_group_id is not None:
            self.log.info(
                f"Adding {len(self.tasks)} task(s) to task group ID '{self.task_group_id}'"
            )
            added_tasks = client.work_client.add_tasks_to_task_group_by_id(
                task_group_id=self.task_group_id,
                tasks=self.tasks,
            )

        elif (
            self.namespace is not None
            and self.work_requirement_name is not None
            and self.task_group_name is not None
        ):
            self.log.info(
                f"Adding {len(self.tasks)} task(s) to task group '{self.task_group_name}' in "
                f"work requirement '{self.namespace}/{self.work_requirement_name}'"
            )
            added_tasks = client.work_client.add_tasks_to_task_group_by_name(
                self.namespace,
                self.work_requirement_name,
                self.task_group_name,
                self.tasks,
            )

        else:
            raise YellowDogException(
                f"Either 'task_group_id ({self.task_group_id})', "
                f"or all of 'namespace' ({self.namespace}), "
                f"'work_requirement_name' ({self.work_requirement_name}) and"
                f"'task_group_name ({self.task_group_name})' must be supplied"
            )

        return [task_.id for task_ in added_tasks]


class AddPopulatedWorkRequirement(YellowDogOperator):
    """
    Add a 'one-shot', populated YellowDog work requirement to the platform,
    including its task groups and tasks.

    :param task_id: the Airflow task ID
    :type task_id: str
    :param connection_id: connection to run the operator with (templated) or a
        Callable that generates the connection ID
    :type connection_id: str | Callable
    :param work_requirement: a YellowDog WorkRequirement object or a
        Callable that generates a WorkRequirement object (templated)
    :type work_requirement: WorkRequirement | Callable
    :param task_groups_and_tasks: a list of YellowDog TaskGroup objects with
        their constituent Task objects, or a Callable that generates the list
        (templated)
    :type task_groups_and_tasks: list[tuple[TaskGroup, list[Task]]] | Callable

    :return: the YellowDog ID of the added work requirement
    :rtype: str
    """

    template_fields: Sequence[str] = (
        *YellowDogOperator.template_fields,
        "work_requirement",
        "task_groups_and_tasks",
    )

    def __init__(
        self,
        task_id: str,
        connection_id: Callable[[Context, Environment], str] | str,
        work_requirement: (
            Callable[[Context, Environment], WorkRequirement] | WorkRequirement
        ),
        task_groups_and_tasks: (
            Callable[[Context, Environment], list[tuple[TaskGroup, list[Task]]]]
            | list[tuple[TaskGroup, list[Task]]]
        ),
        **kwargs,
    ):
        super().__init__(task_id=task_id, connection_id=connection_id, **kwargs)
        self.work_requirement = work_requirement
        self.task_groups_and_tasks = task_groups_and_tasks

    def execute(self, context: Context) -> str:
        """
        Adds the work requirement, task groups and tasks.
        """

        client: PlatformClient = YellowDogHook(self.connection_id).get_conn()

        self.log.info(
            f"Adding work requirement '{self.work_requirement.namespace}/"
            f"{self.work_requirement.name}'"
        )
        self.work_requirement = client.work_client.add_work_requirement(
            self.work_requirement
        )
        self.log.info(f"Added work requirement ID '{self.work_requirement.id}'")

        self.work_requirement.taskGroups = [
            task_group for task_group, _ in self.task_groups_and_tasks
        ]
        self.log.info(
            f"Adding {len(self.work_requirement.taskGroups)} task group(s) to work requirement: "
            f"{[task_group.name for task_group in self.work_requirement.taskGroups]}"
        )

        self.work_requirement.taskGroups.reverse()  # Maintain task group sequencing
        self.work_requirement = client.work_client.update_work_requirement(
            self.work_requirement
        )

        for task_group, tasks in self.task_groups_and_tasks:
            self.log.info(
                f"Adding {len(tasks)} task(s) to task group '{task_group.name}'"
            )
            client.work_client.add_tasks_to_task_group_by_name(
                self.work_requirement.namespace,
                self.work_requirement.name,
                task_group.name,
                tasks,
            )

        return self.work_requirement.id


class CancelWorkRequirement(YellowDogOperator):
    """
    Cancel a YellowDog work requirement. Either the work_requirement_id or both
    namespace and work_requirement_name must be supplied.

    :param task_id: the Airflow task ID
    :type task_id: str
    :param connection_id: connection to run the operator with (templated) or a
        Callable that generates the connection ID
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
    :param abort_running_tasks: abort currently running tasks; defaults to 'False'
    :type abort_running_tasks: bool

    :return: the YellowDog ID of the cancelled work requirement
    :rtype: str
    """

    template_fields: Sequence[str] = (
        *YellowDogOperator.template_fields,
        "work_requirement_id",
        "namespace",
        "work_requirement_name",
    )

    def __init__(
        self,
        task_id: str,
        connection_id: Callable[[Context, Environment], str] | str,
        work_requirement_id: Callable[[Context, Environment], str] | str | None = None,
        namespace: Callable[[Context, Environment], str] | str | None = None,
        work_requirement_name: (
            Callable[[Context, Environment], str] | str | None
        ) = None,
        abort_running_tasks: bool = False,
        **kwargs,
    ):
        super().__init__(task_id=task_id, connection_id=connection_id, **kwargs)
        self.work_requirement_id = work_requirement_id
        self.namespace = namespace
        self.work_requirement_name = work_requirement_name
        self.abort_running_tasks = abort_running_tasks

    def execute(self, context: Context) -> str:
        """
        Cancels the work requirement, optionally aborting running tasks.
        """

        client: PlatformClient = YellowDogHook(self.connection_id).get_conn()

        work_requirement = get_work_requirement_by_id_or_name(
            client,
            self.log,
            self.work_requirement_id,
            self.namespace,
            self.work_requirement_name,
        )

        self.log.info(f"Cancelling work requirement ID '{work_requirement.id}'")
        try:
            work_requirement = client.work_client.cancel_work_requirement_by_id(
                work_requirement.id, self.abort_running_tasks
            )
        except HTTPError as e:  # Tolerate idempotent cancellations (YEL-13327)
            if "invalid transition" in str(e).lower():
                self.log.warning(
                    "Cancellation invalid for work requirement with status "
                    f"'{work_requirement.status.value}'"
                )
            else:
                raise e

        return work_requirement.id


class ProvisionWorkerPool(YellowDogOperator):
    """
    Provision a YellowDog worker pool.

    :param task_id: the Airflow task ID
    :type task_id: str
    :param connection_id: connection to run the operator with (templated) or a
        Callable that generates the connection ID
    :type connection_id: str | Callable
    :param compute_requirement_template_usage: a ComputeRequirementTemplateUsage object or
        a Callable that generates the object (templated)
    :type compute_requirement_template_usage: ComputeRequirementTemplateUsage | Callable
    :param provisioned_worker_pool_properties: a ProvisionedWorkerPoolProperties object
        or a Callable that generates the object (templated)
    :type provisioned_worker_pool_properties: ProvisionedWorkerPoolProperties | Callable | None

    :return: the YellowDog ID of the provisioned worker pool
    :rtype: str
    """

    template_fields: Sequence[str] = (
        *YellowDogOperator.template_fields,
        "compute_requirement_template_usage",
        "provisioned_worker_pool_properties",
    )

    def __init__(
        self,
        task_id: str,
        connection_id: Callable[[Context, Environment], str] | str,
        compute_requirement_template_usage: (
            Callable[[Context, Environment], ComputeRequirementTemplateUsage]
            | ComputeRequirementTemplateUsage
        ),
        provisioned_worker_pool_properties: (
            Callable[[Context, Environment], ProvisionedWorkerPoolProperties]
            | ProvisionedWorkerPoolProperties
            | None
        ),
        **kwargs,
    ):
        super().__init__(task_id=task_id, connection_id=connection_id, **kwargs)
        self.compute_requirement_template_usage = compute_requirement_template_usage
        self.provisioned_worker_pool_properties = provisioned_worker_pool_properties

    def execute(self, context: Context) -> str:
        """
        Provisions the worker pool.
        """

        client: PlatformClient = YellowDogHook(self.connection_id).get_conn()

        self.log.info(
            "Provisioning worker pool "
            f"'{self.compute_requirement_template_usage.requirementNamespace}/"
            f"{self.compute_requirement_template_usage.requirementName}'"
        )

        provisioned_worker_pool = client.worker_pool_client.provision_worker_pool(
            self.compute_requirement_template_usage,
            self.provisioned_worker_pool_properties,
        )
        self.log.info(f"Provisioned worker pool ID '{provisioned_worker_pool.id}'")

        return provisioned_worker_pool.id


class ShutdownProvisionedWorkerPool(YellowDogOperator):
    """
    Shuts down a YellowDog provisioned worker pool, and optionally
    immediately terminates its associated compute requirement. Either the
    worker_pool_id or both the namespace and worker_pool_name must be
    supplied.

    :param task_id: the Airflow task ID
    :type task_id: str
    :param connection_id: connection to run the operator with (templated) or a
        Callable that generates the connection ID
    :type connection_id: str | Callable
    :param worker_pool_id: the ID of the worker pool (templated) or
        a Callable that generates the ID
    :type work_requirement_id: str | Callable | None
    :param namespace: the namespace of the worker pool (templated) or a
        Callable that generates the namespace
    :type namespace: str | Callable | None
    :param worker_pool_name: the name of the worker pool (templated) or a
        Callable that generates a worker pool name
    :type worker_pool_name: str | Callable | None
    :param terminate_immediately: immediately terminate the associated
        compute requirement (default: False)
    :type terminate_immediately: bool

    :return: the YellowDog ID of the worker pool that was shut down
    :rtype: str
    """

    template_fields: Sequence[str] = (
        *YellowDogOperator.template_fields,
        "worker_pool_id",
        "namespace",
        "worker_pool_name",
    )

    def __init__(
        self,
        task_id: str,
        connection_id: Callable[[Context, Environment], str] | str,
        worker_pool_id: Callable[[Context, Environment], str] | str | None = None,
        namespace: Callable[[Context, Environment], str] | str | None = None,
        worker_pool_name: Callable[[Context, Environment], str] | str | None = None,
        terminate_immediately: bool = False,
        **kwargs,
    ):
        super().__init__(task_id=task_id, connection_id=connection_id, **kwargs)
        self.worker_pool_id = worker_pool_id
        self.namespace = namespace
        self.worker_pool_name = worker_pool_name
        self.terminate_immediately = terminate_immediately

    def execute(self, context: Context) -> str | None:
        """
        Shuts down a worker pool and optionally terminates its compute
        requirement.
        """

        client: PlatformClient = YellowDogHook(self.connection_id).get_conn()

        worker_pool = get_worker_pool_by_id_or_name(
            client, self.log, self.worker_pool_id, self.namespace, self.worker_pool_name
        )

        if isinstance(worker_pool, ProvisionedWorkerPool):
            self.log.info(
                f"Shutting down provisioned worker pool ID '{worker_pool.id}'"
            )
            client.worker_pool_client.shutdown_worker_pool_by_id(worker_pool.id)
            self.log.info(f"Shut down worker pool ID '{worker_pool.id}'")

            if self.terminate_immediately:
                self.log.info(
                    "Immediately terminating compute requirement ID "
                    f"'{worker_pool.computeRequirementId}'"
                )
                client.compute_client.terminate_compute_requirement_by_id(
                    worker_pool.computeRequirementId
                )

            return worker_pool.id

        elif isinstance(worker_pool, ConfiguredWorkerPool):
            raise YellowDogException(
                f"Worker pool '{worker_pool.namespace}/{worker_pool.name}' "
                f"({worker_pool.id}) is a configured worker pool"
            )

        return None
