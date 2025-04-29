"""
YellowDog Airflow sensors for work requirements, worker pools and
compute requirements. The sensors test for when one of a specified
set of states is reached.
"""

from collections.abc import Callable, Sequence
from enum import Enum

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from jinja2 import Environment
from yellowdog_client import PlatformClient
from yellowdog_client.model import (
    ComputeRequirement,
    ComputeRequirementStatus,
    ConfiguredWorkerPool,
    ProvisionedWorkerPool,
    WorkerPool,
    WorkerPoolStatus,
    WorkRequirement,
    WorkRequirementStatus,
)

from yellowdog_provider.hooks.yellowdog_hooks import YellowDogHook
from yellowdog_provider.utils.yellowdog_utils import (
    get_compute_requirement_by_id_or_name,
    get_work_requirement_by_id_or_name,
    get_worker_pool_by_id_or_name,
)

XCOM_KEY = "return_value"  # Key to use when writing out IDs from sensors


class ObjectName(Enum):
    """
    Object naming.
    """

    WORK_REQUIREMENT = "work requirement"
    WORKER_POOL = "worker pool"
    COMPUTE_REQUIREMENT = "compute requirement"


class YellowDogSensor(BaseSensorOperator):
    """
    Base class for YellowDog sensors.

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
        self.xcom_written = False

    def check_status(
        self,
        context: Context,
        object_id: str,
        object_type: ObjectName,
        current_state: (
            WorkRequirementStatus | WorkerPoolStatus | ComputeRequirementStatus
        ),
        target_states: (
            list[WorkRequirementStatus]
            | list[WorkerPoolStatus]
            | list[ComputeRequirementStatus]
        ),
        target_states_names: list[str],
    ) -> bool:
        """
        Checks current state against the target state(s) and returns true if the
        condition is met.
        """

        if not self.xcom_written:
            self.log.info(f"Writing {object_type.value} ID to XCom key '{XCOM_KEY}'")
            context["task_instance"].xcom_push(key=XCOM_KEY, value=object_id)
            self.xcom_written = True

        self.log.info(
            f"Checking for {object_type.value} status in {target_states_names}"
        )

        if current_state in target_states:
            self.log.info(
                f"{str(object_type.value).capitalize()} has reached status "
                f"'{current_state.value}'"
            )
            return True

        self.log.info(
            f"Current status is '{current_state.value}' ... "
            f"waiting for {self.poke_interval} seconds"
        )
        return False


class WorkRequirementStateSensor(YellowDogSensor):
    """
    Sensor that tests if a work requirement has reached one of a specified
    set of states. Either the work_requirement_id or both namespace and
    work_requirement_name must be supplied.

    Emits the work requirement ID to XCom using key 'return_value'.

    :param task_id: the Airflow task ID
    :type task_id: str
    :param connection_id: connection to run the operator with (templated) or a
        Callable that generates the connection ID
    :type connection_id: str | Callable
    :param target_states: the list of WorkRequirementStatus states to test for
    :type target_states: list[WorkRequirementStatus]
    :param work_requirement_id: the ID of the work requirement (templated) or
        a Callable that generates the ID
    :type work_requirement_id: str | Callable | None
    :param namespace: the namespace of the work requirement (templated) or a
        Callable that returns the namespace
    :type namespace: str | Callable | None
    :param work_requirement_name: the name of a work requirement (templated) or a
        Callable that generates a work requirement name
    :type work_requirement_name: str | Callable | None
    :param poke_interval: the time between sensor checks in seconds (default = 60.0)
    :type poke_interval: float
    """

    template_fields: Sequence[str] = (
        *YellowDogSensor.template_fields,
        "work_requirement_id",
        "namespace",
        "work_requirement_name",
    )

    def __init__(
        self,
        task_id: str,
        connection_id: Callable[[Context, Environment], str] | str,
        target_states: list[WorkRequirementStatus],
        work_requirement_id: Callable[[Context, Environment], str] | str | None = None,
        namespace: Callable[[Context, Environment], str] | str | None = None,
        work_requirement_name: (
            Callable[[Context, Environment], str] | str | None
        ) = None,
        poke_interval: float = 60,  # Seconds
        **kwargs,
    ):
        super().__init__(
            task_id=task_id,
            connection_id=connection_id,
            poke_interval=poke_interval,
            **kwargs,
        )
        self.work_requirement_id = work_requirement_id
        self.namespace = namespace
        self.work_requirement_name = work_requirement_name
        self.target_states = target_states
        self.target_states_names = [x.value for x in target_states]

    def poke(self, context) -> bool:
        """
        Tests the work requirement's status.
        """

        client: PlatformClient = YellowDogHook(self.connection_id).get_conn()

        work_requirement: WorkRequirement = get_work_requirement_by_id_or_name(
            client,
            self.log,
            self.work_requirement_id,
            self.namespace,
            self.work_requirement_name,
        )

        return self.check_status(
            context,
            work_requirement.id,
            ObjectName.WORK_REQUIREMENT,
            work_requirement.status,
            self.target_states,
            self.target_states_names,
        )


class WorkerPoolStateSensor(YellowDogSensor):
    """
    Sensor that tests if a worker pool has reached one of a specified
    set of states. Either the worker_pool_id or both the namespace and
    worker_pool_name must be supplied.

    Emits the worker pool ID to XCom using key 'return_value'.

    :param task_id: the Airflow task ID
    :type task_id: str
    :param connection_id: connection to run the operator with (templated) or a
        Callable that generates the connection ID
    :type connection_id: str | Callable
    :param target_states: the list of WorkerPoolStatus states to test for
    :type target_states: list[WorkerPoolStatus]
    :param worker_pool_id: the ID of the worker pool (templated) or
        a Callable that generates the ID
    :type worker_pool_id: str | Callable | None
    :param namespace: the namespace of the worker pool (templated) or a
        Callable that returns the namespace
    :type namespace: str | Callable | None
    :param worker_pool_name: the name of a worker pool (templated) or a
        Callable that generates a worker pool name
    :type worker_pool_name: str | Callable | None
    :param poke_interval: the time between sensor checks in seconds (default = 60.0)
    :type poke_interval: float
    """

    template_fields: Sequence[str] = (
        *YellowDogSensor.template_fields,
        "worker_pool_id",
        "namespace",
        "worker_pool_name",
    )

    def __init__(
        self,
        task_id: str,
        connection_id: Callable[[Context, Environment], str] | str,
        target_states: list[WorkerPoolStatus],
        worker_pool_id: Callable[[Context, Environment], str] | str | None = None,
        namespace: Callable[[Context, Environment], str] | str | None = None,
        worker_pool_name: Callable[[Context, Environment], str] | str | None = None,
        poke_interval: float = 60,  # Seconds
        **kwargs,
    ):
        super().__init__(
            task_id=task_id,
            connection_id=connection_id,
            poke_interval=poke_interval,
            **kwargs,
        )
        self.worker_pool_id = worker_pool_id
        self.namespace = namespace
        self.worker_pool_name = worker_pool_name
        self.target_states = target_states
        self.target_states_names = [x.value for x in target_states]

    def poke(self, context) -> bool:
        """
        Tests the worker pool's status.
        """

        client: PlatformClient = YellowDogHook(self.connection_id).get_conn()

        worker_pool: WorkerPool = get_worker_pool_by_id_or_name(
            client,
            self.log,
            self.worker_pool_id,
            self.namespace,
            self.worker_pool_name,
        )

        # Keep the type system happy
        if isinstance(worker_pool, ProvisionedWorkerPool) or isinstance(
            worker_pool, ConfiguredWorkerPool
        ):
            return self.check_status(
                context,
                worker_pool.id,
                ObjectName.WORKER_POOL,
                worker_pool.status,
                self.target_states,
                self.target_states_names,
            )

        return False  # Never gets here


class ComputeRequirementStateSensor(YellowDogSensor):
    """
    Sensor that tests if a compute requirement has reached one of a specified
    set of states. Either the compute_requirement_id or both the namespace and
    compute_requirement_name must be supplied.

    Emits the compute requirement ID to XCom using key 'return_value'.

    :param task_id: the Airflow task ID
    :type task_id: str
    :param connection_id: connection to run the operator with (templated) or a
        Callable that generates the connection ID
    :param target_states: the list of ComputeRequirementStatus states to test for
    :type target_states: list[ComputeRequirementStatus]
    :type connection_id: str | Callable
    :param compute_requirement_id: the ID of the compute requirement (templated) or a
        Callable that returns the ID
    :type compute_requirement_id: str | Callable | None
    :param namespace: the namespace of the work requirement (templated) or a
        Callable that returns the namespace
    :type namespace: str | Callable | None
    :param compute_requirement_name: the name of the compute requirement (templated)
        or a Callable that returns the compute requirement name
    :type compute_requirement_name: str | Callable | None
    :param poke_interval: the time between sensor checks in seconds (default = 60.0)
    :type poke_interval: float
    """

    template_fields: Sequence[str] = (
        *YellowDogSensor.template_fields,
        "compute_requirement_id",
        "namespace",
        "compute_requirement_name",
    )

    def __init__(
        self,
        task_id: str,
        connection_id: Callable[[Context, Environment], str] | str,
        target_states: list[ComputeRequirementStatus],
        compute_requirement_id: (
            Callable[[Context, Environment], str] | str | None
        ) = None,
        namespace: Callable[[Context, Environment], str] | str | None = None,
        compute_requirement_name: (
            Callable[[Context, Environment], str] | str | None
        ) = None,
        poke_interval: float = 60,  # Seconds
        **kwargs,
    ):
        super().__init__(
            task_id=task_id,
            connection_id=connection_id,
            poke_interval=poke_interval,
            **kwargs,
        )
        self.compute_requirement_id = compute_requirement_id
        self.namespace = namespace
        self.compute_requirement_name = compute_requirement_name
        self.target_states = target_states
        self.target_states_names = [x.value for x in target_states]

    def poke(self, context) -> bool:
        """
        Tests the compute requirement's status.
        """

        client: PlatformClient = YellowDogHook(self.connection_id).get_conn()

        compute_requirement: ComputeRequirement = get_compute_requirement_by_id_or_name(
            client,
            self.log,
            self.compute_requirement_id,
            self.namespace,
            self.compute_requirement_name,
        )

        return self.check_status(
            context,
            compute_requirement.id,
            ObjectName.COMPUTE_REQUIREMENT,
            compute_requirement.status,
            self.target_states,
            self.target_states_names,
        )
