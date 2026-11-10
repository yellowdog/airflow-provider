"""
YellowDog Airflow sensors for work requirements, worker pools and
compute requirements. The sensors test for when one of a specified
set of states is reached.
"""

from __future__ import annotations

from collections.abc import Collection, Sequence
from enum import Enum
from typing import Any, cast

from airflow.sdk import BaseSensorOperator, Context
from yellowdog_client.model import (
    ComputeRequirementStatus,
    WorkerPoolStatus,
    WorkRequirementStatus,
)

from yellowdog_provider.exceptions.yellowdog_exceptions import YellowDogArgumentError
from yellowdog_provider.hooks.yellowdog_hooks import YellowDogHook
from yellowdog_provider.utils.templating import (
    Templated,
    resolve,
    resolve_optional_str,
)
from yellowdog_provider.utils.yellowdog_utils import (
    get_compute_requirement_by_id_or_name,
    get_work_requirement_by_id_or_name,
    get_worker_pool_by_id_or_name,
    require_id_or_name,
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
    Base class for YellowDog sensors. All arguments are keyword-only.

    :param connection_id: connection to run the sensor with (templated) or a
        Callable that generates the connection ID; defaults to
        ``yellowdog_default``
    :type connection_id: str | Callable
    """

    template_fields: Collection[str] = ("connection_id",)

    def __init__(
        self,
        *,
        connection_id: Templated[str] = YellowDogHook.default_conn_name,
        poke_interval: float = 60,  # Seconds
        **kwargs: Any,
    ) -> None:
        # Airflow's BaseSensorOperator.__init__ has untyped **kwargs
        super().__init__(  # pyright: ignore[reportUnknownMemberType]
            poke_interval=poke_interval, **kwargs
        )
        self.connection_id = connection_id
        self.xcom_written: bool = False

    def get_hook(self) -> YellowDogHook:
        """Return a hook for the (rendered) connection ID."""
        return YellowDogHook(resolve(self.connection_id, "connection_id"))

    def check_status(
        self,
        context: Context,
        object_id: str,
        object_type: ObjectName,
        current_state: Enum,
        target_states: Sequence[Enum],
    ) -> bool:
        """
        Pushes the object ID to XCom (first poke only), then checks the current
        state against the target state(s) and returns true if the condition is met.
        """

        if not self.xcom_written:
            self.log.info(f"Writing {object_type.value} ID to XCom key '{XCOM_KEY}'")
            context["task_instance"].xcom_push(key=XCOM_KEY, value=object_id)  # type: ignore[typeddict-item]
            self.xcom_written = True

        self.log.info(
            f"Checking for {object_type.value} status in "
            f"{[x.value for x in target_states]}"
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


def _require_target_states(states: Sequence[Enum]) -> None:
    if not states:
        raise YellowDogArgumentError("'target_states' must not be empty")


class WorkRequirementStateSensor(YellowDogSensor):
    """
    Sensor that tests if a work requirement has reached one of a specified
    set of states. Either the work_requirement_id or both namespace and
    work_requirement_name must be supplied, otherwise a YellowDogArgumentError
    is raised. A field that renders to ``"None"`` raises a YellowDogArgumentError.
    All arguments are keyword-only.

    Emits the work requirement ID to XCom using key 'return_value'.

    :param task_id: the Airflow task ID
    :type task_id: str
    :param connection_id: connection to run the sensor with (templated);
        defaults to ``yellowdog_default``
    :type connection_id: str | Callable
    :param target_states: the (non-empty) list of WorkRequirementStatus states
        to test for
    :type target_states: list[WorkRequirementStatus]
    :param work_requirement_id: the ID of the work requirement (templated)
    :type work_requirement_id: str | Callable | None
    :param namespace: the namespace of the work requirement (templated)
    :type namespace: str | Callable | None
    :param work_requirement_name: the name of the work requirement (templated)
    :type work_requirement_name: str | Callable | None
    :param poke_interval: the time between sensor checks in seconds (default = 60)
    :type poke_interval: float
    """

    template_fields: Collection[str] = (
        *YellowDogSensor.template_fields,
        "work_requirement_id",
        "namespace",
        "work_requirement_name",
    )

    def __init__(
        self,
        *,
        target_states: list[WorkRequirementStatus],
        work_requirement_id: Templated[str] | None = None,
        namespace: Templated[str] | None = None,
        work_requirement_name: Templated[str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        _require_target_states(target_states)
        require_id_or_name(
            "work requirement",
            "work_requirement_id",
            work_requirement_id,
            {"namespace": namespace, "work_requirement_name": work_requirement_name},
        )
        self.work_requirement_id = work_requirement_id
        self.namespace = namespace
        self.work_requirement_name = work_requirement_name
        self.target_states = target_states

    def poke(self, context: Context) -> bool:
        """
        Tests the work requirement's status.
        """

        wr_id = resolve_optional_str(self.work_requirement_id, "work_requirement_id")
        namespace = resolve_optional_str(self.namespace, "namespace")
        wr_name = resolve_optional_str(
            self.work_requirement_name, "work_requirement_name"
        )
        with self.get_hook() as hook:
            work_requirement = get_work_requirement_by_id_or_name(
                hook.get_conn(), self.log, wr_id, namespace, wr_name
            )
            return self.check_status(
                context,
                cast(str, work_requirement.id),
                ObjectName.WORK_REQUIREMENT,
                cast(WorkRequirementStatus, work_requirement.status),
                self.target_states,
            )


class WorkerPoolStateSensor(YellowDogSensor):
    """
    Sensor that tests if a worker pool has reached one of a specified
    set of states. Either the worker_pool_id or both the namespace and
    worker_pool_name must be supplied, otherwise a YellowDogArgumentError
    is raised. A field that renders to ``"None"`` raises a YellowDogArgumentError.
    All arguments are keyword-only.

    Emits the worker pool ID to XCom using key 'return_value'.

    :param task_id: the Airflow task ID
    :type task_id: str
    :param connection_id: connection to run the sensor with (templated);
        defaults to ``yellowdog_default``
    :type connection_id: str | Callable
    :param target_states: the (non-empty) list of WorkerPoolStatus states to
        test for
    :type target_states: list[WorkerPoolStatus]
    :param worker_pool_id: the ID of the worker pool (templated)
    :type worker_pool_id: str | Callable | None
    :param namespace: the namespace of the worker pool (templated)
    :type namespace: str | Callable | None
    :param worker_pool_name: the name of the worker pool (templated)
    :type worker_pool_name: str | Callable | None
    :param poke_interval: the time between sensor checks in seconds (default = 60)
    :type poke_interval: float
    """

    template_fields: Collection[str] = (
        *YellowDogSensor.template_fields,
        "worker_pool_id",
        "namespace",
        "worker_pool_name",
    )

    def __init__(
        self,
        *,
        target_states: list[WorkerPoolStatus],
        worker_pool_id: Templated[str] | None = None,
        namespace: Templated[str] | None = None,
        worker_pool_name: Templated[str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        _require_target_states(target_states)
        require_id_or_name(
            "worker pool",
            "worker_pool_id",
            worker_pool_id,
            {"namespace": namespace, "worker_pool_name": worker_pool_name},
        )
        self.worker_pool_id = worker_pool_id
        self.namespace = namespace
        self.worker_pool_name = worker_pool_name
        self.target_states = target_states

    def poke(self, context: Context) -> bool:
        """
        Tests the worker pool's status.
        """

        pool_id = resolve_optional_str(self.worker_pool_id, "worker_pool_id")
        namespace = resolve_optional_str(self.namespace, "namespace")
        pool_name = resolve_optional_str(self.worker_pool_name, "worker_pool_name")
        with self.get_hook() as hook:
            worker_pool = get_worker_pool_by_id_or_name(
                hook.get_conn(), self.log, pool_id, namespace, pool_name
            )
            return self.check_status(
                context,
                cast(str, worker_pool.id),
                ObjectName.WORKER_POOL,
                cast(WorkerPoolStatus, worker_pool.status),
                self.target_states,
            )


class ComputeRequirementStateSensor(YellowDogSensor):
    """
    Sensor that tests if a compute requirement has reached one of a specified
    set of states. Either the compute_requirement_id or both the namespace and
    compute_requirement_name must be supplied, otherwise a YellowDogArgumentError
    is raised. A field that renders to ``"None"`` raises a YellowDogArgumentError.
    All arguments are keyword-only.

    Emits the compute requirement ID to XCom using key 'return_value'.

    :param task_id: the Airflow task ID
    :type task_id: str
    :param connection_id: connection to run the sensor with (templated);
        defaults to ``yellowdog_default``
    :type connection_id: str | Callable
    :param target_states: the (non-empty) list of ComputeRequirementStatus
        states to test for
    :type target_states: list[ComputeRequirementStatus]
    :param compute_requirement_id: the ID of the compute requirement (templated)
    :type compute_requirement_id: str | Callable | None
    :param namespace: the namespace of the compute requirement (templated)
    :type namespace: str | Callable | None
    :param compute_requirement_name: the name of the compute requirement (templated)
    :type compute_requirement_name: str | Callable | None
    :param poke_interval: the time between sensor checks in seconds (default = 60)
    :type poke_interval: float
    """

    template_fields: Collection[str] = (
        *YellowDogSensor.template_fields,
        "compute_requirement_id",
        "namespace",
        "compute_requirement_name",
    )

    def __init__(
        self,
        *,
        target_states: list[ComputeRequirementStatus],
        compute_requirement_id: Templated[str] | None = None,
        namespace: Templated[str] | None = None,
        compute_requirement_name: Templated[str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        _require_target_states(target_states)
        require_id_or_name(
            "compute requirement",
            "compute_requirement_id",
            compute_requirement_id,
            {
                "namespace": namespace,
                "compute_requirement_name": compute_requirement_name,
            },
        )
        self.compute_requirement_id = compute_requirement_id
        self.namespace = namespace
        self.compute_requirement_name = compute_requirement_name
        self.target_states = target_states

    def poke(self, context: Context) -> bool:
        """
        Tests the compute requirement's status.
        """

        cr_id = resolve_optional_str(
            self.compute_requirement_id, "compute_requirement_id"
        )
        namespace = resolve_optional_str(self.namespace, "namespace")
        cr_name = resolve_optional_str(
            self.compute_requirement_name, "compute_requirement_name"
        )
        with self.get_hook() as hook:
            compute_requirement = get_compute_requirement_by_id_or_name(
                hook.get_conn(), self.log, cr_id, namespace, cr_name
            )
            return self.check_status(
                context,
                cast(str, compute_requirement.id),
                ObjectName.COMPUTE_REQUIREMENT,
                cast(ComputeRequirementStatus, compute_requirement.status),
                self.target_states,
            )
