"""
YellowDog Airflow sensors for work requirements, worker pools and
compute requirements. The sensors test for when one of a specified
set of states is reached, and fail if a failure state is reached instead.
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

from yellowdog_provider.exceptions.yellowdog_exceptions import (
    YellowDogArgumentError,
    YellowDogStateError,
)
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

# Final states: once reached, a resource never changes state again. By default,
# a sensor's failure states are the final states that are not target states.
WORK_REQUIREMENT_FINAL_STATES: tuple[WorkRequirementStatus, ...] = tuple(
    status for status in WorkRequirementStatus if status.finished
)
WORKER_POOL_FINAL_STATES: tuple[WorkerPoolStatus, ...] = tuple(
    status for status in WorkerPoolStatus if status.finished
)
# ComputeRequirementStatus has no 'finished' flag; STOPPED can be restarted
COMPUTE_REQUIREMENT_FINAL_STATES: tuple[ComputeRequirementStatus, ...] = (
    ComputeRequirementStatus.TERMINATED,
)


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
    :param poke_interval: the time between sensor checks in seconds (default = 60)
    :type poke_interval: float

    Other keyword arguments are passed to Airflow's BaseSensorOperator
    (e.g., ``timeout``, ``mode``).
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
        *,
        failure_states: Sequence[Enum] = (),
        namespace: str | None = None,
        name: str | None = None,
    ) -> bool:
        """
        Pushes the object ID to XCom (first poke only), then checks the current
        state against the target state(s) and returns true if the condition is
        met. Raises :exc:`.YellowDogStateError` if the current state is one of the
        failure states.
        """

        if not self.xcom_written:
            self.log.info(f"Writing {object_type.value} ID to XCom key '{XCOM_KEY}'")
            context["task_instance"].xcom_push(key=XCOM_KEY, value=object_id)  # type: ignore[typeddict-item]
            self.xcom_written = True

        self.log.info(
            f"Checking for {object_type.value} status in "
            f"{[x.value for x in target_states]} (failure states "
            f"{[x.value for x in failure_states]})"
        )

        if current_state in target_states:
            self.log.info(
                f"{str(object_type.value).capitalize()} has reached status "
                f"'{current_state.value}'"
            )
            return True

        if current_state in failure_states:
            raise YellowDogStateError(
                kind=object_type.value,
                resource_id=object_id,
                current_state=current_state,
                target_states=target_states,
                namespace=namespace,
                name=name,
            )

        self.log.info(
            f"Current status is '{current_state.value}' ... "
            f"waiting for {self.poke_interval} seconds"
        )
        return False


def _require_states[S: Enum](
    field_name: str, status_type: type[S], states: Sequence[S]
) -> None:
    for state in states:
        if not isinstance(state, status_type):
            raise YellowDogArgumentError(
                f"'{field_name}' must contain only {status_type.__name__} values; "
                f"got {state!r}"
            )


def _resolve_states[S: Enum](
    status_type: type[S],
    target_states: Sequence[S],
    failure_states: Sequence[S] | None,
    final_states: Sequence[S],
) -> list[S]:
    """
    Validate the target and failure states and return the failure states:
    as given, or (if None) every final state that is not a target state.
    """
    if not target_states:
        raise YellowDogArgumentError("'target_states' must not be empty")
    _require_states("target_states", status_type, target_states)
    if failure_states is None:
        return [state for state in final_states if state not in target_states]
    _require_states("failure_states", status_type, failure_states)
    overlap = [state for state in failure_states if state in target_states]
    if overlap:
        names = ", ".join(f"'{state.value}'" for state in overlap)
        raise YellowDogArgumentError(
            f"'target_states' and 'failure_states' overlap: {names}"
        )
    return list(failure_states)


class WorkRequirementStateSensor(YellowDogSensor):
    """
    Sensor that tests if a work requirement has reached one of a specified
    set of states. Either the work_requirement_id or both namespace and
    work_requirement_name must be supplied, otherwise a
    :exc:`.YellowDogArgumentError` is raised. A field that renders to ``"None"``
    raises a :exc:`.YellowDogArgumentError`. All arguments are keyword-only.

    Emits the work requirement ID to XCom using key 'return_value'.

    :param task_id: the Airflow task ID
    :type task_id: str
    :param connection_id: connection to run the sensor with (templated) or a
        Callable that generates the connection ID; defaults to
        ``yellowdog_default``
    :type connection_id: str | Callable
    :param target_states: the (non-empty) list of WorkRequirementStatus states
        to test for
    :type target_states: list[WorkRequirementStatus]
    :param failure_states: the WorkRequirementStatus states that make the sensor
        fail at once with :exc:`.YellowDogStateError`. Defaults to the final states
        (COMPLETED, FAILED and CANCELLED) that are not in target_states; an empty
        list disables failure detection
    :type failure_states: Sequence[WorkRequirementStatus] | None
    :param work_requirement_id: the ID of the work requirement
        (templated) or a Callable that generates it
    :type work_requirement_id: str | Callable | None
    :param namespace: the namespace of the work requirement
        (templated) or a Callable that generates it
    :type namespace: str | Callable | None
    :param work_requirement_name: the name of the work requirement
        (templated) or a Callable that generates it
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
        failure_states: Sequence[WorkRequirementStatus] | None = None,
        work_requirement_id: Templated[str] | None = None,
        namespace: Templated[str] | None = None,
        work_requirement_name: Templated[str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        failure_states_ = _resolve_states(
            WorkRequirementStatus,
            target_states,
            failure_states,
            WORK_REQUIREMENT_FINAL_STATES,
        )
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
        self.failure_states: list[WorkRequirementStatus] = failure_states_

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
                failure_states=self.failure_states,
                namespace=work_requirement.namespace,
                name=work_requirement.name,
            )


class WorkerPoolStateSensor(YellowDogSensor):
    """
    Sensor that tests if a worker pool has reached one of a specified
    set of states. Either the worker_pool_id or both the namespace and
    worker_pool_name must be supplied, otherwise a
    :exc:`.YellowDogArgumentError` is raised. A field that renders to ``"None"``
    raises a :exc:`.YellowDogArgumentError`. All arguments are keyword-only.

    Emits the worker pool ID to XCom using key 'return_value'.

    :param task_id: the Airflow task ID
    :type task_id: str
    :param connection_id: connection to run the sensor with (templated) or a
        Callable that generates the connection ID; defaults to
        ``yellowdog_default``
    :type connection_id: str | Callable
    :param target_states: the (non-empty) list of WorkerPoolStatus states to
        test for
    :type target_states: list[WorkerPoolStatus]
    :param failure_states: the WorkerPoolStatus states that make the sensor
        fail at once with :exc:`.YellowDogStateError`. Defaults to the final states
        (SHUTDOWN and TERMINATED) that are not in target_states; an empty
        list disables failure detection
    :type failure_states: Sequence[WorkerPoolStatus] | None
    :param worker_pool_id: the ID of the worker pool
        (templated) or a Callable that generates it
    :type worker_pool_id: str | Callable | None
    :param namespace: the namespace of the worker pool
        (templated) or a Callable that generates it
    :type namespace: str | Callable | None
    :param worker_pool_name: the name of the worker pool
        (templated) or a Callable that generates it
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
        failure_states: Sequence[WorkerPoolStatus] | None = None,
        worker_pool_id: Templated[str] | None = None,
        namespace: Templated[str] | None = None,
        worker_pool_name: Templated[str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        failure_states_ = _resolve_states(
            WorkerPoolStatus, target_states, failure_states, WORKER_POOL_FINAL_STATES
        )
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
        self.failure_states: list[WorkerPoolStatus] = failure_states_

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
                failure_states=self.failure_states,
                namespace=worker_pool.namespace,
                name=worker_pool.name,
            )


class ComputeRequirementStateSensor(YellowDogSensor):
    """
    Sensor that tests if a compute requirement has reached one of a specified
    set of states. Either the compute_requirement_id or both the namespace and
    compute_requirement_name must be supplied, otherwise a
    :exc:`.YellowDogArgumentError` is raised. A field that renders to ``"None"``
    raises a :exc:`.YellowDogArgumentError`. All arguments are keyword-only.

    Emits the compute requirement ID to XCom using key 'return_value'.

    :param task_id: the Airflow task ID
    :type task_id: str
    :param connection_id: connection to run the sensor with (templated) or a
        Callable that generates the connection ID; defaults to
        ``yellowdog_default``
    :type connection_id: str | Callable
    :param target_states: the (non-empty) list of ComputeRequirementStatus
        states to test for
    :type target_states: list[ComputeRequirementStatus]
    :param failure_states: the ComputeRequirementStatus states that make the sensor
        fail at once with :exc:`.YellowDogStateError`. Defaults to the final states
        (TERMINATED) that are not in target_states; an empty
        list disables failure detection
    :type failure_states: Sequence[ComputeRequirementStatus] | None
    :param compute_requirement_id: the ID of the compute requirement
        (templated) or a Callable that generates it
    :type compute_requirement_id: str | Callable | None
    :param namespace: the namespace of the compute requirement
        (templated) or a Callable that generates it
    :type namespace: str | Callable | None
    :param compute_requirement_name: the name of the compute requirement
        (templated) or a Callable that generates it
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
        failure_states: Sequence[ComputeRequirementStatus] | None = None,
        compute_requirement_id: Templated[str] | None = None,
        namespace: Templated[str] | None = None,
        compute_requirement_name: Templated[str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        failure_states_ = _resolve_states(
            ComputeRequirementStatus,
            target_states,
            failure_states,
            COMPUTE_REQUIREMENT_FINAL_STATES,
        )
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
        self.failure_states: list[ComputeRequirementStatus] = failure_states_

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
                failure_states=self.failure_states,
                namespace=compute_requirement.namespace,
                name=compute_requirement.name,
            )
