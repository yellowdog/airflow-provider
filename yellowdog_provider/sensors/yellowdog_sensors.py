"""
YellowDog Airflow sensors for work requirements, worker pools and
compute requirements. The sensors test for when one of a specified
set of states is reached, and fail if a failure state is reached instead.
"""

from __future__ import annotations

import time
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Any, cast

from airflow.sdk import BaseOperatorLink, BaseSensorOperator, Context
from yellowdog_client.model import (
    ComputeRequirementStatus,
    WorkerPoolStatus,
    WorkRequirementStatus,
)

from yellowdog_provider.exceptions.yellowdog_exceptions import (
    YellowDogException,
    YellowDogStateError,
)
from yellowdog_provider.hooks.yellowdog_hooks import YellowDogHook
from yellowdog_provider.links.yellowdog_links import (
    WorkerPoolPortalLink,
    WorkRequirementPortalLink,
)
from yellowdog_provider.triggers.yellowdog_triggers import (
    ResourceKind,
    YellowDogStateTrigger,
)
from yellowdog_provider.utils.airflow_compat import (
    AirflowSensorTimeout,
    AirflowSkipException,
    conf,
)
from yellowdog_provider.utils.templating import (
    Templated,
    resolve,
    resolve_optional_str,
)
from yellowdog_provider.utils.waiting import resolve_states
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


@dataclass(frozen=True)
class _WaitSpec:
    """What a sensor waits for, in the form its deferral trigger needs."""

    kind: ResourceKind
    object_type: ObjectName
    status_type: type[Enum]
    target_states: Sequence[Enum]
    failure_states: Sequence[Enum]


class YellowDogSensor(BaseSensorOperator):
    """
    Base class for YellowDog sensors. All arguments are keyword-only.

    :param connection_id: connection to run the sensor with (templated) or a
        Callable that generates the connection ID; defaults to
        ``yellowdog_default``
    :type connection_id: str | Callable
    :param poke_interval: the time between sensor checks in seconds (default = 60)
    :type poke_interval: float
    :param deferrable: if true, the sensor checks once, then defers to the
        triggerer, which polls every ``poke_interval`` seconds without
        occupying a worker slot; the wait is limited to ``timeout`` seconds
        from the start of the sensor. Defaults to Airflow's
        ``[operators] default_deferrable`` setting (false unless configured),
        read when the sensor is constructed. A deferrable sensor ignores
        ``mode="reschedule"`` (deferral takes precedence). Failure
        semantics when deferrable: ``soft_fail`` skips the task on a failure
        state or a timeout (not on other errors) and ``never_fail`` skips it
        on any of these or on any other error. A polling sensor differs on a
        timeout: Airflow's poke loop skips only with ``soft_fail`` and fails
        with ``never_fail``. ``silent_fail`` has no
        effect, because there is no later poke to retry: an error in the
        initial check, or one reported by the triggerer, fails the task.
    :type deferrable: bool | None

    Other keyword arguments are passed to Airflow's BaseSensorOperator
    (e.g., ``timeout``, ``mode``).
    """

    template_fields: Collection[str] = ("connection_id",)

    def __init__(
        self,
        *,
        connection_id: Templated[str] = YellowDogHook.default_conn_name,
        poke_interval: float = 60,  # Seconds
        deferrable: bool | None = None,
        **kwargs: Any,
    ) -> None:
        # Airflow's BaseSensorOperator.__init__ has untyped **kwargs
        super().__init__(  # pyright: ignore[reportUnknownMemberType]
            poke_interval=poke_interval, **kwargs
        )
        self.connection_id = connection_id
        self.deferrable: bool = (
            conf.getboolean("operators", "default_deferrable", fallback=False)
            if deferrable is None
            else deferrable
        )
        self.xcom_written: bool = False
        self._resolved: tuple[str, str | None, str | None] | None = None

    def get_hook(self) -> YellowDogHook:
        """Return a hook for the (rendered) connection ID."""
        return YellowDogHook(resolve(self.connection_id, "connection_id"))

    def _wait_spec(self) -> _WaitSpec:
        """What the sensor waits for (provided by each concrete sensor)."""
        raise NotImplementedError

    def execute(self, context: Context) -> Any:
        """
        If not deferrable, poll as Airflow's ``BaseSensorOperator`` does.
        Otherwise check once and, unless the target (or a failure) state is
        already reached, defer to a :class:`.YellowDogStateTrigger`.
        """
        if not self.deferrable:
            return super().execute(context)

        try:
            reached = self.poke(context)
        except AirflowSkipException:
            raise
        except YellowDogStateError as exc:
            if self.soft_fail or self.never_fail:
                raise AirflowSkipException(
                    f"Skipping due to {'soft_fail' if self.soft_fail else 'never_fail'}"
                    " being set to True"
                ) from exc
            raise
        except Exception as exc:
            if self.never_fail:
                raise AirflowSkipException(
                    "Skipping due to never_fail being set to True"
                ) from exc
            raise
        resolved = self._resolved
        if resolved is None:  # Defensive: poke() always records it
            raise YellowDogException("The sensor did not resolve its resource")
        object_id, namespace, name = resolved
        if reached:
            return object_id

        spec = self._wait_spec()
        timeout = float(self.timeout)
        self.defer(
            trigger=YellowDogStateTrigger(
                connection_id=resolve(self.connection_id, "connection_id"),
                kind=spec.kind,
                resource_id=object_id,
                target_states=[cast(str, s.value) for s in spec.target_states],
                failure_states=[cast(str, s.value) for s in spec.failure_states],
                poke_interval=self.poke_interval,
                end_time=time.time() + timeout,
            ),
            method_name="execute_complete",
            kwargs={"namespace": namespace, "name": name},
            timeout=timedelta(seconds=timeout),
        )

    def execute_complete(
        self,
        context: Context,
        event: dict[str, Any],
        namespace: str | None = None,
        name: str | None = None,
    ) -> str:
        """
        Resume after the trigger fires. Returns the resource ID if a target
        state was reached; raises :exc:`.YellowDogStateError` for a failure
        state, ``AirflowSensorTimeout`` on timeout and
        :exc:`.YellowDogException` for an error in the triggerer (see
        :class:`.YellowDogSensor` for ``soft_fail`` and ``never_fail``).

        :param event: the :class:`.YellowDogStateTrigger` event
        :param namespace: the resource's namespace, if known (set by ``execute``)
        :param name: the resource's name, if known (set by ``execute``)
        """
        status = event.get("status")
        resource_id = cast(str, event["resource_id"]) if "resource_id" in event else ""
        message = str(event.get("message", ""))
        if status == "target":
            self.log.info(message)
            return resource_id
        if status in ("failure", "timeout"):
            if self.soft_fail or self.never_fail:
                raise AirflowSkipException(
                    f"Skipping due to {'soft_fail' if self.soft_fail else 'never_fail'}"
                    f" being set to True: {message}"
                )
            if status == "timeout":
                raise AirflowSensorTimeout(message)
            spec = self._wait_spec()
            raise YellowDogStateError(
                kind=spec.object_type.value,
                resource_id=resource_id,
                current_state=spec.status_type(event["state"]),
                target_states=spec.target_states,
                namespace=namespace,
                name=name,
            )
        if self.never_fail:
            raise AirflowSkipException(
                f"Skipping due to never_fail being set to True: {message}"
            )
        error_type = event.get("error_type")
        if status == "error":
            raise YellowDogException(
                message if error_type is None else f"{message} ({error_type})"
            )
        raise YellowDogException(f"Unexpected trigger event status '{status}': {event}")

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

        self._resolved = (object_id, namespace, name)
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
    :param deferrable: defer to the triggerer after an initial check; see
        :class:`YellowDogSensor` (default: Airflow's ``default_deferrable`` setting)
    :type deferrable: bool | None
    """

    operator_extra_links: Collection[BaseOperatorLink] = (WorkRequirementPortalLink(),)

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
        _, resolved_failures = resolve_states(
            target_states,
            failure_states,
            WORK_REQUIREMENT_FINAL_STATES,
            "work requirement",
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
        self.failure_states: list[WorkRequirementStatus] = list(resolved_failures)

    def _wait_spec(self) -> _WaitSpec:
        return _WaitSpec(
            ResourceKind.WORK_REQUIREMENT,
            ObjectName.WORK_REQUIREMENT,
            WorkRequirementStatus,
            self.target_states,
            self.failure_states,
        )

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
    :param deferrable: defer to the triggerer after an initial check; see
        :class:`YellowDogSensor` (default: Airflow's ``default_deferrable`` setting)
    :type deferrable: bool | None
    """

    operator_extra_links: Collection[BaseOperatorLink] = (WorkerPoolPortalLink(),)

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
        _, resolved_failures = resolve_states(
            target_states, failure_states, WORKER_POOL_FINAL_STATES, "worker pool"
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
        self.failure_states: list[WorkerPoolStatus] = list(resolved_failures)

    def _wait_spec(self) -> _WaitSpec:
        return _WaitSpec(
            ResourceKind.WORKER_POOL,
            ObjectName.WORKER_POOL,
            WorkerPoolStatus,
            self.target_states,
            self.failure_states,
        )

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
    :param deferrable: defer to the triggerer after an initial check; see
        :class:`YellowDogSensor` (default: Airflow's ``default_deferrable`` setting)
    :type deferrable: bool | None
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
        _, resolved_failures = resolve_states(
            target_states,
            failure_states,
            COMPUTE_REQUIREMENT_FINAL_STATES,
            "compute requirement",
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
        self.failure_states: list[ComputeRequirementStatus] = list(resolved_failures)

    def _wait_spec(self) -> _WaitSpec:
        return _WaitSpec(
            ResourceKind.COMPUTE_REQUIREMENT,
            ObjectName.COMPUTE_REQUIREMENT,
            ComputeRequirementStatus,
            self.target_states,
            self.failure_states,
        )

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
