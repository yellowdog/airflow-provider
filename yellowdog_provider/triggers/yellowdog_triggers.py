"""
Airflow trigger used by the provider's deferrable waits: it polls the status of
a YellowDog work requirement, worker pool or compute requirement from the
triggerer until a target state, a failure state or a deadline is reached.
"""

import asyncio
import time
from collections.abc import AsyncIterator, Sequence
from enum import Enum, StrEnum
from typing import Any, Literal, get_args

from airflow.sdk import BaseHook, Connection
from airflow.triggers.base import BaseTrigger, TriggerEvent
from yellowdog_client.model import (
    ComputeRequirementStatus,
    WorkerPoolStatus,
    WorkRequirementStatus,
)

from yellowdog_provider.exceptions.yellowdog_exceptions import (
    YellowDogArgumentError,
)
from yellowdog_provider.hooks.yellowdog_hooks import YellowDogHook
from yellowdog_provider.utils.waiting import (
    MAX_CONSECUTIVE_TRANSIENT_ERRORS,
    is_transient_error,
)

TriggerStatus = Literal["target", "failure", "timeout", "error"]

KillAction = Literal["cancel_work_requirement", "terminate_worker_pool"]
"""
What a :class:`YellowDogStateTrigger` does to its resource when the deferred
task is killed by a user (see :meth:`YellowDogStateTrigger.on_kill`).
"""


class ResourceKind(StrEnum):
    """The kinds of YellowDog resource a :class:`YellowDogStateTrigger` polls."""

    WORK_REQUIREMENT = "work_requirement"
    WORKER_POOL = "worker_pool"
    COMPUTE_REQUIREMENT = "compute_requirement"


_STATUS_TYPES: dict[ResourceKind, type[Enum]] = {
    ResourceKind.WORK_REQUIREMENT: WorkRequirementStatus,
    ResourceKind.WORKER_POOL: WorkerPoolStatus,
    ResourceKind.COMPUTE_REQUIREMENT: ComputeRequirementStatus,
}


_KILL_ACTION_KINDS: dict[str, ResourceKind] = {
    "cancel_work_requirement": ResourceKind.WORK_REQUIREMENT,
    "terminate_worker_pool": ResourceKind.WORKER_POOL,
}


def _validate_states(kind: ResourceKind, field: str, states: Sequence[str]) -> None:
    allowed = [status.value for status in _STATUS_TYPES[kind]]
    for state in states:
        if state not in allowed:
            raise YellowDogArgumentError(
                f"'{field}' contains '{state}', which is not a {kind.value} "
                f"status; expected one of {', '.join(allowed)}"
            )


def _state_values(states: Sequence[str | Enum]) -> tuple[str, ...]:
    """The status values of ``states`` (enum members or values)."""
    return tuple(str(s.value) if isinstance(s, Enum) else s for s in states)


def event_error_message(event: dict[str, Any]) -> str:
    """
    The message of an ``"error"`` trigger event, followed by its
    ``error_type`` in parentheses when present.
    """
    message = str(event.get("message", ""))
    error_type = event.get("error_type")
    return message if error_type is None else f"{message} ({error_type})"


class YellowDogStateTrigger(BaseTrigger):
    """
    Poll a YellowDog resource's status every ``poke_interval`` seconds, and
    fire a single event when it reaches a target state, a failure state, or
    the ``end_time`` deadline. The first check is made straight away, and one
    is always made at the deadline before timing out.

    States are the status enums' ``.value`` strings, so that the trigger can be
    serialized. The Airflow connection is fetched once per run, in the event
    loop (with ``BaseHook.aget_connection``); each check then runs in a worker
    thread with its own hook (and client) built from that connection, created
    and closed for that check, so worker threads never look the connection up
    themselves (in the triggerer, that would use the supervisor's comms
    channel from outside the event loop, which Airflow 3.1 does not
    tolerate). If the connection lookup fails, the trigger ends with an
    ``"error"`` event. Errors from the check are not raised. Up to
    :data:`~yellowdog_provider.utils.waiting.MAX_CONSECUTIVE_TRANSIENT_ERRORS`
    consecutive transient errors (see
    :func:`~yellowdog_provider.utils.waiting.is_transient_error`) are logged
    and polling continues; the next one, or any other error, ends the trigger
    with an ``"error"`` event, and the deferred task fails when it resumes. If
    the deadline passes after a transient error, the event is ``"timeout"``
    (or ``"error"`` if no state was ever read).

    The event payload is a dict with the keys ``status`` (``"target"``,
    ``"failure"``, ``"timeout"`` or ``"error"``), ``kind`` (a
    :class:`ResourceKind` value), ``resource_id``, ``state`` (the last status
    value read, or ``None``) and ``message``; ``"error"`` events also have
    ``error_type`` (the exception's class name).

    :param connection_id: the Airflow connection ID for YellowDog
    :param kind: the kind of resource
    :param resource_id: the resource's YellowDog ID
    :param target_states: the statuses to wait for (status enum members or
        their values)
    :param failure_states: the statuses that end the wait as a failure
        (status enum members or their values)
    :param poke_interval: seconds between checks
    :param end_time: the deadline, in seconds since the epoch (wall clock,
        because the trigger may be resumed in another process)
    :param kill_action: what to do to the resource if the deferred task is
        killed by a user (marked failed or succeeded, or cleared):
        ``"cancel_work_requirement"`` cancels a work requirement, aborting its
        running tasks; ``"terminate_worker_pool"`` shuts down a worker pool
        and terminates its compute requirement; ``None`` (the default) does
        nothing. The action is best-effort (failures, including failing to
        fetch the connection, are logged). Airflow
        calls the trigger's ``on_kill`` from Airflow 3.3 (verified on 3.4); on
        earlier versions (3.1, 3.2) a deferred task killed by a user is not
        cleaned up. Airflow
        bounds ``on_kill`` by ``[triggerer] on_kill_timeout`` (default 30
        seconds); an action that takes longer is abandoned.
    :raises ~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogArgumentError:
        if a state is not a status of the resource's kind, or ``kill_action``
        is unknown or does not suit ``kind``
    """

    def __init__(
        self,
        *,
        connection_id: str,
        kind: ResourceKind | str,
        resource_id: str,
        target_states: Sequence[str | Enum],
        failure_states: Sequence[str | Enum],
        poke_interval: float,
        end_time: float,
        kill_action: KillAction | None = None,
    ) -> None:
        super().__init__()  # pyright: ignore[reportUnknownMemberType]
        self.connection_id: str = connection_id
        self.kind: ResourceKind = ResourceKind(kind)
        self.resource_id: str = resource_id
        self.target_states: tuple[str, ...] = _state_values(target_states)
        self.failure_states: tuple[str, ...] = _state_values(failure_states)
        self.poke_interval: float = float(poke_interval)
        self.end_time: float = float(end_time)
        _validate_states(self.kind, "target_states", self.target_states)
        _validate_states(self.kind, "failure_states", self.failure_states)
        if kill_action is not None and (
            kill_action not in get_args(KillAction)
            or _KILL_ACTION_KINDS[kill_action] is not self.kind
        ):
            allowed = [a for a, k in _KILL_ACTION_KINDS.items() if k is self.kind]
            raise YellowDogArgumentError(
                f"'kill_action' '{kill_action}' is not valid for a "
                f"{self.kind.value}; expected {' or '.join([*allowed, 'None'])}"
            )
        self.kill_action: KillAction | None = kill_action

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """
        Return the class path and the keyword arguments to rebuild this
        (``kill_action`` only if set).
        """
        kwargs: dict[str, Any] = {
            "connection_id": self.connection_id,
            "kind": self.kind.value,
            "resource_id": self.resource_id,
            "target_states": list(self.target_states),
            "failure_states": list(self.failure_states),
            "poke_interval": self.poke_interval,
            "end_time": self.end_time,
        }
        if self.kill_action is not None:
            kwargs["kill_action"] = self.kill_action
        return f"{type(self).__module__}.{type(self).__qualname__}", kwargs

    async def on_kill(self) -> None:
        """
        Run the ``kill_action``, if any, when the deferred task is killed by
        a user. Called by Airflow 3.3 and later (verified on 3.4); not by
        Airflow 3.1 or 3.2.
        Best-effort: failures are logged, never raised.
        """
        if self.kill_action is None:
            return
        self.log.warning(
            "Task killed while deferred: %s '%s'",
            self.kill_action.replace("_", " "),
            self.resource_id,
        )
        try:
            connection = await BaseHook.aget_connection(self.connection_id)
            await asyncio.to_thread(self._run_kill_action, connection)
        except Exception:
            self.log.exception(
                "Failed to %s '%s'",
                self.kill_action.replace("_", " "),
                self.resource_id,
            )

    def _hook(self, connection: Connection) -> YellowDogHook:
        """A hook that uses ``connection`` rather than looking it up."""
        return YellowDogHook(self.connection_id, connection=connection)

    def _run_kill_action(self, connection: Connection) -> None:
        """Run the kill action (blocking; run in a worker thread)."""
        with self._hook(connection) as hook:
            match self.kill_action:
                case "cancel_work_requirement":
                    hook.cancel_work_requirement(self.resource_id, abort=True)
                case "terminate_worker_pool":
                    hook.shutdown_worker_pool(
                        self.resource_id, terminate_immediately=True
                    )
                case None:
                    pass

    def _get_state(self, connection: Connection) -> str:
        """Read the resource's status (blocking; run in a worker thread)."""
        with self._hook(connection) as hook:
            match self.kind:
                case ResourceKind.WORK_REQUIREMENT:
                    return hook.get_work_requirement_state(self.resource_id).value
                case ResourceKind.WORKER_POOL:
                    return hook.get_worker_pool_state(self.resource_id).value
                case ResourceKind.COMPUTE_REQUIREMENT:
                    return hook.get_compute_requirement_state(self.resource_id).value

    def _event(
        self,
        status: TriggerStatus,
        state: str | None,
        message: str,
        **extra: str,
    ) -> TriggerEvent:
        return TriggerEvent(
            {
                "status": status,
                "kind": self.kind.value,
                "resource_id": self.resource_id,
                "state": state,
                "message": message,
                **extra,
            }
        )

    def _error(self, exc: Exception) -> TriggerEvent:
        error_type = type(exc).__name__
        return self._event("error", None, f"{error_type}: {exc}", error_type=error_type)

    def _timeout(self, state: str) -> TriggerEvent:
        return self._event(
            "timeout",
            state,
            f"Timed out waiting for {self.kind.value} '{self.resource_id}' "
            f"to reach {', '.join(self.target_states)}; its status is '{state}'",
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Poll until a target or failure state, the deadline or an error."""
        try:
            connection = await BaseHook.aget_connection(self.connection_id)
        except Exception as exc:
            yield self._error(exc)
            return
        last_state: str | None = None
        transient_errors = 0
        while True:
            try:
                state = await asyncio.to_thread(self._get_state, connection)
            except Exception as exc:
                if (
                    not is_transient_error(exc)
                    or transient_errors >= MAX_CONSECUTIVE_TRANSIENT_ERRORS
                ):
                    yield self._error(exc)
                    return
                transient_errors += 1
                self.log.warning(
                    "Transient error checking the status of %s '%s' (%d/%d): %s",
                    self.kind.value,
                    self.resource_id,
                    transient_errors,
                    MAX_CONSECUTIVE_TRANSIENT_ERRORS,
                    exc,
                )
                remaining = self.end_time - time.time()
                if remaining <= 0:
                    if last_state is None:
                        yield self._error(exc)
                    else:
                        yield self._timeout(last_state)
                    return
                await asyncio.sleep(min(self.poke_interval, remaining))
                continue
            transient_errors = 0
            last_state = state
            if state in self.target_states:
                yield self._event("target", state, f"Reached status '{state}'")
                return
            if state in self.failure_states:
                yield self._event("failure", state, f"Reached failure status '{state}'")
                return
            remaining = self.end_time - time.time()
            if remaining <= 0:
                yield self._timeout(state)
                return
            delay = min(self.poke_interval, remaining)
            self.log.info(
                "Current status of %s '%s' is '%s' ... waiting for %s seconds",
                self.kind.value,
                self.resource_id,
                state,
                delay,
            )
            await asyncio.sleep(delay)
