import asyncio
import importlib
import json
import threading
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import requests
from airflow.sdk import BaseHook, Connection
from airflow.triggers.base import TriggerEvent
from requests import HTTPError, Response
from yellowdog_client.model import (
    ApiKey,
    ComputeRequirementStatus,
    WorkerPoolStatus,
    WorkRequirementStatus,
)
from yellowdog_client.model.exceptions import (
    InternalServerException,
    InvalidRequestException,
    ServerErrorException,
)

import yellowdog_provider.triggers.yellowdog_triggers as trigger_module
from yellowdog_provider.exceptions.yellowdog_exceptions import (
    YellowDogArgumentError,
)
from yellowdog_provider.hooks.yellowdog_hooks import YellowDogHook
from yellowdog_provider.triggers.yellowdog_triggers import (
    ResourceKind,
    YellowDogStateTrigger,
)

CREATE = "yellowdog_provider.hooks.yellowdog_hooks.PlatformClient.create"
CLASSPATH = "yellowdog_provider.triggers.yellowdog_triggers.YellowDogStateTrigger"
START = 1_000_000.0

GETTERS = {
    ResourceKind.WORK_REQUIREMENT: "get_work_requirement_state",
    ResourceKind.WORKER_POOL: "get_worker_pool_state",
    ResourceKind.COMPUTE_REQUIREMENT: "get_compute_requirement_state",
}


class FakeClock:
    """A wall clock that only advances when the trigger sleeps."""

    def __init__(self, now: float = START) -> None:
        self.now = now
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.now

    async def sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        self.now += delay


CONN = Connection(
    conn_id="yd_test",
    conn_type="yellowdog",
    host="https://api.example.yellowdog",
    login="async-key",
    password="async-secret",
)


@pytest.fixture(autouse=True)
def aget_connection() -> Iterator[AsyncMock]:
    """
    The trigger fetches the connection in the event loop; a synchronous
    lookup (which in the triggerer would use supervisor comms from a worker
    thread) fails the test.
    """
    fetch = AsyncMock(return_value=CONN)
    with (
        patch.object(BaseHook, "aget_connection", fetch),
        patch.object(
            BaseHook,
            "get_connection",
            side_effect=AssertionError("get_connection called from the trigger"),
        ),
    ):
        yield fetch


@pytest.fixture
def clock() -> Iterator[FakeClock]:
    fake = FakeClock()
    with (
        patch.object(trigger_module, "time", SimpleNamespace(time=fake.time)),
        patch("asyncio.sleep", fake.sleep),
    ):
        yield fake


def make_trigger(**overrides: Any) -> YellowDogStateTrigger:
    kwargs: dict[str, Any] = {
        "connection_id": "yd_test",
        "kind": ResourceKind.WORK_REQUIREMENT,
        "resource_id": "ydid:workreq:1",
        "target_states": [WorkRequirementStatus.COMPLETED.value],
        "failure_states": [
            WorkRequirementStatus.FAILED.value,
            WorkRequirementStatus.CANCELLED.value,
        ],
        "poke_interval": 30.0,
        "end_time": START + 100.0,
    }
    kwargs.update(overrides)
    return YellowDogStateTrigger(**kwargs)


def collect(trigger: YellowDogStateTrigger) -> list[TriggerEvent]:
    async def run() -> list[TriggerEvent]:
        return [event async for event in trigger.run()]

    return asyncio.run(run())


def payload(
    status: str,
    state: str | None,
    message: str,
    kind: str = "work_requirement",
    resource_id: str = "ydid:workreq:1",
) -> dict[str, Any]:
    return {
        "status": status,
        "kind": kind,
        "resource_id": resource_id,
        "state": state,
        "message": message,
    }


def test_trigger_serialize_round_trip():
    trigger = make_trigger(
        kind=ResourceKind.WORKER_POOL,
        resource_id="ydid:wrkrpool:1",
        target_states=("IDLE", "RUNNING"),
        failure_states=("SHUTDOWN", "TERMINATED"),
        poke_interval=5,
        end_time=START + 0.5,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == CLASSPATH
    json.dumps(kwargs)  # Triggers are stored as JSON
    assert kwargs == {
        "connection_id": "yd_test",
        "kind": "worker_pool",
        "resource_id": "ydid:wrkrpool:1",
        "target_states": ["IDLE", "RUNNING"],
        "failure_states": ["SHUTDOWN", "TERMINATED"],
        "poke_interval": 5.0,
        "end_time": START + 0.5,
    }
    module_name, _, class_name = classpath.rpartition(".")
    cls = getattr(importlib.import_module(module_name), class_name)
    rebuilt = cls(**kwargs)
    assert isinstance(rebuilt, YellowDogStateTrigger)
    assert rebuilt.kind is ResourceKind.WORKER_POOL
    assert rebuilt.serialize() == (classpath, kwargs)


@pytest.mark.parametrize(
    ("kind", "state"),
    [
        (ResourceKind.WORK_REQUIREMENT, WorkRequirementStatus.COMPLETED),
        (ResourceKind.WORKER_POOL, WorkerPoolStatus.IDLE),
        (ResourceKind.COMPUTE_REQUIREMENT, ComputeRequirementStatus.TERMINATED),
    ],
)
def test_trigger_yields_target(clock: FakeClock, kind: ResourceKind, state: Any):
    trigger = make_trigger(
        kind=kind,
        resource_id="ydid:x:1",
        target_states=[state.value],
        failure_states=[],
    )
    with patch.object(YellowDogHook, GETTERS[kind], return_value=state) as getter:
        events = collect(trigger)
    assert [e.payload for e in events] == [
        payload(
            "target",
            state.value,
            f"Reached status '{state.value}'",
            kind=kind.value,
            resource_id="ydid:x:1",
        )
    ]
    getter.assert_called_once_with("ydid:x:1")
    assert clock.sleeps == []  # The first check is immediate


def test_trigger_yields_failure(clock: FakeClock):
    with patch.object(
        YellowDogHook,
        "get_work_requirement_state",
        side_effect=[WorkRequirementStatus.RUNNING, WorkRequirementStatus.FAILED],
    ):
        events = collect(make_trigger())
    assert [e.payload for e in events] == [
        payload("failure", "FAILED", "Reached failure status 'FAILED'")
    ]


def test_trigger_yields_timeout_at_end_time(clock: FakeClock):
    with patch.object(
        YellowDogHook,
        "get_work_requirement_state",
        return_value=WorkRequirementStatus.RUNNING,
    ) as getter:
        events = collect(make_trigger(poke_interval=30.0, end_time=START + 100.0))
    # 0, 30, 60, 90, then a last check at the deadline (100)
    assert clock.sleeps == [30.0, 30.0, 30.0, 10.0]
    assert getter.call_count == 5
    assert len(events) == 1
    event = events[0].payload
    assert event["status"] == "timeout"
    assert event["state"] == "RUNNING"
    assert "ydid:workreq:1" in event["message"]
    assert "RUNNING" in event["message"]


def test_trigger_past_end_time_checks_once_then_times_out(clock: FakeClock):
    with patch.object(
        YellowDogHook,
        "get_work_requirement_state",
        return_value=WorkRequirementStatus.RUNNING,
    ) as getter:
        events = collect(make_trigger(end_time=START - 1))
    assert getter.call_count == 1
    assert clock.sleeps == []
    assert [e.payload["status"] for e in events] == ["timeout"]


def test_trigger_past_end_time_still_reports_target(clock: FakeClock):
    with patch.object(
        YellowDogHook,
        "get_work_requirement_state",
        return_value=WorkRequirementStatus.COMPLETED,
    ):
        events = collect(make_trigger(end_time=START - 1))
    assert [e.payload["status"] for e in events] == ["target"]


def test_trigger_polls_at_interval(clock: FakeClock):
    with patch.object(
        YellowDogHook,
        "get_work_requirement_state",
        side_effect=[
            WorkRequirementStatus.HELD,
            WorkRequirementStatus.RUNNING,
            WorkRequirementStatus.COMPLETED,
        ],
    ) as getter:
        events = collect(make_trigger(poke_interval=7.5))
    assert clock.sleeps == [7.5, 7.5]
    assert getter.call_count == 3
    assert [e.payload["status"] for e in events] == ["target"]


def test_trigger_sdk_error_yields_error_event(clock: FakeClock):
    with patch.object(
        YellowDogHook,
        "get_work_requirement_state",
        side_effect=RuntimeError("platform unavailable"),
    ):
        events = collect(make_trigger())
    assert [e.payload for e in events] == [
        {
            **payload("error", None, "RuntimeError: platform unavailable"),
            "error_type": "RuntimeError",
        }
    ]
    assert clock.sleeps == []


def test_trigger_checks_status_in_thread_with_fresh_closed_hook(clock: FakeClock):
    threads: list[int] = []
    hooks: list[YellowDogHook] = []

    def get_state(self: YellowDogHook, resource_id: str) -> WorkRequirementStatus:
        threads.append(threading.get_ident())
        hooks.append(self)
        assert self.connection_id == "yd_test"
        return (
            WorkRequirementStatus.RUNNING
            if len(hooks) == 1
            else WorkRequirementStatus.COMPLETED
        )

    with (
        patch.object(YellowDogHook, "get_work_requirement_state", get_state),
        patch.object(YellowDogHook, "close", autospec=True) as close,
    ):
        collect(make_trigger())
    assert threading.get_ident() not in threads
    assert len(hooks) == 2 and hooks[0] is not hooks[1]
    assert [c.args[0] for c in close.call_args_list] == hooks


def test_trigger_kind_accepts_string():
    trigger = make_trigger(
        kind="compute_requirement", target_states=["TERMINATED"], failure_states=[]
    )
    assert trigger.kind is ResourceKind.COMPUTE_REQUIREMENT


@pytest.mark.parametrize(
    ("kind", "field", "states", "bad"),
    [
        (ResourceKind.WORK_REQUIREMENT, "target_states", ["COMPLETED", "DONE"], "DONE"),
        (ResourceKind.WORK_REQUIREMENT, "failure_states", ["IDLE"], "IDLE"),
        (ResourceKind.WORKER_POOL, "target_states", ["COMPLETED"], "COMPLETED"),
        (ResourceKind.COMPUTE_REQUIREMENT, "failure_states", ["stopped"], "stopped"),
    ],
)
def test_trigger_rejects_unknown_state(
    kind: ResourceKind, field: str, states: Any, bad: str
):
    kwargs: dict[str, Any] = {
        "kind": kind,
        "target_states": ["TERMINATED"],
        "failure_states": [],
    }
    if kind is ResourceKind.WORK_REQUIREMENT:
        kwargs["target_states"] = ["COMPLETED"]
    kwargs[field] = states
    with pytest.raises(YellowDogArgumentError) as info:
        make_trigger(**kwargs)
    message = str(info.value)
    assert f"'{bad}'" in message and kind.value in message and field in message


# -- connection lookup --------------------------------------------------------


def test_trigger_fetches_connection_once_in_event_loop(
    clock: FakeClock, aget_connection: AsyncMock
):
    states = [
        WorkRequirementStatus.RUNNING,
        WorkRequirementStatus.RUNNING,
        WorkRequirementStatus.COMPLETED,
    ]

    def get_state(self: YellowDogHook, resource_id: str) -> WorkRequirementStatus:
        self.get_conn()
        return states.pop(0)

    with (
        patch.object(YellowDogHook, "get_work_requirement_state", get_state),
        patch(CREATE) as create,
    ):
        events = collect(make_trigger(poke_interval=5))
    assert [e.payload["status"] for e in events] == ["target"]
    aget_connection.assert_awaited_once_with("yd_test")
    assert create.call_count == 3  # A fresh client per check ...
    for call in create.call_args_list:  # ... built from the fetched connection
        assert call.args[1] == ApiKey("async-key", "async-secret")


def test_trigger_transient_retries_reuse_fetched_connection(
    clock: FakeClock, aget_connection: AsyncMock
):
    hooks: list[YellowDogHook] = []

    def get_state(self: YellowDogHook, resource_id: str) -> WorkRequirementStatus:
        hooks.append(self)
        self.get_conn()
        if len(hooks) < 3:
            raise _http_error(503)
        return WorkRequirementStatus.COMPLETED

    with (
        patch.object(YellowDogHook, "get_work_requirement_state", get_state),
        patch(CREATE),
    ):
        events = collect(make_trigger(poke_interval=5))
    assert [e.payload["status"] for e in events] == ["target"]
    assert len(hooks) == 3
    aget_connection.assert_awaited_once_with("yd_test")


def test_trigger_connection_lookup_error_yields_error_event(
    clock: FakeClock, aget_connection: AsyncMock
):
    aget_connection.side_effect = RuntimeError("no such connection")
    with patch.object(YellowDogHook, "get_work_requirement_state") as getter:
        events = collect(make_trigger())
    getter.assert_not_called()
    assert [e.payload for e in events] == [
        {
            **payload("error", None, "RuntimeError: no such connection"),
            "error_type": "RuntimeError",
        }
    ]


# -- transient errors ---------------------------------------------------------


def _http_error(status: int) -> HTTPError:
    response = Response()
    response.status_code = status
    return HTTPError(f"{status} error", response=response)


def _patch_states(*steps: Any):
    return patch.object(
        YellowDogHook, "get_work_requirement_state", side_effect=list(steps)
    )


def test_trigger_tolerates_transient_then_completes(clock: FakeClock):
    with (
        _patch_states(
            WorkRequirementStatus.RUNNING,
            _http_error(502),
            requests.exceptions.ConnectionError("reset"),
            WorkRequirementStatus.COMPLETED,
        ),
        patch.object(YellowDogStateTrigger, "log", MagicMock()) as log,
    ):
        events = collect(make_trigger(poke_interval=5))
    assert [e.payload["status"] for e in events] == ["target"]
    assert clock.sleeps == [5, 5, 5]
    counts = [c.args[3:5] for c in log.warning.call_args_list]
    assert counts == [(1, 3), (2, 3)]


def test_trigger_fourth_consecutive_transient_error_yields_error(clock: FakeClock):
    with _patch_states(
        WorkRequirementStatus.RUNNING, *[_http_error(503) for _ in range(4)]
    ) as getter:
        events = collect(make_trigger(poke_interval=5))
    assert getter.call_count == 5
    assert clock.sleeps == [5, 5, 5, 5]
    assert [e.payload for e in events] == [
        {
            **payload("error", None, "HTTPError: 503 error"),
            "error_type": "HTTPError",
        }
    ]


def test_trigger_non_transient_error_yields_error_immediately(clock: FakeClock):
    with _patch_states(WorkRequirementStatus.RUNNING, _http_error(404)):
        events = collect(make_trigger(poke_interval=5))
    assert clock.sleeps == [5]
    assert events[0].payload["status"] == "error"
    assert events[0].payload["error_type"] == "HTTPError"


def test_trigger_transient_counter_resets_after_success(clock: FakeClock):
    e = requests.exceptions.Timeout
    with _patch_states(
        e("1"),
        e("2"),
        e("3"),
        WorkRequirementStatus.RUNNING,
        e("4"),
        e("5"),
        e("6"),
        WorkRequirementStatus.COMPLETED,
    ):
        events = collect(make_trigger(poke_interval=1))
    assert [e.payload["status"] for e in events] == ["target"]


def test_trigger_deadline_wins_over_transient_error(clock: FakeClock):
    with _patch_states(
        WorkRequirementStatus.RUNNING, WorkRequirementStatus.RUNNING, _http_error(500)
    ):
        events = collect(make_trigger(poke_interval=5, end_time=START + 10))
    assert clock.sleeps == [5, 5]
    assert [(e.payload["status"], e.payload["state"]) for e in events] == [
        ("timeout", "RUNNING")
    ]


def test_trigger_deadline_with_no_state_seen_yields_error(clock: FakeClock):
    with _patch_states(_http_error(500)):
        events = collect(make_trigger(end_time=START - 1))
    assert [e.payload["status"] for e in events] == ["error"]


def test_trigger_tolerates_sdk_server_errors(clock: FakeClock):
    with _patch_states(
        InternalServerException("platform error"),
        ServerErrorException(503, "unavailable"),
        WorkRequirementStatus.COMPLETED,
    ):
        events = collect(make_trigger(poke_interval=5))
    assert [e.payload["status"] for e in events] == ["target"]
    assert clock.sleeps == [5, 5]


def test_trigger_sdk_invalid_request_yields_error_immediately(clock: FakeClock):
    with _patch_states(InvalidRequestException("bad request")):
        events = collect(make_trigger())
    assert clock.sleeps == []
    assert events[0].payload["status"] == "error"
    assert events[0].payload["error_type"] == "InvalidRequestException"


# -- kill actions --------------------------------------------------------------


def _pool_trigger(**overrides: Any) -> YellowDogStateTrigger:
    return make_trigger(
        kind=ResourceKind.WORKER_POOL,
        resource_id="wp-1",
        target_states=["IDLE", "RUNNING"],
        failure_states=["SHUTDOWN", "TERMINATED"],
        **overrides,
    )


def test_trigger_kill_action_serialized() -> None:
    trigger = _pool_trigger(kill_action="terminate_worker_pool")
    classpath, kwargs = trigger.serialize()
    assert kwargs["kill_action"] == "terminate_worker_pool"
    json.dumps(kwargs)
    rebuilt = YellowDogStateTrigger(**kwargs)
    assert rebuilt.kill_action == "terminate_worker_pool"
    assert rebuilt.serialize() == (classpath, kwargs)
    assert "kill_action" not in make_trigger().serialize()[1]


@pytest.mark.parametrize(
    ("kind", "action"),
    [
        (ResourceKind.WORK_REQUIREMENT, "terminate_worker_pool"),
        (ResourceKind.WORK_REQUIREMENT, "explode"),
        (ResourceKind.COMPUTE_REQUIREMENT, "cancel_work_requirement"),
    ],
)
def test_trigger_rejects_unsuitable_kill_action(kind: ResourceKind, action: str):
    states = {
        ResourceKind.WORK_REQUIREMENT: ["COMPLETED"],
        ResourceKind.COMPUTE_REQUIREMENT: ["TERMINATED"],
    }[kind]
    with pytest.raises(YellowDogArgumentError, match="kill_action"):
        make_trigger(
            kind=kind, target_states=states, failure_states=[], kill_action=action
        )


def test_trigger_on_kill_cancels_work_requirement() -> None:
    threads: list[int] = []
    cancel = MagicMock(
        side_effect=lambda *a, **k: threads.append(threading.get_ident())
    )
    with (
        patch.object(YellowDogHook, "cancel_work_requirement", cancel),
        patch.object(YellowDogHook, "close") as close,
    ):
        asyncio.run(make_trigger(kill_action="cancel_work_requirement").on_kill())
    cancel.assert_called_once_with("ydid:workreq:1", abort=True)
    assert threads and threads[0] != threading.get_ident()
    close.assert_called_once()


def test_trigger_on_kill_terminates_worker_pool() -> None:
    with patch.object(YellowDogHook, "shutdown_worker_pool") as shutdown:
        asyncio.run(_pool_trigger(kill_action="terminate_worker_pool").on_kill())
    shutdown.assert_called_once_with("wp-1", terminate_immediately=True)


def test_trigger_on_kill_without_action_does_nothing() -> None:
    with (
        patch.object(YellowDogHook, "shutdown_worker_pool") as shutdown,
        patch.object(YellowDogHook, "cancel_work_requirement") as cancel,
    ):
        asyncio.run(_pool_trigger().on_kill())
        asyncio.run(make_trigger().on_kill())
    shutdown.assert_not_called()
    cancel.assert_not_called()


def test_trigger_on_kill_uses_connection_fetched_in_event_loop(
    aget_connection: AsyncMock,
) -> None:
    keys: list[ApiKey] = []

    def shutdown(self: YellowDogHook, *args: Any, **kwargs: Any) -> None:
        self.get_conn()

    with (
        patch.object(YellowDogHook, "shutdown_worker_pool", shutdown),
        patch(CREATE, side_effect=lambda _s, key: keys.append(key)),
    ):
        asyncio.run(_pool_trigger(kill_action="terminate_worker_pool").on_kill())
    aget_connection.assert_awaited_once_with("yd_test")
    assert keys == [ApiKey("async-key", "async-secret")]


def test_trigger_on_kill_without_action_fetches_nothing(
    aget_connection: AsyncMock,
) -> None:
    asyncio.run(make_trigger().on_kill())
    aget_connection.assert_not_awaited()


def test_trigger_on_kill_logs_connection_lookup_error(
    aget_connection: AsyncMock,
) -> None:
    aget_connection.side_effect = RuntimeError("no such connection")
    with (
        patch.object(YellowDogHook, "cancel_work_requirement") as cancel,
        patch.object(YellowDogStateTrigger, "log", MagicMock()) as log,
    ):
        asyncio.run(make_trigger(kill_action="cancel_work_requirement").on_kill())
    cancel.assert_not_called()
    log.exception.assert_called_once()


def test_trigger_on_kill_never_raises() -> None:
    with patch.object(
        YellowDogHook, "shutdown_worker_pool", side_effect=RuntimeError("down")
    ) as shutdown:
        asyncio.run(_pool_trigger(kill_action="terminate_worker_pool").on_kill())
    shutdown.assert_called_once()
