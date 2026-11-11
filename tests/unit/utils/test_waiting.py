import logging
from enum import Enum

import pytest
import requests
from requests import HTTPError, Response
from yellowdog_client.model.exceptions import (
    InternalServerException,
    InvalidOperationException,
    InvalidRequestException,
    NotAuthorisedException,
    ServerErrorException,
)

from yellowdog_provider.exceptions.yellowdog_exceptions import (
    WaitTimeoutError,
    YellowDogArgumentError,
    YellowDogException,
    YellowDogStateError,
)
from yellowdog_provider.utils.waiting import (
    MAX_CONSECUTIVE_TRANSIENT_ERRORS,
    is_transient_error,
    resolve_states,
    wait_for_state,
)


class St(Enum):
    NEW = "new"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"


class Other(Enum):
    X = "x"


FINAL = (St.DONE, St.FAILED, St.CANCELLED)
LOG = logging.getLogger("test_waiting")


def _describe(state: St) -> YellowDogStateError:
    return YellowDogStateError("thing", "id-1", state, [St.DONE])


class FakeTime:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def _states(*states: St):
    it = iter(states)
    return lambda: next(it)


def test_resolve_states_default_failure_is_final_minus_targets():
    targets, failures = resolve_states([St.DONE], None, FINAL, "thing")
    assert targets == (St.DONE,)
    assert failures == (St.FAILED, St.CANCELLED)


def test_resolve_states_explicit_failures_and_empty():
    assert resolve_states([St.DONE], [St.FAILED], FINAL, "thing")[1] == (St.FAILED,)
    assert resolve_states([St.DONE], [], FINAL, "thing")[1] == ()


def test_resolve_states_overlap_rejected():
    with pytest.raises(YellowDogArgumentError, match="overlap"):
        resolve_states([St.DONE], [St.DONE], FINAL, "thing")


def test_resolve_states_empty_targets_and_wrong_type_rejected():
    with pytest.raises(YellowDogArgumentError, match="must not be empty"):
        resolve_states([], None, FINAL, "thing")
    with pytest.raises(YellowDogArgumentError, match="must contain only St"):
        resolve_states([Other.X], None, FINAL, "thing")
    with pytest.raises(YellowDogArgumentError, match="'failure_states'"):
        resolve_states([St.DONE], [Other.X], FINAL, "thing")


def test_wait_returns_on_target():
    t = FakeTime()
    result = wait_for_state(
        _states(St.NEW, St.RUNNING, St.DONE),
        (St.DONE,),
        (St.FAILED,),
        poke_interval=5,
        timeout=100,
        describe=_describe,
        log=LOG,
        sleep=t.sleep,
        clock=t.clock,
    )
    assert result is St.DONE
    assert t.sleeps == [5, 5]


def test_wait_raises_state_error_on_failure():
    t = FakeTime()
    with pytest.raises(YellowDogStateError, match="failed"):
        wait_for_state(
            _states(St.NEW, St.FAILED),
            (St.DONE,),
            (St.FAILED,),
            poke_interval=5,
            timeout=100,
            describe=_describe,
            log=LOG,
            sleep=t.sleep,
            clock=t.clock,
        )


def test_wait_times_out():
    t = FakeTime()
    with pytest.raises(WaitTimeoutError) as info:
        wait_for_state(
            lambda: St.RUNNING,
            (St.DONE,),
            (St.FAILED,),
            poke_interval=10,
            timeout=25,
            describe=_describe,
            log=LOG,
            sleep=t.sleep,
            clock=t.clock,
            kind="thing",
            resource_id="id-1",
        )
    message = str(info.value)
    assert "thing" in message and "id-1" in message
    assert "running" in message and "25" in message
    assert isinstance(info.value, YellowDogException)
    assert not isinstance(info.value, YellowDogStateError)
    assert sum(t.sleeps) <= 25


def test_wait_honours_poke_interval():
    t = FakeTime()
    wait_for_state(
        _states(St.NEW, St.NEW, St.NEW, St.DONE),
        (St.DONE,),
        (),
        poke_interval=7,
        timeout=1000,
        describe=_describe,
        log=LOG,
        sleep=t.sleep,
        clock=t.clock,
    )
    assert t.sleeps == [7, 7, 7]


def test_wait_sleep_never_exceeds_remaining_time():
    t = FakeTime()
    with pytest.raises(WaitTimeoutError):
        wait_for_state(
            lambda: St.NEW,
            (St.DONE,),
            (),
            poke_interval=10,
            timeout=15,
            describe=_describe,
            log=LOG,
            sleep=t.sleep,
            clock=t.clock,
        )
    assert t.sleeps == [10, 5]


# -- transient errors ---------------------------------------------------------


def _http_error(status: int) -> HTTPError:
    response = Response()
    response.status_code = status
    return HTTPError(f"{status} error", response=response)


def _script(*steps: St | Exception):
    """A get_state that returns states and raises exceptions in order."""
    it = iter(steps)

    def get_state() -> St:
        step = next(it)
        if isinstance(step, Exception):
            raise step
        return step

    return get_state


def _wait(get_state, t: FakeTime, timeout: float = 1000, **kwargs):
    return wait_for_state(
        get_state,
        (St.DONE,),
        (St.FAILED,),
        poke_interval=5,
        timeout=timeout,
        describe=_describe,
        log=LOG,
        sleep=t.sleep,
        clock=t.clock,
        kind="thing",
        resource_id="id-1",
        **kwargs,
    )


@pytest.mark.parametrize(
    "exc",
    [
        requests.exceptions.ConnectionError("reset"),
        requests.exceptions.Timeout("slow"),
        requests.exceptions.ReadTimeout("slow"),
        _http_error(500),
        _http_error(503),
        _http_error(429),
        InternalServerException("platform error"),
        ServerErrorException(500, "server error"),
    ],
)
def test_is_transient_error_true(exc: Exception):
    assert is_transient_error(exc)


@pytest.mark.parametrize(
    "exc",
    [
        _http_error(404),
        _http_error(400),
        _http_error(401),
        HTTPError("no response"),
        RuntimeError("boom"),
        YellowDogException("no status"),
        InvalidRequestException("bad request"),
        NotAuthorisedException("denied"),
        InvalidOperationException("nope"),
    ],
)
def test_is_transient_error_false(exc: Exception):
    assert not is_transient_error(exc)


def test_max_consecutive_transient_errors_is_three():
    assert MAX_CONSECUTIVE_TRANSIENT_ERRORS == 3


def test_wait_tolerates_transient_then_completes(caplog: pytest.LogCaptureFixture):
    t = FakeTime()
    get_state = _script(
        St.RUNNING, _http_error(502), requests.exceptions.ConnectionError("x"), St.DONE
    )
    with caplog.at_level(logging.WARNING, logger="test_waiting"):
        assert _wait(get_state, t) is St.DONE
    assert t.sleeps == [5, 5, 5]  # Normal poke interval after an error
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 2
    assert "1/3" in warnings[0] and "2/3" in warnings[1]


def test_wait_fourth_consecutive_transient_error_propagates():
    t = FakeTime()
    errors = [_http_error(500 + i) for i in range(4)]
    with pytest.raises(HTTPError) as info:
        _wait(_script(St.RUNNING, *errors), t)
    assert info.value is errors[3]
    assert t.sleeps == [5, 5, 5, 5]


def test_wait_non_transient_error_propagates_immediately():
    t = FakeTime()
    error = _http_error(404)
    with pytest.raises(HTTPError) as info:
        _wait(_script(St.RUNNING, error), t)
    assert info.value is error
    assert t.sleeps == [5]


def test_wait_transient_counter_resets_after_success():
    t = FakeTime()
    e = requests.exceptions.ConnectionError
    get_state = _script(
        e("1"), e("2"), e("3"), St.RUNNING, e("4"), e("5"), e("6"), St.DONE
    )
    assert _wait(get_state, t) is St.DONE


def test_wait_deadline_wins_over_transient_error():
    t = FakeTime()
    get_state = _script(St.RUNNING, St.RUNNING, _http_error(503))
    with pytest.raises(WaitTimeoutError) as info:
        _wait(get_state, t, timeout=10)
    assert info.value.last_state is St.RUNNING
    assert t.sleeps == [5, 5]


def test_wait_deadline_with_no_state_seen_reraises_transient_error():
    t = FakeTime()
    error = _http_error(503)
    with pytest.raises(HTTPError) as info:
        _wait(_script(error), t, timeout=0)
    assert info.value is error
