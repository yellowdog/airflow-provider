"""
Shared state resolution and a synchronous wait loop, used by the sensors and
the operators that wait for a resource to reach a state.
"""

import time
from collections.abc import Callable, Sequence
from enum import Enum

from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import HTTPError, Timeout
from yellowdog_client.model.exceptions import (
    InternalServerException,
    ServerErrorException,
)

from yellowdog_provider.exceptions.yellowdog_exceptions import (
    WaitTimeoutError,
    YellowDogArgumentError,
    YellowDogStateError,
)
from yellowdog_provider.utils.yellowdog_utils import SupportsLog

MAX_CONSECUTIVE_TRANSIENT_ERRORS = 3
"""
How many consecutive transient errors (see :func:`is_transient_error`) a wait
tolerates; the next one ends the wait.
"""


def is_transient_error(exc: BaseException) -> bool:
    """
    Whether ``exc`` is worth retrying on the next poll: a connection error, a
    request timeout, an HTTP error with status 429 or 5xx, or the SDK's
    server-side errors (``InternalServerException``, ``ServerErrorException``).
    Anything else (including other HTTP 4xx errors such as 404, and the SDK's
    other exceptions) is not.
    """
    if isinstance(
        exc,
        RequestsConnectionError
        | Timeout
        | InternalServerException
        | ServerErrorException,
    ):
        return True
    if isinstance(exc, HTTPError):
        response = exc.response
        if response is None:
            return False
        status = response.status_code
        return status >= 500 or status == 429
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


def resolve_states[S: Enum](
    target_states: Sequence[S],
    failure_states: Sequence[S] | None,
    final_states: Sequence[S],
    kind: str,
) -> tuple[tuple[S, ...], tuple[S, ...]]:
    """
    Validate the target and failure states and return ``(targets, failures)``.
    The failure states are as given or, if ``None``, every final state that is
    not a target state. The status type is that of ``final_states``.

    :param kind: the kind of resource (e.g. ``"work requirement"``), used in
        error messages
    :raises YellowDogArgumentError: if the targets are empty, of the wrong
        type, or overlap the failure states
    """
    if not target_states:
        raise YellowDogArgumentError(
            f"'target_states' must not be empty (for a {kind})"
        )
    status_type = type(final_states[0])
    _require_states("target_states", status_type, target_states)
    targets = tuple(target_states)
    if failure_states is None:
        return targets, tuple(s for s in final_states if s not in targets)
    _require_states("failure_states", status_type, failure_states)
    overlap = [state for state in failure_states if state in targets]
    if overlap:
        names = ", ".join(f"'{state.value}'" for state in overlap)
        raise YellowDogArgumentError(
            f"'target_states' and 'failure_states' overlap: {names}"
        )
    return targets, tuple(failure_states)


def wait_for_state[S: Enum](
    get_state: Callable[[], S],
    targets: Sequence[S],
    failures: Sequence[S],
    *,
    poke_interval: float,
    timeout: float,
    describe: Callable[[S], YellowDogStateError],
    log: SupportsLog,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    kind: str = "resource",
    resource_id: str | None = None,
) -> S:
    """
    Poll ``get_state`` every ``poke_interval`` seconds until it returns a
    target state (which is returned), a failure state (``describe(state)`` is
    raised) or ``timeout`` seconds have passed (:exc:`.WaitTimeoutError`).
    The state is always checked once more at the deadline before timing out.
    Up to :data:`MAX_CONSECUTIVE_TRANSIENT_ERRORS` consecutive transient errors
    from ``get_state`` (see :func:`is_transient_error`) are logged and the
    wait continues; the next one, or any other error, is raised. If the
    deadline passes after a transient error, :exc:`.WaitTimeoutError` is
    raised (or the error itself, if no state was ever read).
    ``kind`` and ``resource_id`` only label the timeout message.
    """
    start = clock()
    state: S | None = None  # The last state observed
    transient_errors = 0
    while True:
        try:
            state = get_state()
        except Exception as exc:
            if (
                not is_transient_error(exc)
                or transient_errors >= MAX_CONSECUTIVE_TRANSIENT_ERRORS
            ):
                raise
            transient_errors += 1
            log.warning(
                f"Transient error checking the status of {kind} "
                f"'{resource_id}' ({transient_errors}/"
                f"{MAX_CONSECUTIVE_TRANSIENT_ERRORS}): {exc}"
            )
            remaining = timeout - (clock() - start)
            if remaining <= 0:
                if state is None:
                    raise
                raise WaitTimeoutError(kind, resource_id, state, timeout) from exc
            sleep(min(poke_interval, remaining))
            continue
        transient_errors = 0
        if state in targets:
            log.info(f"Reached status '{state.value}'")
            return state
        if state in failures:
            raise describe(state)
        remaining = timeout - (clock() - start)
        if remaining <= 0:
            raise WaitTimeoutError(kind, resource_id, state, timeout)
        log.info(
            f"Current status is '{state.value}' ... waiting for "
            f"{min(poke_interval, remaining):g} seconds"
        )
        sleep(min(poke_interval, remaining))
