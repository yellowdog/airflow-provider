"""YellowDog Airflow provider exceptions."""

from collections.abc import Sequence
from enum import Enum
from typing import TYPE_CHECKING, Any

# YellowDogStateError's runtime base is the AirflowFailException that the
# running Airflow's task runner and sensors catch: airflow.sdk.exceptions in
# Airflow 3.4 (where the airflow.exceptions name is deprecated),
# airflow.exceptions in Airflow 3.1 (absent from airflow.sdk.exceptions). No
# single static import is valid on both, so type checkers see a stand-in.
if TYPE_CHECKING:

    class _AirflowFailBase(Exception):
        """Static stand-in; at runtime this is Airflow's AirflowFailException."""

else:
    try:
        from airflow.sdk.exceptions import AirflowFailException as _AirflowFailBase
    except ImportError:  # Airflow 3.1
        from airflow.exceptions import AirflowFailException as _AirflowFailBase


class YellowDogException(Exception):
    """Base class for errors raised by the YellowDog provider."""


class YellowDogArgumentError(YellowDogException, ValueError):
    """An operator or sensor argument is missing, malformed or inconsistent."""


class YellowDogStateError(YellowDogException, _AirflowFailBase):
    """
    A sensor found its resource in one of its failure states (by default, a
    final state that is not a target).

    It is also Airflow's ``AirflowFailException`` (at runtime, the class that the
    installed Airflow version's task runner and sensors catch), so Airflow fails
    the task without retrying it, and a sensor's ``silent_fail`` does not
    suppress it.

    :param kind: the kind of resource, e.g. ``"work requirement"``
    :param resource_id: the resource's ID
    :param current_state: the state the resource is in
    :param target_states: the states the sensor was waiting for
    :param namespace: the resource's namespace, if known
    :param name: the resource's name, if known
    """

    def __init__(
        self,
        kind: str,
        resource_id: str,
        current_state: Enum,
        target_states: Sequence[Enum],
        namespace: str | None = None,
        name: str | None = None,
    ) -> None:
        self.kind = kind
        self.resource_id = resource_id
        self.current_state = current_state
        self.target_states: tuple[Enum, ...] = tuple(target_states)
        self.namespace = namespace
        self.name = name
        label = (
            f"'{namespace}/{name}' (ID '{resource_id}')"
            if namespace is not None and name is not None
            else f"ID '{resource_id}'"
        )
        targets = ", ".join(f"'{state.value}'" for state in self.target_states)
        super().__init__(
            f"{kind.capitalize()} {label} reached status '{current_state.value}', "
            f"which is a failure state (target states: {targets})"
        )

    def __reduce__(self) -> tuple[Any, ...]:
        return (
            type(self),
            (
                self.kind,
                self.resource_id,
                self.current_state,
                self.target_states,
                self.namespace,
                self.name,
            ),
        )
