"""Typed helpers for Airflow templated fields that may hold callables."""

from typing import Protocol

from airflow.sdk import Context
from jinja2 import Environment

from yellowdog_provider.exceptions.yellowdog_exceptions import (
    YellowDogArgumentError,
    YellowDogException,
)


class TemplateCallable[T](Protocol):
    """A callable Airflow invokes at render time to produce a field value.

    Airflow calls it as ``value(context=..., jinja_env=...)``, so the parameter
    names must be exactly ``context`` and ``jinja_env``.
    """

    def __call__(self, *, context: Context, jinja_env: Environment) -> T: ...


type Templated[T] = T | TemplateCallable[T]


def resolve[T](value: Templated[T], field_name: str) -> T:
    """Return a rendered templated field value, rejecting unrendered callables."""
    if callable(value):
        raise YellowDogException(
            f"Templated field '{field_name}' has not been rendered; "
            "a callable was supplied but never evaluated by Airflow"
        )
    return value


def resolve_optional_str(value: Templated[str] | None, field_name: str) -> str | None:
    """Normalise an optional string field: empty/None -> None, 'None' -> error."""
    if value is None:
        return None
    text = resolve(value, field_name).strip()
    if text == "":
        return None
    if text == "None":
        raise YellowDogArgumentError(
            f"Templated field '{field_name}' rendered to 'None'; the upstream XCom "
            "value is probably missing (did the upstream task fail or return nothing?)"
        )
    return text
