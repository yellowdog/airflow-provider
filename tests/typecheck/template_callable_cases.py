"""Static typing cases for TemplateCallable (checked by pyright, never run)."""

from airflow.sdk import Context
from jinja2 import Environment

from yellowdog_provider.utils.templating import TemplateCallable


def ok(context: Context, jinja_env: Environment) -> str:
    return "x"


def bad(ctx: Context, env: Environment) -> str:
    return "x"


good_callable: TemplateCallable[str] = ok
bad_callable: TemplateCallable[str] = bad  # pyright: ignore[reportAssignmentType]
