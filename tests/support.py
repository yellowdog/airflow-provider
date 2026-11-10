"""Helpers shared by unit and system tests."""

from typing import Any, cast

import jinja2.sandbox
from airflow.sdk import BaseOperator, Context


def render(op: BaseOperator, context: Context) -> None:
    """Render the operator's templated fields in place."""
    op.render_template_fields(context, jinja2.sandbox.SandboxedEnvironment())


def make_context(
    params: dict[str, Any] | None = None,
    xcom: dict[str, Any] | None = None,
) -> Context:
    """Build a minimal task context with a mocked task instance."""
    data: dict[str, Any] = xcom or {}

    class _TaskInstance:
        """Plain stand-in (a MagicMock is rejected by the Jinja sandbox)."""

        def xcom_pull(self, task_ids: Any = None, **_: Any) -> Any:
            if isinstance(task_ids, list):
                return [data.get(t) for t in task_ids]
            return data.get(task_ids)

    ti = _TaskInstance()
    return cast("Context", {"params": params or {}, "ti": ti, "task_instance": ti})
