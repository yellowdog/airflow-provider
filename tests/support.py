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
    pushed: dict[str, Any] | None = None,
    dag_id: str = "test_dag",
    run_id: str = "test_run",
    task_id: str = "test_task",
    map_index: int | None = -1,
    try_number: int = 1,
    max_tries: int = 0,
) -> Context:
    """
    Build a minimal task context with a stand-in task instance. ``xcom`` maps
    task IDs to the values ``xcom_pull`` returns; values pushed with
    ``xcom_push`` are stored in ``pushed`` (by key) if given. ``try_number``
    and ``max_tries`` are the task instance's (Airflow's) attempt counters.
    """
    data: dict[str, Any] = xcom or {}
    store: dict[str, Any] = {} if pushed is None else pushed

    class _TaskInstance:
        """Plain stand-in (a MagicMock is rejected by the Jinja sandbox)."""

        def __init__(self) -> None:
            self.dag_id = dag_id
            self.run_id = run_id
            self.task_id = task_id
            self.map_index = map_index
            self.try_number = try_number
            self.max_tries = max_tries

        def xcom_pull(self, task_ids: Any = None, **_: Any) -> Any:
            if isinstance(task_ids, list):
                return [data.get(t) for t in task_ids]
            return data.get(task_ids)

        def xcom_push(self, key: str, value: Any) -> None:
            store[key] = value

    ti = _TaskInstance()
    return cast("Context", {"params": params or {}, "ti": ti, "task_instance": ti})
