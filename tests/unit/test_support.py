"""Tests for the shared test helpers in tests/support.py."""

from typing import Any

from tests.support import make_context


def test_make_context_captures_xcom_push() -> None:
    pushed: dict[str, Any] = {}
    ctx = make_context(xcom={"t": 1}, pushed=pushed)
    ti: Any = ctx.get("ti")
    ti.xcom_push(key="return_value", value="obj-1")
    assert pushed == {"return_value": "obj-1"}
    assert ti.xcom_pull(task_ids="t") == 1


def test_make_context_xcom_push_without_capture() -> None:
    ti: Any = make_context().get("task_instance")
    ti.xcom_push(key="k", value="v")  # accepted, not recorded anywhere
    assert ti.xcom_pull(task_ids="t") is None
