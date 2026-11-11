"""Do YellowDog SDK model objects round-trip through Airflow's XCom serialisation?

SDK_OBJECTS_VIA_XCOM = False

XCom values are serialised with ``serialize`` / ``deserialize`` from
``airflow.sdk.serde`` (Airflow 3.4) or ``airflow.serialization.serde``
(Airflow 3.1), exactly as ``BaseXCom.serialize_value`` / ``deserialize_value``
do. Results are identical on Airflow 3.1.0 and 3.4.0 (yellowdog-sdk 15.10.0):

Result on both versions (xfail = fails as shown):

====================================  ===========  ===========  ==========
Class                                 Airflow 3.1  Airflow 3.4  Cause
====================================  ===========  ===========  ==========
WorkRequirement (with taskGroups)     TypeError    TypeError    init=False
TaskGroup (with RunSpecification)     TypeError    TypeError    init=False
Task (arguments, environment)         TypeError    TypeError    init=False
ComputeRequirementTemplateUsage       ImportError  ImportError  allow-list
ProvisionedWorkerPoolProperties       TypeError    TypeError    enum serde
====================================  ===========  ===========  ==========

Causes:

* allow-list: deserialisation is refused unless the class matches
  ``[core] allowed_deserialization_classes`` (default ``airflow.*``). This is
  the only obstacle for ``ComputeRequirementTemplateUsage``; once allow-listed
  it round-trips (see ``test_round_trip_when_allow_listed``).
* init=False: classes with ``field(init=False)`` members (``id``, ``status``,
  ...) serialise via ``dataclasses.asdict`` including those fields, and
  deserialisation then fails in ``cls(**data)`` with ``unexpected keyword
  argument 'id'``, even when allow-listed. Not fixable by configuration.
* enum serde: SDK enums (e.g. ``NodeWorkerTargetType``) have no serde support,
  so serialisation fails with TypeError.

No wrappers are provided: operators must not accept SDK objects from upstream
XComs. The xfails are strict so a future Airflow or SDK that fixes this is
noticed. The allow-list is pinned inside the tests, so a developer's
``allowed_deserialization_classes`` setting cannot change the outcome.
"""

import importlib
import re
from collections.abc import Callable
from datetime import timedelta
from types import ModuleType
from typing import Any

import pytest
from yellowdog_client.model import (
    AutoShutdown,
    ComputeRequirementTemplateUsage,
    NodeWorkerTarget,
    ProvisionedWorkerPoolProperties,
    RunSpecification,
    Task,
    TaskGroup,
    WorkRequirement,
)


def _import_serde() -> ModuleType:
    try:
        return importlib.import_module("airflow.sdk.serde")  # Airflow 3.4
    except ImportError:
        return importlib.import_module("airflow.serialization.serde")  # Airflow 3.1


serde_module = _import_serde()
serialize: Callable[[object], Any] = serde_module.serialize
deserialize: Callable[[Any], Any] = serde_module.deserialize

SDK_OBJECTS_VIA_XCOM = False


def _run_spec() -> RunSpecification:
    return RunSpecification(
        taskTypes=["bash"], maxWorkers=2, taskTimeout=timedelta(minutes=5)
    )


OBJECTS: dict[str, Any] = {
    "WorkRequirement": WorkRequirement(
        namespace="ns",
        name="wr",
        taskGroups=[TaskGroup(name="tg", runSpecification=_run_spec())],
    ),
    "TaskGroup": TaskGroup(name="tg", runSpecification=_run_spec()),
    "Task": Task(
        name="t",
        taskType="bash",
        arguments=["-c", "echo hi"],
        environment={"A": "b"},
    ),
    "ComputeRequirementTemplateUsage": ComputeRequirementTemplateUsage(
        templateId="tmpl", requirementName="cr", userData="#!/bin/bash\necho hi"
    ),
    "ProvisionedWorkerPoolProperties": ProvisionedWorkerPoolProperties(
        createNodeWorkers=NodeWorkerTarget.per_node(1),
        idleNodeShutdown=AutoShutdown(enabled=True, timeout=timedelta(minutes=3)),
    ),
}


def _pin_allow_list(monkeypatch: pytest.MonkeyPatch, allow_sdk: bool) -> None:
    """Pin the deserialisation allow-list to the default, optionally plus the SDK."""
    patterns = [re.compile("airflow.*")]
    if allow_sdk:
        patterns.append(re.compile("yellowdog_client.*"))
    monkeypatch.setattr(serde_module, "_get_patterns", lambda: patterns)
    monkeypatch.setattr(serde_module, "_get_regexp_patterns", lambda: [])
    # Matching results are memoised: clear them now (fixture clears on teardown)
    for name in ("_match_glob", "_match_regexp"):
        getattr(serde_module, name).cache_clear()


@pytest.fixture
def pin_allow_list(monkeypatch: pytest.MonkeyPatch):
    def pin(allow_sdk: bool) -> None:
        _pin_allow_list(monkeypatch, allow_sdk)

    yield pin
    for name in ("_match_glob", "_match_regexp"):
        getattr(serde_module, name).cache_clear()


def _case(name: str, allow_sdk: bool, exc: type[Exception], reason: str) -> Any:
    return pytest.param(
        name,
        allow_sdk,
        id=name,
        marks=pytest.mark.xfail(strict=True, raises=exc, reason=reason),
    )


INIT_FALSE = "field(init=False) members are serialised but rejected by cls(**data)"


@pytest.mark.parametrize(
    ("name", "allow_sdk"),
    [
        _case("WorkRequirement", True, TypeError, INIT_FALSE),
        _case("TaskGroup", True, TypeError, INIT_FALSE),
        _case("Task", True, TypeError, INIT_FALSE),
        _case(
            "ComputeRequirementTemplateUsage",
            False,
            ImportError,
            "yellowdog_client is not in allowed_deserialization_classes by default",
        ),
        _case(
            "ProvisionedWorkerPoolProperties",
            True,
            TypeError,
            "SDK enums (NodeWorkerTargetType) have no serde support",
        ),
    ],
)
def test_round_trip(name: str, allow_sdk: bool, pin_allow_list: Any) -> None:
    pin_allow_list(allow_sdk)
    obj = OBJECTS[name]
    result = deserialize(serialize(obj))
    assert type(result) is type(obj)
    assert result == obj


def test_round_trip_when_allow_listed(pin_allow_list: Any) -> None:
    """The one SDK class that works, once allow-listed (control for the xfails)."""
    pin_allow_list(True)
    obj = OBJECTS["ComputeRequirementTemplateUsage"]
    result = deserialize(serialize(obj))
    assert type(result) is type(obj)
    assert result == obj
