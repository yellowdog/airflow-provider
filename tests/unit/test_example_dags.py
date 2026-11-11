import copy
import importlib.util
import re
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest
from airflow.sdk import DAG
from yellowdog_client.model import WorkRequirementStatus
from yellowdog_client.platform_client import PlatformClient

import yellowdog_provider
from yellowdog_provider.sensors.yellowdog_sensors import WorkRequirementStateSensor

EXAMPLE_DAGS = sorted(
    (Path(yellowdog_provider.__file__).parent / "example_dags").glob("*.py")
)


def _load(path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(path.stem.replace("-", "_"), path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    with patch.object(
        PlatformClient, "create", side_effect=AssertionError("network used")
    ):
        spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("path", EXAMPLE_DAGS, ids=lambda p: p.name)
def test_example_dags_import(path: Path):
    module = _load(path)
    assert isinstance(module.dag, DAG)


def test_provider_test_dag_task_ids():
    path = next(p for p in EXAMPLE_DAGS if p.name == "yd-provider-test.py")
    assert set(_load(path).dag.task_dict) == {
        "provision-worker-pool",
        "add-work-requirement",
        "add-task-groups-to-work-requirement",
        "add-tasks-to-task-group-1",
        "add-tasks-to-task-group-2",
        "add-populated-work-requirement",
        "wait-for-work-requirement-1-to-finish",
        "wait-for-work-requirement-2-to-finish",
        "shutdown-worker-pool",
        "terminate-worker-pool",
        "wait-for-worker-pool-to-shut-down",
        "wait-for-compute-requirement-to-terminate",
        "cancel-work-requirement-1",
        "cancel-work-requirement-2",
    }


def test_provider_test_dag_params():
    path = next(p for p in EXAMPLE_DAGS if p.name == "yd-provider-test.py")
    assert set(_load(path).dag.params) == {
        "connection_id",
        "namespace",
        "compute_requirement_template",
        "image_family",
        "task_type",
        "task_arguments",
        "sensor_timeout_seconds",
        "user_data",
        "max_nodes",
        "poke_interval_seconds",
    }


def test_provider_test_dag_id_matches_e2e_test():
    from tests.system.test_example_dag_e2e import DAG_ID

    path = next(p for p in EXAMPLE_DAGS if p.name == "yd-provider-test.py")
    module = _load(path)
    assert module.DAG_ID == DAG_ID
    assert module.dag.dag_id == DAG_ID


def test_pwpp_max_nodes_from_param():
    path = next(p for p in EXAMPLE_DAGS if p.name == "yd-provider-test.py")
    module = _load(path)
    context = {
        "ti": SimpleNamespace(run_id="manual__2026-11-10T12:00:00.123456+00:00"),
        "params": {"max_nodes": 3},
    }
    assert module.gen_pwpp(context, None).maxNodes == 3


def test_images_id_uses_param_connection():
    path = next(p for p in EXAMPLE_DAGS if p.name == "yd-provider-test.py")
    module = _load(path)
    with patch.object(module, "YellowDogHook") as hook:
        conn = hook.return_value.__enter__.return_value.get_conn.return_value
        images_client = conn.images_client
        images_client.get_image_families.return_value.list_all.return_value = []
        module._get_images_id("ns/family", {"params": {"connection_id": "from-params"}})
    hook.assert_called_once_with("from-params")


@pytest.mark.parametrize(
    ("param", "expected"),
    [("#!/bin/bash\necho hi\n", "#!/bin/bash\necho hi\n"), ("", None), ("  \n", None)],
)
def test_crtu_user_data_from_param(param: str, expected: str | None):
    path = next(p for p in EXAMPLE_DAGS if p.name == "yd-provider-test.py")
    module = _load(path)
    context = {
        "ti": SimpleNamespace(run_id="manual__2026-11-10T12:00:00.123456+00:00"),
        "params": {
            "connection_id": "c",
            "namespace": "ns",
            "compute_requirement_template": "ns/t",
            "image_family": "ns/f",
            "user_data": param,
        },
    }
    with patch.object(module, "_get_images_id", return_value="images-id"):
        crtu = module.gen_crtu(context, None)
    assert crtu.userData == expected


def test_sensors_take_timeout_and_poke_interval_from_params():
    path = next(p for p in EXAMPLE_DAGS if p.name == "yd-provider-test.py")
    dag = _load(path).dag
    sensor = dag.task_dict["wait-for-work-requirement-1-to-finish"]
    assert sensor.timeout == 300.0
    assert sensor.poke_interval == 10.0
    hook = sensor._pre_execute_hook  # the task runner calls this before execute
    assert hook is not None
    params = {"sensor_timeout_seconds": 42, "poke_interval_seconds": 3}
    hook({"task": sensor, "ti": SimpleNamespace(task=sensor), "params": params})
    assert sensor.timeout == 42.0
    assert sensor.poke_interval == 3.0


def test_sensor_params_applied_to_executing_task_copy():
    # Airflow 3.1 runs a copy (ti.task = task.prepare_for_execution()) while
    # context["task"] still references the original; the hook must update ti.task
    path = next(p for p in EXAMPLE_DAGS if p.name == "yd-provider-test.py")
    dag = _load(path).dag
    original = dag.task_dict["wait-for-work-requirement-1-to-finish"]
    executing = copy.copy(original)
    assert executing is not original
    params = {"sensor_timeout_seconds": 42, "poke_interval_seconds": 3}
    original._pre_execute_hook(
        {"task": original, "ti": SimpleNamespace(task=executing), "params": params}
    )
    assert executing.timeout == 42.0
    assert executing.poke_interval == 3.0


def test_quickstart_work_requirement_name_is_valid():
    path = next(p for p in EXAMPLE_DAGS if p.name == "yd-quickstart.py")
    module = _load(path)
    context = {
        "run_id": "manual__2026-11-10T12:00:00.123456+00:00",
        "params": {"namespace": "ns", "worker_tag": "t"},
    }
    wr = module.make_work_requirement(context, None)
    assert wr.namespace == "ns"
    assert re.fullmatch(r"[a-z0-9_-]{1,60}", wr.name)
    long_ctx = {**context, "run_id": "x" * 200}
    assert re.fullmatch(
        r"[a-z0-9_-]{1,60}", module.make_work_requirement(long_ctx, None).name
    )


@pytest.mark.parametrize(
    ("params", "name"),
    [
        ({"max_nodes": 0}, "max_nodes"),
        ({"max_nodes": "abc"}, "max_nodes"),
        ({"max_nodes": None}, "max_nodes"),
        ({"max_nodes": float("nan")}, "max_nodes"),
        ({"max_nodes": float("inf")}, "max_nodes"),
        ({"max_nodes": 2.5}, "max_nodes"),
        ({"max_nodes": "2.5"}, "max_nodes"),
        ({"max_nodes": True}, "max_nodes"),
        ({"poke_interval_seconds": 0}, "poke_interval_seconds"),
        ({"poke_interval_seconds": -1}, "poke_interval_seconds"),
        ({"sensor_timeout_seconds": 0}, "sensor_timeout_seconds"),
        ({"poke_interval_seconds": float("nan")}, "poke_interval_seconds"),
        ({"poke_interval_seconds": float("inf")}, "poke_interval_seconds"),
        ({"sensor_timeout_seconds": float("-inf")}, "sensor_timeout_seconds"),
        ({"sensor_timeout_seconds": "nan"}, "sensor_timeout_seconds"),
        ({"sensor_timeout_seconds": "abc"}, "sensor_timeout_seconds"),
        ({"poke_interval_seconds": None}, "poke_interval_seconds"),
        ({"poke_interval_seconds": [3]}, "poke_interval_seconds"),
        ({"sensor_timeout_seconds": True}, "sensor_timeout_seconds"),
    ],
)
def test_invalid_numeric_params_rejected(params: dict[str, Any], name: str):
    from yellowdog_provider.exceptions.yellowdog_exceptions import (
        YellowDogArgumentError,
    )

    path = next(p for p in EXAMPLE_DAGS if p.name == "yd-provider-test.py")
    module = _load(path)
    valid = {
        "max_nodes": 1,
        "poke_interval_seconds": 10,
        "sensor_timeout_seconds": 300,
    }
    context = {
        "ti": SimpleNamespace(run_id="manual__2026-11-10T12:00:00.123456+00:00"),
        "params": {**valid, **params},
    }
    if name == "max_nodes":
        with pytest.raises(YellowDogArgumentError, match=name):
            module.gen_pwpp(context, None)
    else:
        sensor = module.dag.task_dict["wait-for-work-requirement-1-to-finish"]
        context["ti"] = SimpleNamespace(task=sensor)
        with pytest.raises(YellowDogArgumentError, match=name):
            module._apply_sensor_params(context)


@pytest.mark.parametrize("value", [3, 3.0, "3"])
def test_max_nodes_accepts_integral_values(value: Any):
    path = next(p for p in EXAMPLE_DAGS if p.name == "yd-provider-test.py")
    module = _load(path)
    context = {
        "ti": SimpleNamespace(run_id="manual__2026-11-10T12:00:00.123456+00:00"),
        "params": {"max_nodes": value},
    }
    assert module.gen_pwpp(context, None).maxNodes == 3


def _sensors(name: str) -> dict[str, WorkRequirementStateSensor]:
    path = next(p for p in EXAMPLE_DAGS if p.name == name)
    return {
        task_id: task
        for task_id, task in _load(path).dag.task_dict.items()
        if isinstance(task, WorkRequirementStateSensor)
    }


@pytest.mark.parametrize(
    ("name", "count"), [("yd-quickstart.py", 1), ("yd-provider-test.py", 2)]
)
def test_example_work_requirement_sensors_fail_on_failure(name: str, count: int):
    """Wait only for COMPLETED, so failed or cancelled work fails the DAG run."""
    sensors = _sensors(name)
    assert len(sensors) == count
    for sensor in sensors.values():
        assert sensor.target_states == [WorkRequirementStatus.COMPLETED]
        assert set(sensor.failure_states) == {
            WorkRequirementStatus.FAILED,
            WorkRequirementStatus.CANCELLED,
        }
