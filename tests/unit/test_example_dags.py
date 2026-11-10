import importlib.util
import re
from pathlib import Path
from types import ModuleType
from unittest.mock import patch

import pytest
from airflow.sdk import DAG
from yellowdog_client.platform_client import PlatformClient

import yellowdog_provider

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
    }


def test_images_id_uses_param_connection():
    path = next(p for p in EXAMPLE_DAGS if p.name == "yd-provider-test.py")
    module = _load(path)
    with patch.object(module, "YellowDogHook") as hook:
        images_client = hook.return_value.get_conn.return_value.images_client
        images_client.get_image_families.return_value.list_all.return_value = []
        module._get_images_id("ns/family", {"params": {"connection_id": "from-params"}})
    hook.assert_called_once_with("from-params")


def test_sensors_take_timeout_from_params():
    path = next(p for p in EXAMPLE_DAGS if p.name == "yd-provider-test.py")
    dag = _load(path).dag
    sensor = dag.task_dict["wait-for-work-requirement-1-to-finish"]
    assert sensor.timeout == 300.0
    hook = sensor._pre_execute_hook  # the task runner calls this before execute
    assert hook is not None
    hook({"task": sensor, "params": {"sensor_timeout_seconds": 42}})
    assert sensor.timeout == 42.0


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
