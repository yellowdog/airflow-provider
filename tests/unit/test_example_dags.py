import copy
import importlib.util
import re
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest
from airflow.sdk import DAG, TriggerRule
from yellowdog_client.model import WorkRequirementStatus
from yellowdog_client.platform_client import PlatformClient

import yellowdog_provider
from yellowdog_provider.operators.yellowdog_operators import (
    ProvisionWorkerPool,
    RunWorkRequirement,
    ShutdownProvisionedWorkerPool,
)

# YellowDog's naming rule
NAME_RE = re.compile(r"[a-z](?:[a-z0-9_-]{0,58}[a-z0-9])?")

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


def _main_dag():
    path = next(p for p in EXAMPLE_DAGS if p.name == "yd-provider-test.py")
    return _load(path).dag


def test_example_dag_three_task_shape():
    dag = _main_dag()
    assert set(dag.task_dict) == {
        "provision-worker-pool",
        "run-work-requirement-1",
        "run-work-requirement-2",
        "shutdown-worker-pool",
    }
    pool = dag.task_dict["provision-worker-pool"]
    stop = dag.task_dict["shutdown-worker-pool"]
    assert isinstance(pool, ProvisionWorkerPool)
    assert pool.is_setup and not pool.is_teardown
    assert pool.wait_until_ready is True
    assert isinstance(stop, ShutdownProvisionedWorkerPool)
    assert stop.is_teardown and not stop.is_setup
    assert stop.terminate_immediately is True
    assert stop.wait is True
    assert pool.downstream_task_ids == {
        "run-work-requirement-1",
        "run-work-requirement-2",
        "shutdown-worker-pool",
    }
    for run_id in ("run-work-requirement-1", "run-work-requirement-2"):
        run = dag.task_dict[run_id]
        assert isinstance(run, RunWorkRequirement)
        assert run.downstream_task_ids == {"shutdown-worker-pool"}
    assert stop.upstream_task_ids == {
        "provision-worker-pool",
        "run-work-requirement-1",
        "run-work-requirement-2",
    }


def test_teardown_uses_pool_id_from_xcom():
    stop = _main_dag().task_dict["shutdown-worker-pool"]
    assert stop.worker_pool_id == (
        "{{ ti.xcom_pull(task_ids='provision-worker-pool') }}"
    )
    assert stop.worker_pool_name is None
    assert stop.namespace is None


def test_pool_name_includes_dag_id():
    module = _load(next(p for p in EXAMPLE_DAGS if p.name == "yd-provider-test.py"))
    ti = SimpleNamespace(dag_id="dag-a", run_id="manual__2026-11-10T12:00:00")
    other = SimpleNamespace(dag_id="dag-b", run_id="manual__2026-11-10T12:00:00")
    name = module._get_wp_name({"ti": ti})
    assert "dag-a" in name and len(name) <= 60
    assert name != module._get_wp_name({"ti": other})


def test_example_dag_has_no_all_done_tasks():
    for example in EXAMPLE_DAGS:
        for task in _load(example).dag.task_dict.values():
            assert task.trigger_rule != TriggerRule.ALL_DONE, task.task_id


def test_example_dag_params_include_deferrable():
    params = _main_dag().params
    assert "deferrable" in params
    assert params["deferrable"] is False


def test_example_pool_has_idle_shutdown_backstop():
    module = _load(next(p for p in EXAMPLE_DAGS if p.name == "yd-provider-test.py"))
    context = {
        "ti": SimpleNamespace(run_id="manual__2026-11-10T12:00:00.123456+00:00"),
        "params": {"max_nodes": 1},
    }
    props = module.gen_pwpp(context, None)
    assert props.idlePoolShutdown is not None and props.idlePoolShutdown.enabled
    assert props.idleNodeShutdown is not None and props.idleNodeShutdown.enabled


def test_second_work_requirement_name_is_left_empty():
    module = _load(next(p for p in EXAMPLE_DAGS if p.name == "yd-provider-test.py"))
    context = {
        "ti": SimpleNamespace(run_id="manual__2026-11-10T12:00:00.123456+00:00"),
        "params": {"namespace": "ns"},
    }
    assert module.gen_work_requirement_2(context, None).name == ""


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
        "deferrable",
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
        "ti": SimpleNamespace(
            dag_id="d", run_id="manual__2026-11-10T12:00:00.123456+00:00"
        ),
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


WAITING_TASK_IDS = [
    "provision-worker-pool",
    "run-work-requirement-1",
    "run-work-requirement-2",
    "shutdown-worker-pool",
]
RUN_PARAMS: dict[str, Any] = {
    "sensor_timeout_seconds": 42,
    "poke_interval_seconds": 3,
    "deferrable": True,
}


@pytest.mark.parametrize("task_id", WAITING_TASK_IDS)
def test_waiting_tasks_take_run_params(task_id: str):
    task = _main_dag().task_dict[task_id]
    assert task.timeout == 300.0
    assert task.poke_interval == 10.0
    assert task.deferrable is False
    hook = task._pre_execute_hook  # the task runner calls this before execute
    assert hook is not None
    hook({"task": task, "ti": SimpleNamespace(task=task), "params": RUN_PARAMS})
    assert task.timeout == 42.0
    assert task.poke_interval == 3.0
    assert task.deferrable is True


@pytest.mark.parametrize("task_id", WAITING_TASK_IDS)
def test_run_params_applied_to_executing_task_copy(task_id: str):
    # Airflow 3.1 runs a copy (ti.task = task.prepare_for_execution()) while
    # context["task"] still references the original; the hook must update ti.task
    original = _main_dag().task_dict[task_id]
    executing = copy.copy(original)
    assert executing is not original
    original._pre_execute_hook(
        {"task": original, "ti": SimpleNamespace(task=executing), "params": RUN_PARAMS}
    )
    assert executing.timeout == 42.0
    assert executing.poke_interval == 3.0
    assert executing.deferrable is True


def test_quickstart_work_requirement_name_is_valid():
    path = next(p for p in EXAMPLE_DAGS if p.name == "yd-quickstart.py")
    module = _load(path)
    context = {
        "run_id": "manual__2026-11-10T12:00:00.123456+00:00",
        "params": {"namespace": "ns", "worker_tag": "t"},
    }
    wr = module.make_work_requirement(context, None)
    assert wr.namespace == "ns"
    assert NAME_RE.fullmatch(wr.name)
    for run_id in ("x" * 200, "x" * 49 + "-" * 20, "x" * 49 + "_" * 20):
        long_ctx = {**context, "run_id": run_id}
        name = module.make_work_requirement(long_ctx, None).name
        assert NAME_RE.fullmatch(name), name


@pytest.mark.parametrize(
    ("dag_id", "run_id"),
    [
        ("2026-dag", "manual__2026-11-10T12:00:00+00:00"),
        ("_private", "run"),
        ("yellowdog-provider-test", "systest-20261111-093633-b95a82-max"),
        # 60-character cuts that land on "-" or "_"
        ("yellowdog-provider-test", "max-main-systest-20261111-093633-b95a82"),
        ("d" * 30, "r" * 26 + "-" * 10 + "z"),
        ("d" * 30, "r" * 26 + "_" * 10 + "z"),
        ("dag", "manual__"),
        ("dag", "scheduled__2026-11-10T12:00:00+00:00"),
    ],
)
def test_example_dag_names_valid(dag_id: str, run_id: str):
    module = _load(next(p for p in EXAMPLE_DAGS if p.name == "yd-provider-test.py"))
    ctx = {"ti": SimpleNamespace(dag_id=dag_id, run_id=run_id)}
    for name in (
        module._get_wp_name(ctx),
        module._get_wr_name(ctx, module.FIRST_WR_PREFIX),
        module._get_worker_tag(ctx),
    ):
        assert NAME_RE.fullmatch(name), name


@pytest.mark.parametrize("raw", ["", "-", "_9", "9", "a" * 59 + "-b", "a" * 59 + "_"])
def test_normalise_yd_name_valid(raw: str):
    module = _load(next(p for p in EXAMPLE_DAGS if p.name == "yd-provider-test.py"))
    assert NAME_RE.fullmatch(module._normalise_yd_name(raw))


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
        ({"deferrable": "yes"}, "deferrable"),
        ({"deferrable": None}, "deferrable"),
        ({"deferrable": 1}, "deferrable"),
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
        "deferrable": False,
    }
    context = {
        "ti": SimpleNamespace(run_id="manual__2026-11-10T12:00:00.123456+00:00"),
        "params": {**valid, **params},
    }
    if name == "max_nodes":
        with pytest.raises(YellowDogArgumentError, match=name):
            module.gen_pwpp(context, None)
    else:
        task = module.dag.task_dict["run-work-requirement-1"]
        context["ti"] = SimpleNamespace(task=task)
        with pytest.raises(YellowDogArgumentError, match=name):
            module._apply_run_params(context)


@pytest.mark.parametrize("value", [3, 3.0, "3"])
def test_max_nodes_accepts_integral_values(value: Any):
    path = next(p for p in EXAMPLE_DAGS if p.name == "yd-provider-test.py")
    module = _load(path)
    context = {
        "ti": SimpleNamespace(run_id="manual__2026-11-10T12:00:00.123456+00:00"),
        "params": {"max_nodes": value},
    }
    assert module.gen_pwpp(context, None).maxNodes == 3


def _run_tasks(name: str) -> dict[str, RunWorkRequirement]:
    path = next(p for p in EXAMPLE_DAGS if p.name == name)
    return {
        task_id: task
        for task_id, task in _load(path).dag.task_dict.items()
        if isinstance(task, RunWorkRequirement)
    }


@pytest.mark.parametrize(
    ("name", "count"), [("yd-quickstart.py", 1), ("yd-provider-test.py", 2)]
)
def test_example_run_tasks_fail_on_failure(name: str, count: int):
    """Wait only for COMPLETED, so failed or cancelled work fails the DAG run."""
    tasks = _run_tasks(name)
    assert len(tasks) == count
    for task in tasks.values():
        assert task.target_states == [WorkRequirementStatus.COMPLETED]
        assert set(task.failure_states) == {
            WorkRequirementStatus.FAILED,
            WorkRequirementStatus.CANCELLED,
        }
