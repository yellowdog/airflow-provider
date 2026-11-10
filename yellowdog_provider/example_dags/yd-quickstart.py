"""Quickstart: run one task on an existing YellowDog worker pool. Needs the Airflow
Connection `yellowdog_default` and a running pool whose workers carry `worker_tag`."""

from re import sub

from airflow.sdk import DAG, Context, chain
from airflow.sdk.definitions.param import ParamsDict
from jinja2 import Environment
from yellowdog_client.model import (
    RunSpecification,
    Task,
    TaskGroup,
    WorkRequirement,
    WorkRequirementStatus,
)

from yellowdog_provider.operators.yellowdog_operators import AddPopulatedWorkRequirement
from yellowdog_provider.sensors.yellowdog_sensors import WorkRequirementStateSensor


# Airflow calls these at run time (parameter names must be exactly these).
def make_work_requirement(context: Context, jinja_env: Environment) -> WorkRequirement:
    assert "params" in context and "run_id" in context
    # YellowDog names allow only a-z, 0-9, "_" and "-", up to 60 characters.
    name = sub(r"[^a-z0-9_-]", "-", f"quickstart-{context['run_id']}".lower())
    return WorkRequirement(namespace=context["params"]["namespace"], name=name[:60])


def make_groups_and_tasks(
    context: Context, jinja_env: Environment
) -> list[tuple[TaskGroup, list[Task]]]:
    assert "params" in context
    tag = context["params"]["worker_tag"]
    spec = RunSpecification(taskTypes=["docker"], workerTags=[tag])
    task = Task(name="task-1", taskType="docker", arguments=["hello-world"])
    return [(TaskGroup(name="group-1", runSpecification=spec), [task])]


_FINAL = [WorkRequirementStatus.COMPLETED, WorkRequirementStatus.FAILED]
dag: DAG = DAG(
    dag_id="yellowdog-quickstart",
    # worker_tag must match the tag of an existing, running worker pool
    params=ParamsDict({"namespace": "airflow-quickstart", "worker_tag": "my-tag"}),
)
with dag:
    # Submit a work requirement, then poll until it reaches a final state.
    chain(
        AddPopulatedWorkRequirement(
            task_id="submit",
            work_requirement=make_work_requirement,
            task_groups_and_tasks=make_groups_and_tasks,
        ),
        WorkRequirementStateSensor(
            task_id="wait",
            work_requirement_id="{{ ti.xcom_pull(task_ids='submit') }}",
            target_states=_FINAL,
            poke_interval=30,
            timeout=30 * 60,
        ),
    )
