"""
Test the YellowDog Provider.
"""

###############################################################################
# Adjust the following to alter identity and behaviour

# The defaults below seed the DAG's Airflow params, which can be overridden per
# run (e.g. `--conf '{"namespace": "x"}'`). All of them are read from the
# run's params at run time.

# The YellowDog Application credentials to use, contained in an Airflow
# Connection.
CONNECTION_ID = "prod-yellowdog-yd-demo"  # Param: connection_id

# The compute requirement template and image family to use for the worker pool.
# Must match items in the selected YellowDog account.
CRT_NAME_OR_ID = "yd-demo/yd-demo-aws-gcp-azure-oci-split-ondemand"
IMAGE_FAMILY_NAME_OR_ID = "yellowdog/yd-agent-docker"

# Optional userdata script for the worker pool's instances (empty means none).
# Note: userdata passed through this DAG param travels in the DAG-run conf (visible
# on an `airflow dags trigger --conf` command line and stored in Airflow's DB),
# so don't put secrets in it.
USER_DATA = ""  # Param: user_data

# Namespace for work requirements and worker pool
NAMESPACE = "airflow-testing"  # Param: namespace

# Worker pool scaling properties
TARGET_INSTANCES = 1
MAX_NODES = 1  # Param: max_nodes
NODE_SHUTDOWN_TIMER_MINUTES = 2
POOL_SHUTDOWN_TIMER_MINUTES = 2

# Work requirements
TASK_GROUP_NAME_PREFIX = "task-group"
TASK_NAME = "task_1"
TASK_TYPE = "docker"  # Param: task_type
TASK_ARGS = ["hello-world"]  # Param: task_arguments
FIRST_WR_PREFIX = "a-"

# Polling interval and timeout for the tasks that wait (provisioning the pool,
# running work, shutting the pool down), and whether they defer to the
# triggerer while waiting. These are `__init__` arguments of the operators, not
# templated, so the tasks start with these values and `_apply_run_params`
# replaces them with the run's `poke_interval_seconds`, `sensor_timeout_seconds`
# and `deferrable` params just before each task executes (the operators decide
# whether to defer at execute time). The hook updates `context["ti"].task`, the
# task object actually executing: Airflow 3.1 runs a copy of the task
# (`prepare_for_execution()`) while `context["task"]` still refers to the
# original, so changing `context["task"]` would have no effect there.
POKE_INTERVAL_SECONDS = 10.0  # Param: poke_interval_seconds
SENSOR_TIMEOUT_SECONDS = 300.0  # Param: sensor_timeout_seconds
DEFERRABLE = False  # Param: deferrable

# The name of the DAG in Airflow
DAG_ID = "yellowdog-provider-test"

###############################################################################

import math
from datetime import timedelta
from re import sub
from typing import Any, cast

from airflow.sdk import (
    BaseOperator,
    Context,
    dag,  # pyright: ignore[reportUnknownVariableType]  # Airflow's @dag overloads are partially untyped
)
from airflow.sdk.types import RuntimeTaskInstanceProtocol
from jinja2 import Environment
from yellowdog_client.common import SearchClient
from yellowdog_client.model import (
    AutoShutdown,
    ComputeRequirementTemplateUsage,
    MachineImageFamilySearch,
    MachineImageFamilySummary,
    NodeWorkerTarget,
    NodeWorkerTargetType,
    ProvisionedWorkerPoolProperties,
    RunSpecification,
    Task,
    TaskGroup,
    WorkRequirement,
    WorkRequirementStatus,
)

from yellowdog_provider.exceptions.yellowdog_exceptions import YellowDogArgumentError
from yellowdog_provider.hooks.yellowdog_hooks import YellowDogHook
from yellowdog_provider.operators.yellowdog_operators import (
    ProvisionWorkerPool,
    RunWorkRequirement,
    ShutdownProvisionedWorkerPool,
)

###############################################################################
# Define the DAG


@dag(
    dag_id=DAG_ID,
    params={
        "connection_id": CONNECTION_ID,
        "namespace": NAMESPACE,
        "compute_requirement_template": CRT_NAME_OR_ID,
        "image_family": IMAGE_FAMILY_NAME_OR_ID,
        "task_type": TASK_TYPE,
        "task_arguments": TASK_ARGS,
        "sensor_timeout_seconds": SENSOR_TIMEOUT_SECONDS,
        "poke_interval_seconds": POKE_INTERVAL_SECONDS,
        "max_nodes": MAX_NODES,
        "user_data": USER_DATA,
        "deferrable": DEFERRABLE,
    },
)
def yd_provider_test():

    # Setup: provision the worker pool and wait until it is ready. As a setup
    # task, its teardown below runs however the work tasks end. If this task
    # itself fails, it shuts down its own pool, and Airflow does not run the
    # teardown.
    provision_worker_pool = ProvisionWorkerPool(
        task_id="provision-worker-pool",
        connection_id="{{ params.connection_id }}",
        compute_requirement_template_usage=gen_crtu,
        provisioned_worker_pool_properties=gen_pwpp,
        wait_until_ready=True,
        poke_interval=POKE_INTERVAL_SECONDS,
        timeout=SENSOR_TIMEOUT_SECONDS,
        pre_execute=_apply_run_params,
    ).as_setup()

    # Work: two work requirements, running on the pool in parallel. The first
    # is named explicitly; the second leaves its name empty, so it is named
    # after the DAG, task and run.
    run_work_requirement_1 = RunWorkRequirement(
        task_id="run-work-requirement-1",
        connection_id="{{ params.connection_id }}",
        work_requirement=gen_work_requirement_1,
        task_groups_and_tasks=gen_task_groups_and_tasks,
        # Fails at once if the work requirement fails or is cancelled
        target_states=[WorkRequirementStatus.COMPLETED],
        poke_interval=POKE_INTERVAL_SECONDS,
        timeout=SENSOR_TIMEOUT_SECONDS,
        pre_execute=_apply_run_params,
    )

    run_work_requirement_2 = RunWorkRequirement(
        task_id="run-work-requirement-2",
        connection_id="{{ params.connection_id }}",
        work_requirement=gen_work_requirement_2,
        task_groups_and_tasks=gen_task_groups_and_tasks,
        target_states=[WorkRequirementStatus.COMPLETED],
        poke_interval=POKE_INTERVAL_SECONDS,
        timeout=SENSOR_TIMEOUT_SECONDS,
        pre_execute=_apply_run_params,
    )

    # Teardown: always runs once the work is done, however it ended. It also
    # cancels the unfinished work requirements of the tasks in its scope (the
    # setup and the two work tasks), and waits for the pool and its compute
    # requirement to terminate.
    shut_down_worker_pool = ShutdownProvisionedWorkerPool(
        task_id="shutdown-worker-pool",
        connection_id="{{ params.connection_id }}",
        # The pool's ID, not its name: a name could match another run's pool
        worker_pool_id="{{ ti.xcom_pull(task_ids='provision-worker-pool') }}",
        terminate_immediately=True,
        wait=True,
        poke_interval=POKE_INTERVAL_SECONDS,
        timeout=SENSOR_TIMEOUT_SECONDS,
        pre_execute=_apply_run_params,
    ).as_teardown(setups=provision_worker_pool)

    _ = provision_worker_pool >> [run_work_requirement_1, run_work_requirement_2]
    _ = [run_work_requirement_1, run_work_requirement_2] >> shut_down_worker_pool


###############################################################################
# Object generator functions; these are used as Callable arguments for the
# YellowDog operator and sensor classes


def gen_namespace(context: Context, jinja_env: Environment) -> str:
    """
    Example of acquiring the 'namespace' directly from the context.
    """
    return _params(context)["namespace"]


def gen_work_requirement_1(context: Context, jinja_env: Environment) -> WorkRequirement:
    """
    Example using simple jinja templating to acquire the 'namespace'.
    """
    task = context.get("task")  # pyright: ignore[reportUnknownMemberType]  # Airflow Context.get overloads are partially untyped
    assert isinstance(task, BaseOperator)
    return WorkRequirement(
        namespace=task.render_template("{{ params.namespace }}", context, jinja_env),
        name=_get_wr_name(context, FIRST_WR_PREFIX),
    )


def gen_work_requirement_2(context: Context, jinja_env: Environment) -> WorkRequirement:
    return WorkRequirement(
        namespace=gen_namespace(context, Environment()),
        name="",  # Left empty: named automatically after the DAG, task and run
    )


def gen_crtu(
    context: Context, jinja_env: Environment
) -> ComputeRequirementTemplateUsage:
    return ComputeRequirementTemplateUsage(
        requirementName=_get_wp_name(context),
        targetInstanceCount=TARGET_INSTANCES,
        templateId=_params(context)["compute_requirement_template"],
        imagesId=_get_images_id(_params(context)["image_family"], context),
        requirementNamespace=gen_namespace(context, Environment()),
        userData=_get_user_data(context),
    )


def _get_user_data(context: Context) -> str | None:
    user_data: str = _params(context).get("user_data") or ""
    return user_data if user_data.strip() else None


def gen_pwpp(
    context: Context, jinja_env: Environment
) -> ProvisionedWorkerPoolProperties:
    max_nodes = _positive_int_param(_params(context), "max_nodes")
    if max_nodes < TARGET_INSTANCES:
        raise YellowDogArgumentError(
            f"Param 'max_nodes' must be >= {TARGET_INSTANCES} (got {max_nodes})"
        )
    return ProvisionedWorkerPoolProperties(
        createNodeWorkers=NodeWorkerTarget(
            targetCount=1, targetType=NodeWorkerTargetType.PER_VCPU
        ),
        # Safety net: if a deferred setup task is killed on Airflow 3.1 or 3.2
        # (no kill hook for deferred tasks), or its worker crashes while it
        # waits, nothing else shuts the pool down, so
        # the pool and its nodes shut themselves down when idle.
        idleNodeShutdown=AutoShutdown(
            enabled=True, timeout=timedelta(minutes=NODE_SHUTDOWN_TIMER_MINUTES)
        ),
        idlePoolShutdown=AutoShutdown(
            enabled=True, timeout=timedelta(minutes=POOL_SHUTDOWN_TIMER_MINUTES)
        ),
        minNodes=0,
        maxNodes=max_nodes,
        workerTag=_get_worker_tag(context),
    )


def gen_task_groups_and_tasks(
    context: Context, jinja_env: Environment
) -> list[tuple[TaskGroup, list[Task]]]:
    run_specification = RunSpecification(
        taskTypes=[_params(context)["task_type"]], workerTags=[_get_worker_tag(context)]
    )
    task_group_1 = TaskGroup(
        name=TASK_GROUP_NAME_PREFIX + "_1", runSpecification=run_specification
    )
    task_group_2 = TaskGroup(
        name=TASK_GROUP_NAME_PREFIX + "_2", runSpecification=run_specification
    )
    task = Task(
        name=TASK_NAME,
        taskType=_params(context)["task_type"],
        arguments=_params(context)["task_arguments"],
    )
    return [(task_group_1, [task]), (task_group_2, [task])]


###############################################################################
# Generate unique names using the Airflow run ID from the context


def _params(context: Context) -> dict[str, Any]:
    """
    The DAG params for this run (defaults overlaid with any `--conf`).
    """
    return cast("dict[str, Any]", context)["params"]


def _positive_param(params: dict[str, Any], name: str) -> float:
    """
    A numeric param that must be a finite number greater than zero.
    """
    raw = params[name]
    value = _number_param(name, raw)
    if not math.isfinite(value) or value <= 0:
        raise YellowDogArgumentError(
            f"Param '{name}' must be a finite number > 0 (got {raw!r})"
        )
    return value


def _positive_int_param(params: dict[str, Any], name: str) -> int:
    """
    A numeric param that must be a whole number greater than zero.
    """
    raw = params[name]
    value = _number_param(name, raw)
    if not math.isfinite(value) or not value.is_integer() or value <= 0:
        raise YellowDogArgumentError(
            f"Param '{name}' must be a whole number > 0 (got {raw!r})"
        )
    return int(value)


def _number_param(name: str, raw: Any) -> float:
    # bool is an int subclass, but True/False is never a meaningful number here
    if isinstance(raw, bool):
        raise YellowDogArgumentError(f"Param '{name}' must be a number (got {raw!r})")
    try:
        return float(raw)
    except (TypeError, ValueError):
        raise YellowDogArgumentError(
            f"Param '{name}' must be a number (got {raw!r})"
        ) from None


def _apply_run_params(context: Context) -> None:
    """
    `pre_execute` hook: take the task's timeout, poke interval and deferrable
    setting from the run's params.
    """
    # ti.task, not context["task"]: see the note above SENSOR_TIMEOUT_SECONDS
    task = cast("dict[str, Any]", context)["ti"].task
    assert isinstance(task, BaseOperator)
    params = _params(context)
    deferrable = params["deferrable"]
    if not isinstance(deferrable, bool):
        raise YellowDogArgumentError(
            f"Param 'deferrable' must be true or false (got {deferrable!r})"
        )
    # Validate everything before changing anything
    timeout = _positive_param(params, "sensor_timeout_seconds")
    poke_interval = _positive_param(params, "poke_interval_seconds")
    task.timeout = timeout
    task.poke_interval = poke_interval
    task.deferrable = deferrable


def _get_wr_name(context: Context, prefix: str = "") -> str:
    return _get_entity_name(context, prefix + "wr-airflow")


def _get_wp_name(context: Context) -> str:
    # Includes the DAG ID so that copies of this DAG name their pools distinctly
    task_instance: RuntimeTaskInstanceProtocol | None = context.get("ti")  # pyright: ignore[reportUnknownMemberType]  # Airflow Context.get overloads are partially untyped
    assert task_instance is not None
    return _get_entity_name(context, f"wp-{task_instance.dag_id}")


def _get_worker_tag(context: Context) -> str:
    return _get_entity_name(context, "tag-")


def _get_entity_name(context: Context, prefix: str) -> str:
    task_instance: RuntimeTaskInstanceProtocol | None = context.get("ti")  # pyright: ignore[reportUnknownMemberType]  # Airflow Context.get overloads are partially untyped
    assert task_instance is not None
    # Drop "manual__" before normalising, so the result is still valid
    run_id = task_instance.run_id.replace("manual__", "")
    return _normalise_yd_name(f"{prefix}-{run_id}")


def _normalise_yd_name(name: str) -> str:
    """
    Format a string to be consistent with YellowDog naming requirements: at
    most 60 characters, only lower-case letters, digits, underscore or
    hyphen, starting with a letter and ending with a letter or digit.
    """
    # Make obvious substitutions
    new_yd_name = name.replace("/", "-").replace(" ", "_").replace(".", "_").lower()

    # Enforce acceptable characters
    new_yd_name = sub("[^a-z0-9_-]", "", new_yd_name).strip("-_")

    # Must start with a letter
    if not new_yd_name[:1].isalpha():
        new_yd_name = "x-" + new_yd_name if new_yd_name else "x"

    # Mustn't exceed 60 chars, nor end with "-" or "_" once shortened
    return new_yd_name[:60].rstrip("-_")


###############################################################################
# YellowDog Utilities


def _get_images_id(image_family_name: str, context: Context) -> str:
    """
    Convert an image family namespace/name into an ID.
    Return the original name if not found. The connection comes from the run's
    `connection_id` param.
    """
    namespace_and_name = image_family_name.split("/")
    if len(namespace_and_name) != 2:
        return image_family_name

    namespace, name = namespace_and_name
    image_family_search = MachineImageFamilySearch(
        namespace=namespace, familyName=name, includePublic=True
    )
    with YellowDogHook(_params(context)["connection_id"]) as hook:
        client = hook.get_conn()
        search_client: SearchClient[MachineImageFamilySummary] = (
            client.images_client.get_image_families(image_family_search)
        )
        image_families: list[MachineImageFamilySummary] = search_client.list_all()

    # Partial names will match, so find an exact match
    for image_family in image_families:
        if image_family.name == name:
            return image_family.id or image_family_name

    return image_family_name


###############################################################################
# Run the DAG

dag = yd_provider_test()

###############################################################################
