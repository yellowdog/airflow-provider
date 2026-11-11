Operators
=========

All operators live in ``yellowdog_provider.operators.yellowdog_operators``::

   from yellowdog_provider.operators.yellowdog_operators import (
       AddPopulatedWorkRequirement,
       AddTaskGroupsToWorkRequirement,
       AddTasksToTaskGroup,
       AddWorkRequirement,
       CancelWorkRequirement,
       ProvisionWorkerPool,
       ShutdownProvisionedWorkerPool,
   )

.. contents:: On this page
   :local:
   :depth: 1

Common behaviour
----------------

**Keyword-only arguments.** Every argument must be passed by name
(``AddWorkRequirement(task_id="x", work_requirement=wr)``). As well as the
arguments listed for each operator, all operators accept the standard Airflow
``BaseOperator`` arguments (``task_id``, ``retries``, ``trigger_rule`` and so
on). ``task_id`` (``str``) is required for every operator; like the other
``BaseOperator`` arguments, it is not repeated in the tables below.

**Connection.** Every operator takes ``connection_id`` (default
``yellowdog_default``); see :doc:`connections`.

**Reading the parameter tables.**

- *Jinja*: the argument is a templated field and a string value is rendered
  with Jinja before the task runs (``"{{ params.namespace }}"``,
  ``"{{ ti.xcom_pull(task_ids='submit') }}"``). Jinja inside SDK objects is
  *not* rendered: to build an object from run-time values, use a callable.
- *Callable*: the argument may be given a callable that Airflow calls when the
  task runs, with the keyword arguments ``context`` and ``jinja_env``, and
  whose return value is used. See :doc:`templating`.
- Arguments marked *no* in both columns are fixed when the DAG is parsed.

**Return values and XCom.** Each operator returns the YellowDog ID (or IDs) of
the resource it acted on. With Airflow's default ``do_xcom_push=True`` this is
stored as the task's XCom (key ``return_value``), so downstream tasks can use
``"{{ ti.xcom_pull(task_ids='<task_id>') }}"``.

**Examples.** The examples below assume these imports (and the operator
imports above), and this helper, which builds a YellowDog-safe name
(lower-case letters, digits, ``-`` and ``_``, at most 60 characters) that is
unique to the DAG run::

   import re
   from datetime import timedelta

   from airflow.sdk import TriggerRule
   from yellowdog_client.model import (
       AutoShutdown,
       ComputeRequirementTemplateUsage,
       ProvisionedWorkerPoolProperties,
       RunSpecification,
       Task,
       TaskGroup,
       WorkRequirement,
   )


   def unique_name(context, prefix):
       name = f"{prefix}-{context['run_id']}".lower()
       return re.sub(r"[^a-z0-9_-]", "-", name)[:60]

.. _id-or-name:

**Identifying an existing resource: ID or namespace and name.** Operators that
act on an existing resource accept either its YellowDog ID or its namespace and
name:

- You must supply the ID, or *all* of the name arguments. Otherwise a
  :class:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogArgumentError`
  is raised when the operator is constructed, which Airflow reports as a DAG
  import error.
- If both the ID and the name arguments are supplied, **the ID wins**: the name
  arguments are ignored. All operators except ``AddTasksToTaskGroup`` also
  log a warning (``Both '<id_field>' and namespace/name supplied ...;
  ignoring the name``).
- The check is repeated after templates are rendered. A templated argument
  that renders to an empty (or whitespace-only) string counts as not supplied.
  One that renders to the string ``"None"`` raises ``YellowDogArgumentError``,
  because that almost always means an ``xcom_pull`` found no value (see
  :ref:`none-ids`).
- Looking up a name that does not exist fails with the SDK's ``404`` error.

**Errors.** Errors returned by YellowDog are raised by the SDK and are not
caught (with the one exception noted for
:ref:`CancelWorkRequirement <cancel-work-requirement>`); they fail the Airflow
task, which is then retried according to its ``retries`` setting. See
:doc:`troubleshooting`.

AddWorkRequirement
------------------

Submits a work requirement. Task groups and tasks can then be added with
:ref:`AddTaskGroupsToWorkRequirement <add-task-groups>` and
:ref:`AddTasksToTaskGroup <add-tasks>`.

.. list-table::
   :header-rows: 1
   :widths: 24 26 18 10 10

   * - Parameter
     - Type
     - Default
     - Jinja
     - Callable
   * - ``work_requirement``
     - ``WorkRequirement``
     - required
     - n/a
     - yes
   * - ``connection_id``
     - ``str``
     - ``"yellowdog_default"``
     - yes
     - yes

**Returns:** the ID of the new work requirement (``str``).

**Fails if:** YellowDog rejects the work requirement, for example because a
work requirement with the same name already exists in the namespace (which
happens on a retry or re-run if the name does not vary per run).

Example::

   def make_wr(context, jinja_env):
       return WorkRequirement(
           namespace=context["params"]["namespace"],
           name=unique_name(context, "wr"),
       )

   add_wr = AddWorkRequirement(task_id="add-wr", work_requirement=make_wr)

.. _add-task-groups:

AddTaskGroupsToWorkRequirement
------------------------------

Adds task groups to an existing work requirement, keeping any task groups it
already has. Identify the work requirement by ID or by namespace and name
(:ref:`rules <id-or-name>`).

.. list-table::
   :header-rows: 1
   :widths: 24 26 18 10 10

   * - Parameter
     - Type
     - Default
     - Jinja
     - Callable
   * - ``task_groups``
     - ``list[TaskGroup]``
     - required
     - n/a
     - yes
   * - ``work_requirement_id``
     - ``str | None``
     - ``None``
     - yes
     - yes
   * - ``namespace``
     - ``str | None``
     - ``None``
     - yes
     - yes
   * - ``work_requirement_name``
     - ``str | None``
     - ``None``
     - yes
     - yes
   * - ``connection_id``
     - ``str``
     - ``"yellowdog_default"``
     - yes
     - yes

**Returns:** the IDs of the added task groups (``list[str]``), in the order the
task groups were supplied. Task groups that were already on the work
requirement are not included.

**Fails if:** the work requirement cannot be found, or YellowDog rejects the
update.

Example (the ID comes from the ``add-wr`` task above)::

   add_groups = AddTaskGroupsToWorkRequirement(
       task_id="add-groups",
       work_requirement_id="{{ ti.xcom_pull(task_ids='add-wr') }}",
       task_groups=[
           TaskGroup(
               name="group-1",
               runSpecification=RunSpecification(
                   taskTypes=["docker"], workerTags=["my-tag"]
               ),
           )
       ],
   )

.. _add-tasks:

AddTasksToTaskGroup
-------------------

Adds tasks to an existing task group. Identify the task group either by
``task_group_id``, or by *all* of ``namespace``, ``work_requirement_name`` and
``task_group_name`` (:ref:`rules <id-or-name>`).

.. list-table::
   :header-rows: 1
   :widths: 24 26 18 10 10

   * - Parameter
     - Type
     - Default
     - Jinja
     - Callable
   * - ``tasks``
     - ``list[Task]``
     - required
     - n/a
     - yes
   * - ``task_group_id``
     - ``str | None``
     - ``None``
     - yes
     - yes
   * - ``namespace``
     - ``str | None``
     - ``None``
     - yes
     - yes
   * - ``work_requirement_name``
     - ``str | None``
     - ``None``
     - yes
     - yes
   * - ``task_group_name``
     - ``str | None``
     - ``None``
     - yes
     - yes
   * - ``connection_id``
     - ``str``
     - ``"yellowdog_default"``
     - yes
     - yes

When both the ID and the names are given, the ID is used and the names are
ignored, without a warning.

**Returns:** the IDs of the added tasks (``list[str]``).

**Fails if:** the task group cannot be found, or YellowDog rejects the
tasks.

Example, using the first task group ID returned by ``add-groups``::

   add_tasks = AddTasksToTaskGroup(
       task_id="add-tasks",
       task_group_id="{{ ti.xcom_pull(task_ids='add-groups')[0] }}",
       tasks=[Task(name="task-1", taskType="docker", arguments=["hello-world"])],
   )

AddPopulatedWorkRequirement
---------------------------

Submits a work requirement together with its task groups and tasks, in one
Airflow task: it adds the work requirement, then its task groups, then each
group's tasks.

.. list-table::
   :header-rows: 1
   :widths: 24 26 18 10 10

   * - Parameter
     - Type
     - Default
     - Jinja
     - Callable
   * - ``work_requirement``
     - ``WorkRequirement``
     - required
     - n/a
     - yes
   * - ``task_groups_and_tasks``
     - ``list[tuple[TaskGroup, list[Task]]]``
     - required
     - n/a
     - yes
   * - ``connection_id``
     - ``str``
     - ``"yellowdog_default"``
     - yes
     - yes

``task_groups_and_tasks`` is a list of ``(task_group, tasks)`` pairs. The
``work_requirement`` itself must **not** already contain task groups (its
``taskGroups`` must be empty or ``None``): supply them through
``task_groups_and_tasks`` instead.

**Returns:** the ID of the new work requirement (``str``).

**Fails if:** the work requirement has preset ``taskGroups``
(``YellowDogArgumentError``, raised when the task runs, before anything is
sent to YellowDog), or YellowDog rejects any step. Because the work
requirement is created first, a failure in a later step leaves a partly
populated work requirement on the platform, and a retry with the same name
will then fail as a duplicate; see :doc:`troubleshooting`.

Example: see the :doc:`quickstart`.

.. _cancel-work-requirement:

CancelWorkRequirement
---------------------

Cancels a work requirement. Identify it by ID or by namespace and name
(:ref:`rules <id-or-name>`).

.. list-table::
   :header-rows: 1
   :widths: 24 26 18 10 10

   * - Parameter
     - Type
     - Default
     - Jinja
     - Callable
   * - ``work_requirement_id``
     - ``str | None``
     - ``None``
     - yes
     - yes
   * - ``namespace``
     - ``str | None``
     - ``None``
     - yes
     - yes
   * - ``work_requirement_name``
     - ``str | None``
     - ``None``
     - yes
     - yes
   * - ``abort_running_tasks``
     - ``bool``
     - ``False``
     - no
     - no
   * - ``connection_id``
     - ``str``
     - ``"yellowdog_default"``
     - yes
     - yes

**abort_running_tasks.** By default, tasks that have not started are
cancelled but tasks already running are allowed to finish before the
cancellation completes. With ``abort_running_tasks=True``, running tasks are
aborted as well.

**Already finished.** If YellowDog rejects the cancellation as an invalid state
transition (for example, the work requirement has already completed or been
cancelled), the operator logs a warning and succeeds. This makes it safe to
use as a teardown task with ``trigger_rule=TriggerRule.ALL_DONE``. Any other
error from YellowDog fails the task.

**Returns:** the ID of the work requirement (``str``).

Example::

   cancel = CancelWorkRequirement(
       task_id="cancel-wr",
       work_requirement_id="{{ ti.xcom_pull(task_ids='add-wr') }}",
       abort_running_tasks=True,
       trigger_rule=TriggerRule.ALL_DONE,
   )

ProvisionWorkerPool
-------------------

Provisions a worker pool, and the compute requirement behind it, from a
compute requirement template.

.. list-table::
   :header-rows: 1
   :widths: 24 26 18 10 10

   * - Parameter
     - Type
     - Default
     - Jinja
     - Callable
   * - ``compute_requirement_template_usage``
     - ``ComputeRequirementTemplateUsage``
     - required
     - n/a
     - yes
   * - ``provisioned_worker_pool_properties``
     - ``ProvisionedWorkerPoolProperties | None``
     - ``None``
     - n/a
     - yes
   * - ``connection_id``
     - ``str``
     - ``"yellowdog_default"``
     - yes
     - yes

``compute_requirement_template_usage`` names the template (``templateId``, an
ID or ``namespace/name``), the name and namespace of the new compute
requirement (``requirementName``, ``requirementNamespace``), the number of
instances (``targetInstanceCount``) and, optionally, the image family
(``imagesId``, the ID of an image family). Find an image family's ID in the
YellowDog Portal, or look it up by namespace and name with the SDK's
``client.images_client.get_image_families(MachineImageFamilySearch(...))``, as
the example DAG ``yd-provider-test.py`` does.
``provisioned_worker_pool_properties`` sets the pool's worker tag, node
limits, workers per node and idle shutdown timers; if omitted, YellowDog's
defaults apply.

**Returns:** the ID of the new worker pool (``str``). The pool may still be
starting when the operator returns; follow it with a
:ref:`WorkerPoolStateSensor <worker-pool-state-sensor>` waiting for ``IDLE``
or ``RUNNING`` if later tasks need the pool to be up.

**Fails if:** YellowDog rejects the request (for example, an unknown template
or image family).

**Instance userdata.** A script run when each instance boots is set with the
SDK's ``ComputeRequirementTemplateUsage(userData=...)`` field, which the
operator passes through unchanged; the example DAG ``yd-provider-test.py``
exposes it as its ``user_data`` param.

Example::

   def make_crtu(context, jinja_env):
       return ComputeRequirementTemplateUsage(
           templateId="my-namespace/my-template",
           requirementNamespace=context["params"]["namespace"],
           requirementName=unique_name(context, "pool"),
           targetInstanceCount=1,
           imagesId="<IMAGE_FAMILY_ID>",
       )

   provision = ProvisionWorkerPool(
       task_id="provision-pool",
       compute_requirement_template_usage=make_crtu,
       provisioned_worker_pool_properties=ProvisionedWorkerPoolProperties(
           workerTag="my-tag",
           idlePoolShutdown=AutoShutdown(enabled=True, timeout=timedelta(minutes=10)),
       ),
   )

ShutdownProvisionedWorkerPool
-----------------------------

Shuts down a provisioned worker pool: no further tasks are given to its
workers, and tasks already running are allowed to finish. Identify the pool by
ID or by namespace and name (:ref:`rules <id-or-name>`).

.. list-table::
   :header-rows: 1
   :widths: 24 26 18 10 10

   * - Parameter
     - Type
     - Default
     - Jinja
     - Callable
   * - ``worker_pool_id``
     - ``str | None``
     - ``None``
     - yes
     - yes
   * - ``namespace``
     - ``str | None``
     - ``None``
     - yes
     - yes
   * - ``worker_pool_name``
     - ``str | None``
     - ``None``
     - yes
     - yes
   * - ``terminate_immediately``
     - ``bool``
     - ``False``
     - no
     - no
   * - ``connection_id``
     - ``str``
     - ``"yellowdog_default"``
     - yes
     - yes

**terminate_immediately.** With ``terminate_immediately=True``, the pool's
compute requirement is also terminated straight away, so its instances are
stopped and any tasks still running on them fail.

**Returns:** the ID of the worker pool (``str``).

**Fails if:** the pool cannot be found (SDK ``404``), or the pool is not a
provisioned worker pool. A configured worker pool, or any other pool type,
raises
:class:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogException`
without changing the pool.

Example, as a teardown task::

   shutdown = ShutdownProvisionedWorkerPool(
       task_id="shutdown-pool",
       worker_pool_id="{{ ti.xcom_pull(task_ids='provision-pool') }}",
       trigger_rule=TriggerRule.ALL_DONE,
   )
