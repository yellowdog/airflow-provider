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
       RunWorkRequirement,
       ShutdownProvisionedWorkerPool,
   )

For most DAGs you need three of them: ``ProvisionWorkerPool`` as a setup
task, ``RunWorkRequirement`` for the work, and ``ShutdownProvisionedWorkerPool``
as the teardown. :doc:`workflow` explains how they fit together, and what
they do about deferral, retries and clean-up; this page lists every argument.

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
``"{{ ti.xcom_pull(task_ids='<task_id>') }}"``. Operators that wait
(``RunWorkRequirement``, ``ProvisionWorkerPool`` with ``wait_until_ready``,
``ShutdownProvisionedWorkerPool`` with ``wait``) also push the ID to XCom
before they start waiting, whatever ``do_xcom_push`` is.

**Waiting and deferral.** The waiting operators take ``poke_interval``,
``timeout`` and ``deferrable``. With ``deferrable=True`` they check once, then
hand the wait to Airflow's triggerer, freeing the worker slot;
``deferrable`` defaults to Airflow's ``[operators] default_deferrable``
setting. See :ref:`deferral`.

**Portal links.** ``RunWorkRequirement``, ``AddWorkRequirement`` and
``AddPopulatedWorkRequirement`` show a *YellowDog work requirement* link, and
``ProvisionWorkerPool`` and ``ShutdownProvisionedWorkerPool`` a *YellowDog
worker pool* link, to the resource in the YellowDog Portal; see
:doc:`connections`.

**Examples.** The examples below assume these imports (and the operator
imports above), and this helper, which builds a valid YellowDog name (at most
60 characters, only lower-case letters, digits, ``_`` or ``-``, starting with
a letter and ending with a letter or digit) that is unique to the DAG run;
``prefix`` must start with a letter::

   import re
   from datetime import timedelta

   from yellowdog_client.model import (
       AutoShutdown,
       ComputeRequirementTemplateUsage,
       ProvisionedWorkerPoolProperties,
       RunSpecification,
       Task,
       TaskGroup,
       WorkRequirement,
       WorkRequirementStatus,
   )


   def unique_name(context, prefix):
       name = f"{prefix}-{context['run_id']}".lower()
       # Ends with a letter or digit even when shortened
       return re.sub(r"[^a-z0-9_-]", "-", name)[:60].rstrip("-_")

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
  One that renders to the string ``"None"`` raises :exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogArgumentError`,
  because that almost always means an ``xcom_pull`` found no value (see
  :ref:`none-ids`).
- Looking up a name that does not exist fails with the SDK's ``404`` error.

**Errors.** Errors returned by YellowDog are raised by the SDK and fail the
Airflow task, which is then retried according to its ``retries`` setting. The
provider tolerates only these: a cancellation, pool shutdown or compute
requirement termination that YellowDog rejects because it has already
happened (an "invalid transition"); and, while an operator waits, up to three
consecutive :ref:`transient errors <transient-errors>`. See
:doc:`troubleshooting`.

RunWorkRequirement
------------------

Submits a work requirement with its task groups and tasks, then waits until
it reaches a target state (by default ``COMPLETED``). It fails at once if the
work requirement reaches a failure state (by default ``FAILED`` or
``CANCELLED``). It is the operator to use for most work: it is safe to retry,
and it cancels its work requirement if it gives up. See :doc:`workflow`.

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
   * - ``target_states``
     - ``Sequence[WorkRequirementStatus]``
     - ``(COMPLETED,)``
     - no
     - no
   * - ``failure_states``
     - ``Sequence[WorkRequirementStatus] | None``
     - ``None``: the final states (``COMPLETED``, ``FAILED``,
       ``CANCELLED``) not in ``target_states``
     - no
     - no
   * - ``poke_interval``
     - ``float`` (seconds)
     - ``60``
     - no
     - no
   * - ``timeout``
     - ``float`` (seconds)
     - ``604800`` (7 days)
     - no
     - no
   * - ``deferrable``
     - ``bool | None``
     - ``None``: Airflow's ``[operators] default_deferrable``
     - no
     - no
   * - ``connection_id``
     - ``str``
     - ``"yellowdog_default"``
     - yes
     - yes

``work_requirement`` must not contain task groups; supply them, in order, as
``(task_group, tasks)`` pairs in ``task_groups_and_tasks``. If its ``name`` is
empty, a name is derived from the DAG, run and task (for a mapped task, leave
it empty: derived names are unique per map index, whereas concurrent map
indices with the same fixed name can all fail on the duplicate). Its ``tag`` is replaced
by the run stamp. ``target_states`` and ``failure_states`` follow the same
rules as the sensors' (:ref:`failure states <failure-states>`): non-empty
targets, no overlap, ``WorkRequirementStatus`` values only, and ``[]``
disables failure detection.

**Returns:** the ID of the work requirement (``str``), which is also pushed to
XCom as soon as it is submitted.

**Retries and kill:** submission is retry-safe. A retry resumes this task's
earlier work requirement if it is unfinished, returns it if it completed
(unless the task instance was cleared since), and otherwise submits a fresh
copy named ``<name>-try<N>``. A work requirement with the same name from
another task or run is never touched. If the wait times out or fails with an
error, or the task is killed, the work requirement is cancelled, aborting its
running tasks; so is a work requirement whose submission fails partway; a
task killed while deferred is cleaned up on Airflow 3.3 and later only. See :ref:`safe-retries` and :ref:`cancellation`.

**Fails if:** the work requirement reaches a failure state
(:exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogStateError`,
not retried); the wait times out
(:exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.WaitTimeoutError`,
retried); ``work_requirement`` has preset task groups
(:exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogArgumentError`);
or YellowDog returns an error.

Example::

   def make_wr(context, jinja_env):
       return WorkRequirement(namespace=context["params"]["namespace"], name="")

   def make_groups_and_tasks(context, jinja_env):
       spec = RunSpecification(taskTypes=["docker"], workerTags=["my-tag"])
       task = Task(name="task-1", taskType="docker", arguments=["hello-world"])
       return [(TaskGroup(name="group-1", runSpecification=spec), [task])]

   run = RunWorkRequirement(
       task_id="run",
       work_requirement=make_wr,
       task_groups_and_tasks=make_groups_and_tasks,
       target_states=[WorkRequirementStatus.COMPLETED],
       timeout=6 * 3600,
       deferrable=True,
   )

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

**Retries and kill:** the work requirement is tagged with the task's run
stamp. A retry (or re-run after a clear) returns this task's earlier work
requirement if it is unfinished or completed in the current clear cycle;
otherwise a new one is added. If the name is held by a work requirement of
another task or run, the new one gets a suffixed name (``<name>-try<N>``, with
a short hash if needed) rather than failing; downstream operators that look
the work requirement up by namespace and name should therefore use the ID
from XCom instead. If the task is killed, the work
requirement is cancelled, aborting running tasks (best effort).

**Fails if:** YellowDog rejects the work requirement.

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

**Retries and kill:** the groups are appended to the work requirement's
current ones in a single update, so a retry after that update succeeded (but
the task still failed) fails on the duplicate group names. Nothing is
cancelled if the task is killed.

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

**Retries and kill:** tasks are added without checking for earlier ones, so a
retry after a partial failure may add some tasks twice. Use
``AddPopulatedWorkRequirement`` for
retry-safe submission. Nothing is cancelled if the task is killed.

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
(:exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogArgumentError`, raised when the task runs, before anything is
sent to YellowDog), or YellowDog rejects any step.

**Retries and kill:** submission is retry-safe, exactly as for
:class:`~yellowdog_provider.operators.yellowdog_operators.RunWorkRequirement`: the work requirement is
tagged with the task's run stamp, a retry resumes this task's earlier, fully
built work requirement or submits a fresh copy named ``<name>-try<N>`` (never
failing on a duplicate name), and a work requirement with the same name
submitted by another task or run is left alone. If the task is killed, its
work requirement is cancelled, aborting running tasks (best effort).

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
use as a teardown task. Any other error from YellowDog fails the task.
(``ShutdownProvisionedWorkerPool`` already cancels the unfinished work
requirements of its setup/teardown scope, so a separate cancel task is needed
only for work outside every such scope.)

**Returns:** the ID of the work requirement (``str``).

Example::

   cancel = CancelWorkRequirement(
       task_id="cancel-wr",
       work_requirement_id="{{ ti.xcom_pull(task_ids='add-wr') }}",
       abort_running_tasks=True,
   ).as_teardown(setups=add_wr)

ProvisionWorkerPool
-------------------

Provisions a worker pool, and the compute requirement behind it, from a
compute requirement template and, with ``wait_until_ready=True``, waits until
the pool is ready for work. Use it as the setup task of :doc:`workflow`.

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
   * - ``wait_until_ready``
     - ``bool``
     - ``False``
     - no
     - no
   * - ``poke_interval``
     - ``float`` (seconds)
     - ``30``
     - no
     - no
   * - ``timeout``
     - ``float`` (seconds)
     - ``3600``
     - no
     - no
   * - ``deferrable``
     - ``bool | None``
     - ``None``: Airflow's ``[operators] default_deferrable``
     - no
     - no
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

**wait_until_ready.** With ``False`` (the default), the operator returns as
soon as the pool has been provisioned; the pool may still be starting, so
follow it with a :ref:`WorkerPoolStateSensor <worker-pool-state-sensor>`
waiting for ``IDLE`` or ``RUNNING`` if later tasks need it. With ``True``, the
operator pushes the pool's ID to XCom, then waits (every ``poke_interval``
seconds, for at most ``timeout`` seconds, polling or deferred) until the
pool is ``IDLE`` or ``RUNNING``: its workers have registered. ``EMPTY``,
``PENDING`` and ``CONFIGURING`` are not ready. ``poke_interval``, ``timeout``
and ``deferrable`` are used only with ``wait_until_ready=True``.

**Returns:** the ID of the new worker pool (``str``).

**Retries and kill:** with ``wait_until_ready=True``, a pool that does not
become ready is cleaned up: if the wait fails (failure state, timeout or
error) or the task is killed while it waits, the pool is shut down and its
compute requirement terminated before the task fails (best effort). A task
killed while deferred is cleaned up only on Airflow 3.3 and later, and a
worker crash is not cleaned up at all, so a retry then provisions a second
pool: give the pool an idle shutdown as a backstop (see :ref:`cancellation`).
Once the pool is ready, or without ``wait_until_ready``, the operator never
touches it again; the teardown shuts it down. The pool is not run-stamped, so
a retry always provisions a new pool.

**Fails if:** YellowDog rejects the request (for example, an unknown template
or image family); with ``wait_until_ready``, the pool reaches ``SHUTDOWN`` or
``TERMINATED`` before it is ready
(:exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogStateError`)
or is not ready within ``timeout``
(:exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.WaitTimeoutError`).

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
       wait_until_ready=True,
       timeout=30 * 60,
   ).as_setup()

ShutdownProvisionedWorkerPool
-----------------------------

Shuts down a provisioned worker pool: no further tasks are given to its
workers, and tasks already running are allowed to finish. Identify the pool by
ID or by namespace and name (:ref:`rules <id-or-name>`). Use it as the
teardown task of :doc:`workflow`. First, by default, it cancels the DAG run's
unfinished work requirements.

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
   * - ``cancel_work_requirements``
     - ``"scope" | "run" | "none"``
     - ``"scope"``
     - no
     - no
   * - ``wait``
     - ``bool``
     - ``False``
     - no
     - no
   * - ``poke_interval``
     - ``float`` (seconds)
     - ``30``
     - no
     - no
   * - ``timeout``
     - ``float`` (seconds)
     - ``3600``
     - no
     - no
   * - ``deferrable``
     - ``bool | None``
     - ``None``: Airflow's ``[operators] default_deferrable``
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

**cancel_work_requirements.** Before shutting the pool down, the operator
cancels, with abort, unfinished work requirements submitted in the *same DAG
run*, in every namespace the credentials can see:

- ``"scope"`` (the default): those submitted by the tasks in this teardown's
  setup/teardown scope, that is its setups and every task downstream of a
  setup and upstream of the teardown (every map index of a mapped task;
  inside a mapped task group, each teardown instance covers only its own map
  instance of the group's tasks).
  Parallel scopes, for example a CPU pool and a GPU pool each with its own
  setup and teardown, leave each other's work alone. If the operator is not a
  teardown (``as_teardown(...)``) or has no setups, it cancels nothing and
  logs a warning.
- ``"run"``: those submitted by any task of the DAG run.
- ``"none"``: nothing.

Any other value raises ``YellowDogArgumentError`` when the DAG is parsed. The
work requirements are recognised by the run stamp in their tag, so other
runs' work and work not submitted by the provider are never touched. Failures
are logged and the shutdown goes ahead. Under ``"scope"``, a task outside
every scope (for example a ``RunWorkRequirement`` on an existing pool) is not
cleaned up after a worker crash; use ``"run"`` to cover it. See
:ref:`teardown-cancellation`, and set :ref:`deployment_id <deployment-id>` if
several Airflow environments share the YellowDog account.

**wait.** With ``wait=True``, the operator pushes the pool ID to XCom, then
waits until the pool is ``SHUTDOWN`` or ``TERMINATED`` and, with
``terminate_immediately``, until its compute requirement is ``TERMINATED``.
``timeout`` covers both waits together. ``poke_interval``, ``timeout`` and
``deferrable`` are used only with ``wait=True``.

**Already shut down.** A pool that is already shut down, or a compute
requirement that is already terminating or terminated, is logged and treated
as success (the operator still terminates the compute requirement and waits,
as asked), so a teardown after an idle shutdown, or a retried teardown,
succeeds. If the shutdown fails for another reason, the compute requirement
is still terminated (with ``terminate_immediately``), then the task fails.

**Returns:** the ID of the worker pool (``str``).

**Fails if:** the pool cannot be found (SDK ``404``), or the pool is not a
provisioned worker pool. A configured worker pool, or any other pool type,
raises
:class:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogException`
without changing the pool or cancelling anything. With ``wait``, also if the
wait times out
(:exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.WaitTimeoutError`).

Example, as the teardown of the ``provision-pool`` setup task above::

   shutdown = ShutdownProvisionedWorkerPool(
       task_id="shutdown-pool",
       worker_pool_id="{{ ti.xcom_pull(task_ids='provision-pool') }}",
       terminate_immediately=True,
       wait=True,
   ).as_teardown(setups=provision)
