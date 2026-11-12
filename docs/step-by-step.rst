Step by step
============

:doc:`workflow` does each job (provision, run, shut down) in one task. The
provider also has finer-grained operators, one per YellowDog call, and
sensors that only wait. Use them when you need a step in between, for example
to add task groups to a work requirement in stages, to wait for a resource
created outside the DAG, or to keep a 1.x DAG's shape.

.. contents:: On this page
   :local:

The operators and sensors
-------------------------

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Step
     - Class
   * - Provision a worker pool (and return at once)
     - ``ProvisionWorkerPool`` (``wait_until_ready=False``, the default)
   * - Wait for the pool to be ready
     - ``WorkerPoolStateSensor`` with ``target_states=[IDLE, RUNNING]``
   * - Submit a work requirement without task groups
     - ``AddWorkRequirement``
   * - Add task groups to it
     - ``AddTaskGroupsToWorkRequirement``
   * - Add tasks to a task group
     - ``AddTasksToTaskGroup``
   * - Submit a work requirement with its task groups and tasks, without
       waiting
     - ``AddPopulatedWorkRequirement``
   * - Wait for a work requirement to finish
     - ``WorkRequirementStateSensor`` with ``target_states=[COMPLETED]``
   * - Cancel a work requirement
     - ``CancelWorkRequirement``
   * - Shut down a pool
     - ``ShutdownProvisionedWorkerPool``
   * - Wait for a compute requirement (for example until ``TERMINATED``)
     - ``ComputeRequirementStateSensor``

Every argument is described in :doc:`operators` and :doc:`sensors`.

An example
----------

The same work as the :doc:`workflow` example, in seven tasks. The helper
functions ``make_crtu``, ``make_pool_properties`` and ``unique_name``, and the
``NAMESPACE`` constant, are the ones on that page.

.. code-block:: python

   from airflow.sdk import dag
   from yellowdog_client.model import (
       RunSpecification,
       Task,
       TaskGroup,
       WorkerPoolStatus,
       WorkRequirement,
       WorkRequirementStatus,
   )

   from yellowdog_provider.operators.yellowdog_operators import (
       AddTaskGroupsToWorkRequirement,
       AddTasksToTaskGroup,
       AddWorkRequirement,
       ProvisionWorkerPool,
       ShutdownProvisionedWorkerPool,
   )
   from yellowdog_provider.sensors.yellowdog_sensors import (
       WorkerPoolStateSensor,
       WorkRequirementStateSensor,
   )


   def make_task_groups(context, jinja_env):
       spec = RunSpecification(
           taskTypes=["docker"], workerTags=[unique_name(context, "tag")]
       )
       return [TaskGroup(name="group-1", runSpecification=spec)]


   @dag(dag_id="yellowdog-step-by-step")
   def step_by_step():
       pool = ProvisionWorkerPool(
           task_id="provision-pool",
           compute_requirement_template_usage=make_crtu,
           provisioned_worker_pool_properties=make_pool_properties,
       ).as_setup()

       pool_ready = WorkerPoolStateSensor(
           task_id="wait-for-pool",
           worker_pool_id="{{ ti.xcom_pull(task_ids='provision-pool') }}",
           target_states=[WorkerPoolStatus.IDLE, WorkerPoolStatus.RUNNING],
           timeout=30 * 60,
           deferrable=True,
       )

       add_wr = AddWorkRequirement(
           task_id="add-wr",
           work_requirement=lambda context, jinja_env: WorkRequirement(
               namespace=NAMESPACE, name=unique_name(context, "wr")
           ),
       )

       add_groups = AddTaskGroupsToWorkRequirement(
           task_id="add-groups",
           work_requirement_id="{{ ti.xcom_pull(task_ids='add-wr') }}",
           task_groups=make_task_groups,
       )

       add_tasks = AddTasksToTaskGroup(
           task_id="add-tasks",
           task_group_id="{{ ti.xcom_pull(task_ids='add-groups')[0] }}",
           tasks=[Task(name="task-1", taskType="docker", arguments=["hello-world"])],
       )

       work_done = WorkRequirementStateSensor(
           task_id="wait-for-work",
           work_requirement_id="{{ ti.xcom_pull(task_ids='add-wr') }}",
           target_states=[WorkRequirementStatus.COMPLETED],
           timeout=6 * 3600,
           deferrable=True,
       )

       shutdown = ShutdownProvisionedWorkerPool(
           task_id="shutdown-pool",
           worker_pool_id="{{ ti.xcom_pull(task_ids='provision-pool') }}",
           terminate_immediately=True,
       ).as_teardown(setups=pool)

       pool >> pool_ready >> add_wr >> add_groups >> add_tasks >> work_done >> shutdown


   step_by_step()

What changes compared with the three-task shape
-----------------------------------------------

**Clean-up is the teardown's job.** A sensor only watches: if
``wait-for-work`` times out or is killed, the work requirement keeps running
until the teardown cancels it (by default the teardown cancels the
unfinished work requirements of every task in its setup/teardown scope,
which here is every other task of the DAG; see :ref:`teardown-cancellation`).
Likewise, if ``wait-for-pool`` fails, ``provision-pool`` has already
succeeded, so the teardown shuts the pool down. ``AddWorkRequirement`` and
``AddPopulatedWorkRequirement`` cancel their work requirement only if they
are killed while they run, which is a short window.

**Retries differ per step.**

- ``AddWorkRequirement`` and ``AddPopulatedWorkRequirement`` are retry-safe,
  like ``RunWorkRequirement``: a retry returns this task's earlier work
  requirement if it is unfinished (or completed in the current clear cycle)
  instead of failing on the duplicate name. See :ref:`safe-retries`.
- ``AddTaskGroupsToWorkRequirement`` appends its groups in one update, so a
  retry after that update succeeded fails on the duplicate group names.
- ``AddTasksToTaskGroup`` adds tasks without checking for earlier ones, so a
  retry after a part-way failure may add some tasks twice.
- ``ProvisionWorkerPool`` without ``wait_until_ready`` returns as soon as the
  pool exists, so it rarely needs a retry; a retry provisions another pool.

**Sensors defer too.** Each sensor takes ``deferrable`` (by default Airflow's
``[operators] default_deferrable``); see :ref:`deferral`.

**Names and IDs.** Pass IDs between tasks with ``xcom_pull``, as above. The
name of a work requirement added by ``AddWorkRequirement`` can differ from the
one you supplied, if a work requirement of another task or run already has
that name (see :ref:`safe-retries`), so look work requirements up by ID.
