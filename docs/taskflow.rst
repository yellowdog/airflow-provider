Using the hook from TaskFlow
============================

The operators cover the common workflow, but sometimes it is simpler to call
YellowDog from your own Python code, in a TaskFlow ``@task`` function. The
:class:`~yellowdog_provider.hooks.yellowdog_hooks.YellowDogHook` has typed
methods for the same operations the operators use, including the retry-safe
submission, so your own tasks can behave like the operators.

.. contents:: On this page
   :local:

Hook operations
---------------

Use the hook as a context manager, so its YellowDog client is closed
afterwards. All methods raise the SDK's errors unchanged, except where noted.

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Method
     - What it does
   * - ``get_work_requirement_state(work_requirement_id)``,
       ``get_worker_pool_state(worker_pool_id)``,
       ``get_compute_requirement_state(compute_requirement_id)``
     - Return the resource's current status enum (``WorkRequirementStatus``,
       ``WorkerPoolStatus``, ``ComputeRequirementStatus``).
   * - ``submit_work_requirement(work_requirement, task_groups_and_tasks, *,
       stamp, try_number, cycle)``
     - Submit a work requirement with its task groups and tasks, safely
       across retries and clears (as ``RunWorkRequirement`` does; see
       :ref:`safe-retries`). Returns a
       :class:`~yellowdog_provider.hooks.yellowdog_hooks.SubmitResult`
       (``work_requirement_id``, ``name``, ``action``: ``"submitted"``,
       ``"resumed"`` or ``"already_completed"``).
   * - ``add_work_requirement(work_requirement, *, stamp, try_number,
       cycle)``
     - The same for a work requirement on its own (as
       ``AddWorkRequirement`` does).
   * - ``cancel_work_requirement(work_requirement_id, *, abort)``
     - Cancel a work requirement; ``abort=True`` stops running tasks. A
       work requirement that has already finished is not an error.
   * - ``find_unfinished_run_work_requirements(namespace, run_tag_prefix, task_tag_prefixes=None)``
     - IDs of the unfinished work requirements submitted by one DAG run, in a
       namespace or (``None``) in every visible namespace; with
       ``task_tag_prefixes``, only those of the given tasks: each prefix is
       a task's ``airflow:<run>:<task>`` (every map index) or a task
       instance's ``airflow:<run>:<task>:m<index>`` (that map index only).
       Used by the teardown.
   * - ``provision_worker_pool(usage, properties)``
     - Provision a worker pool; returns its ID.
   * - ``shutdown_worker_pool(worker_pool_id, *, terminate_immediately)``
     - Shut down a provisioned worker pool and optionally terminate its
       compute requirement (returning its ID). A pool that is already shut
       down, or a compute requirement already terminating or terminated, is
       not an error.
   * - ``terminate_compute_requirement(compute_requirement_id)``
     - Terminate a compute requirement; already terminating or terminated is
       not an error.
   * - ``portal_base_url()``
     - The YellowDog Portal URL for the connection (see :doc:`connections`).
   * - ``get_conn()``
     - The SDK ``PlatformClient``, for anything else.

To wait for a state, use
:func:`~yellowdog_provider.utils.waiting.wait_for_state`, the loop that the
operators use when they do not defer. It polls a function until it returns a
target state, raises the error you describe for a failure state, raises
:exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.WaitTimeoutError`
when the timeout passes, and tolerates :ref:`transient errors
<transient-errors>`.
:func:`~yellowdog_provider.utils.waiting.resolve_states` validates target
and failure states and works out the default failure states.

The full signatures are in the API reference:
:class:`~yellowdog_provider.hooks.yellowdog_hooks.YellowDogHook`, and
:doc:`yellowdog_provider.utils`.

Example: submit, wait, cancel
-----------------------------

This ``@task`` does what ``RunWorkRequirement`` does without deferral:
it submits a work requirement (resuming this task's earlier attempt on a
retry), waits until it completes, and cancels it if the wait times out or
fails:

.. code-block:: python

   from airflow.sdk import dag, get_current_context, task
   from yellowdog_client.model import (
       RunSpecification,
       Task,
       TaskGroup,
       WorkRequirement,
       WorkRequirementStatus,
   )

   from yellowdog_provider.exceptions.yellowdog_exceptions import YellowDogStateError
   from yellowdog_provider.hooks.yellowdog_hooks import YellowDogHook
   from yellowdog_provider.sensors.yellowdog_sensors import (
       WORK_REQUIREMENT_FINAL_STATES,
   )
   from yellowdog_provider.utils.run_stamp import RunStamp
   from yellowdog_provider.utils.waiting import resolve_states, wait_for_state


   @task
   def run_hello_world(namespace: str, worker_tag: str) -> str:
       context = get_current_context()
       assert "ti" in context  # For type checkers: always present in a task
       ti = context["ti"]
       spec = RunSpecification(taskTypes=["docker"], workerTags=[worker_tag])
       task = Task(name="task-1", taskType="docker", arguments=["hello-world"])
       groups = [(TaskGroup(name="group-1", runSpecification=spec), [task])]
       # Wait for COMPLETED; fail on FAILED or CANCELLED
       targets, failures = resolve_states(
           [WorkRequirementStatus.COMPLETED],
           None,
           WORK_REQUIREMENT_FINAL_STATES,
           "work requirement",
       )

       with YellowDogHook("yellowdog_default") as hook:
           result = hook.submit_work_requirement(
               WorkRequirement(namespace=namespace, name=""),  # name derived
               groups,
               stamp=RunStamp.from_context(context),
               try_number=ti.try_number,
               cycle=ti.max_tries,  # the clear cycle
           )
           wr_id = result.work_requirement_id
           if result.action == "already_completed":
               return wr_id
           try:
               wait_for_state(
                   lambda: hook.get_work_requirement_state(wr_id),
                   targets,
                   failures,
                   poke_interval=30,
                   timeout=3600,
                   describe=lambda state: YellowDogStateError(
                       kind="work requirement",
                       resource_id=wr_id,
                       current_state=state,
                       target_states=targets,
                   ),
                   log=hook.log,
                   kind="work requirement",
                   resource_id=wr_id,
               )
           except YellowDogStateError:
               raise  # A final state: nothing to cancel
           except Exception:
               hook.cancel_work_requirement(wr_id, abort=True)
               raise
       return wr_id


   @dag(dag_id="yellowdog-taskflow")
   def yellowdog_taskflow():
       run_hello_world("my-namespace", "my-tag")


   yellowdog_taskflow()

Notes:

- ``stamp``, ``try_number`` and ``cycle`` make the submission retry-safe:
  pass the run stamp built from the task's context, Airflow's try number
  (used in ``-try<N>`` names) and the task instance's ``max_tries`` (the
  clear cycle). See :ref:`safe-retries`. The work requirement is tagged with
  the run stamp, so a teardown's cancellation
  (:ref:`teardown-cancellation`) covers it too when the task is in that
  teardown's setup/teardown scope (or with
  ``cancel_work_requirements="run"``).
- Unlike the operators, a ``@task`` function has no ``on_kill``, so this
  task does not cancel its work requirement when it is killed (for example
  marked failed). Use a ``ShutdownProvisionedWorkerPool`` teardown whose
  scope includes this task, which cancels its unfinished work requirements,
  as the backstop.
- This task holds a worker slot while it waits. For long waits, use
  ``RunWorkRequirement`` with ``deferrable=True`` instead.

Passing SDK objects between tasks
---------------------------------

A ``@task`` cannot return a ``WorkRequirement`` (or other SDK object) for
an operator to use: Airflow cannot pass SDK objects through XCom (see
:ref:`sdk-objects-xcom`). Pass plain values (IDs, names, numbers, lists and
dicts) between tasks, and build SDK objects inside the task that uses them,
or in an operator's callable.
