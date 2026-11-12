Provision, run, shut down
=========================

Most DAGs that use YellowDog do the same three things: get some workers, run
work on them, and release the workers however the work ended. The provider
has one operator for each step, and Airflow's *setup* and *teardown* tasks tie
them together:

.. code-block:: text

   provision-pool (setup)  >>  run (one or more)  >>  shutdown-pool (teardown)

- :class:`~yellowdog_provider.operators.yellowdog_operators.ProvisionWorkerPool`
  with ``wait_until_ready=True`` provisions a worker pool and waits until its
  workers have registered (``IDLE`` or ``RUNNING``).
- :class:`~yellowdog_provider.operators.yellowdog_operators.RunWorkRequirement`
  submits a work requirement with its task groups and tasks, and waits until
  it completes. It fails if the work fails or is cancelled.
- :class:`~yellowdog_provider.operators.yellowdog_operators.ShutdownProvisionedWorkerPool`
  cancels any work of its setup/teardown scope that is still running, then
  shuts the pool down.

No sensors are needed: each operator waits for its own resource. This page
explains what the three operators do for you: deferral, cancellation,
retries, and clean-up. :doc:`operators` lists every argument. If you prefer
one Airflow task per YellowDog call, see :doc:`step-by-step`; to call
YellowDog from your own ``@task`` functions, see :doc:`taskflow`.

.. important::

   **If more than one Airflow environment uses the same YellowDog account**
   (for example staging and production with the same Application key), give
   each one a different ``[yellowdog] deployment_id`` setting. Without it,
   runs with the same DAG ID and run ID in two environments look like the
   same run to the provider. See :ref:`deployment-id`.

.. contents:: On this page
   :local:
   :depth: 2

An example
----------

.. code-block:: python

   import re
   from datetime import timedelta

   from airflow.sdk import dag
   from yellowdog_client.model import (
       AutoShutdown,
       ComputeRequirementTemplateUsage,
       ProvisionedWorkerPoolProperties,
       RunSpecification,
       Task,
       TaskGroup,
       WorkRequirement,
   )

   from yellowdog_provider.operators.yellowdog_operators import (
       ProvisionWorkerPool,
       RunWorkRequirement,
       ShutdownProvisionedWorkerPool,
   )

   NAMESPACE = "my-namespace"


   def unique_name(context, prefix):
       """A valid YellowDog name unique to the DAG run (prefix starts with a letter)."""
       name = f"{prefix}-{context['run_id']}".lower()
       # Ends with a letter or digit even when shortened
       return re.sub(r"[^a-z0-9_-]", "-", name)[:60].rstrip("-_")


   def make_crtu(context, jinja_env):
       return ComputeRequirementTemplateUsage(
           templateId="my-namespace/my-template",
           requirementNamespace=NAMESPACE,
           requirementName=unique_name(context, "pool"),
           targetInstanceCount=1,
           imagesId="<IMAGE_FAMILY_ID>",
       )


   def make_pool_properties(context, jinja_env):
       return ProvisionedWorkerPoolProperties(
           workerTag=unique_name(context, "tag"),
           # Backstop: the pool shuts itself down if it is left idle
           idleNodeShutdown=AutoShutdown(enabled=True, timeout=timedelta(minutes=10)),
           idlePoolShutdown=AutoShutdown(enabled=True, timeout=timedelta(minutes=10)),
       )


   def make_work_requirement(context, jinja_env):
       # An empty name is derived from the DAG, run and task
       return WorkRequirement(namespace=NAMESPACE, name="")


   def make_groups_and_tasks(context, jinja_env):
       spec = RunSpecification(
           taskTypes=["docker"], workerTags=[unique_name(context, "tag")]
       )
       task = Task(name="task-1", taskType="docker", arguments=["hello-world"])
       return [(TaskGroup(name="group-1", runSpecification=spec), [task])]


   @dag(dag_id="yellowdog-provision-run-shutdown")
   def provision_run_shutdown():
       pool = ProvisionWorkerPool(
           task_id="provision-pool",
           compute_requirement_template_usage=make_crtu,
           provisioned_worker_pool_properties=make_pool_properties,
           wait_until_ready=True,
           timeout=30 * 60,
       ).as_setup()

       run = RunWorkRequirement(
           task_id="run",
           work_requirement=make_work_requirement,
           task_groups_and_tasks=make_groups_and_tasks,
           timeout=6 * 3600,
       )

       shutdown = ShutdownProvisionedWorkerPool(
           task_id="shutdown-pool",
           # The pool's ID from XCom, pushed by the setup task
           worker_pool_id="{{ ti.xcom_pull(task_ids='provision-pool') }}",
           terminate_immediately=True,
       ).as_teardown(setups=pool)

       pool >> run >> shutdown


   provision_run_shutdown()

The package's example DAG ``yellowdog_provider/example_dags/yd-provider-test.py``
has the same shape, with two work requirements running in parallel on the
pool, and everything driven by DAG params. Its DAG function:

.. literalinclude:: ../yellowdog_provider/example_dags/yd-provider-test.py
   :language: python
   :start-at: @dag(
   :end-before: ###############################################################################

How the three tasks fit together
--------------------------------

**Setup and teardown.** ``.as_setup()`` and ``.as_teardown(setups=...)`` are
Airflow's way of saying "this task creates a resource and that one removes
it". Airflow runs the teardown once all the tasks between them have finished,
whether they succeeded or failed, and a teardown's result does not decide
whether the DAG run succeeded. (In 1.x DAGs the same was done with
``trigger_rule=TriggerRule.ALL_DONE``; setup and teardown are clearer and are
what the examples now use.) If the setup task itself fails, Airflow does not
run the teardown. That is safe here, because ``ProvisionWorkerPool`` shuts down
a pool that never became ready before it fails (see
:ref:`cancellation`).

**IDs through XCom.** ``ProvisionWorkerPool`` (with ``wait_until_ready``) and
``RunWorkRequirement`` push the resource's ID to XCom (key ``return_value``)
*as soon as it exists*, before they start waiting, and also return it, so
downstream tasks such as the teardown
(``"{{ ti.xcom_pull(task_ids='provision-pool') }}"``) can use it. The
*YellowDog worker pool* and *YellowDog work requirement* links (see
:doc:`connections`) appear once a task has deferred or finished, not while it
is polling on a worker. Pass IDs, not names, to downstream
tasks: the name of a work requirement can differ from the one you supplied
(see :ref:`safe-retries`), and a name such as a pool's could match another
run's resource.

**Waiting.** Each operator polls its resource every ``poke_interval``
seconds, for at most ``timeout`` seconds:

.. list-table::
   :header-rows: 1
   :widths: 30 25 15 30

   * - Operator
     - Waits for
     - Defaults
     - On timeout
   * - ``ProvisionWorkerPool`` (``wait_until_ready=True``)
     - ``IDLE`` or ``RUNNING``; fails on ``SHUTDOWN`` or ``TERMINATED``
     - 30 s, 1 hour
     - shuts down the pool and terminates its compute requirement, then
       fails with :exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.WaitTimeoutError`
   * - ``RunWorkRequirement``
     - ``target_states`` (default ``COMPLETED``); fails on
       ``failure_states`` (default ``FAILED`` and ``CANCELLED``)
     - 60 s, 7 days
     - cancels the work requirement, aborting running tasks, then fails with
       :exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.WaitTimeoutError`
   * - ``ShutdownProvisionedWorkerPool`` (``wait=True``)
     - pool ``SHUTDOWN`` or ``TERMINATED``, then (with
       ``terminate_immediately``) compute requirement ``TERMINATED``
     - 30 s, 1 hour (both waits together)
     - fails with
       :exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.WaitTimeoutError`

A failure state raises
:exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogStateError`,
which Airflow does not retry (the resource will not leave a final state).
``WaitTimeoutError`` is retried according to the task's ``retries``: the
resource has been cleaned up, so a retry starts afresh.

.. _deferral:

Deferral
--------

While an operator waits it can either keep its worker slot, polling from the
worker, or *defer*: hand the polling to Airflow's **triggerer** and free the
slot until the wait is over. Deferring is the better choice for anything but
short waits, because a waiting task then costs almost nothing.

All three operators, and the three sensors, take ``deferrable``:

- ``deferrable=True``: check the state once on the worker; if the wait is not
  already over, defer to a
  :class:`~yellowdog_provider.triggers.yellowdog_triggers.YellowDogStateTrigger`,
  which polls every ``poke_interval`` seconds until the target state, a
  failure state or the timeout, then resumes the task on a worker.
- ``deferrable=False``: poll on the worker.
- Not given (``None``): Airflow's ``[operators] default_deferrable`` setting
  (environment variable ``AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE``), which is
  ``False`` unless you change it. It is read when the DAG file is parsed.

**Deferral needs a running triggerer** (``airflow triggerer``). Where one runs,
we recommend turning deferral on, either per task or for the whole deployment
with ``default_deferrable = True``. Without a triggerer a deferred task never
resumes: it stays *deferred* until Airflow's own deferral timeout fails it,
and the operator does not get the chance to clean up (see
:doc:`troubleshooting`).

The operators set Airflow's deferral timeout to the remaining ``timeout`` plus
:data:`~yellowdog_provider.operators.yellowdog_operators.DEFER_TIMEOUT_GRACE`
(300 seconds), so that the trigger's own timeout arrives first and the
operator can cancel or clean up. Airflow's timeout is only a backstop.

``ShutdownProvisionedWorkerPool`` with ``wait=True`` and
``terminate_immediately=True`` defers twice: once for the pool, then once for
the compute requirement, with one overall deadline.

.. _transient-errors:

Transient errors while waiting
------------------------------

A long wait should not fail because of a single network problem. While
waiting, the operators (polling or deferred) and the deferred sensors tolerate
up to **3 consecutive** transient errors when checking the state, logging a
warning for each (``Transient error ... (1/3)``) and trying again after
``poke_interval``. Any successful check resets the count. The 4th consecutive
transient error, or any other error, ends the wait, and the operator cleans up
and fails as for a timeout.

Transient errors are (see
:func:`~yellowdog_provider.utils.waiting.is_transient_error`): connection
errors and request timeouts (``requests.exceptions.ConnectionError`` and
``Timeout``), HTTP errors with status 429 or 5xx, and the SDK's
``InternalServerException`` and ``ServerErrorException``. Other errors, such
as a ``404`` or an authorisation error, are not transient.

If the timeout passes during a run of transient errors, the wait ends with a
timeout (reporting the last state read). Sensors that poll (not deferred) do
not have this tolerance: an error in a poke fails the sensor unless
``silent_fail=True`` (see :doc:`sensors`).

.. _cancellation:

Nothing left running: cancellation and clean-up
-----------------------------------------------

The waiting operators clean up after themselves when they give up:

- ``RunWorkRequirement`` **cancels its work requirement with abort** (running
  tasks are stopped straight away).
- ``ProvisionWorkerPool`` (``wait_until_ready=True``) **shuts down its pool and
  terminates the pool's compute requirement**, as long as the pool is not yet
  ready. Once the pool is ready, it belongs to the teardown, and killing the
  task leaves it alone.

Clean-up is best-effort: if it fails, the failure is logged, and the
original error is still the one the task fails with.

.. list-table::
   :header-rows: 1
   :widths: 34 66

   * - What happens
     - What the provider does
   * - The wait times out (``timeout``)
     - Cleans up, then fails with ``WaitTimeoutError``. Same when deferred.
   * - An error while waiting (not transient, or a 4th consecutive transient
       error)
     - Cleans up, then fails with the error. Same when deferred.
   * - The work requirement reaches a failure state
     - Fails with ``YellowDogStateError``. A final state (``FAILED``,
       ``CANCELLED``) needs no clean-up; a non-final state you listed in
       ``failure_states`` (for example ``HELD``) cancels the work
       requirement first. A pool that reaches ``SHUTDOWN`` or ``TERMINATED``
       is still terminated (best-effort).
   * - The task is killed while polling on a worker: marked failed or
       succeeded or cleared in the UI, or stopped by ``execution_timeout``
     - Airflow calls the operator's ``on_kill``, which cleans up.
   * - The task is killed **while deferred** (marked failed or succeeded, or
       cleared), on **Airflow 3.3 or later** (verified on 3.4)
     - Airflow calls the trigger's ``on_kill``, which cancels the work
       requirement (``RunWorkRequirement``) or terminates the pool
       (``ProvisionWorkerPool``). Airflow limits this to ``[triggerer]
       on_kill_timeout`` (30 seconds by default).
   * - The task is killed while deferred, on **Airflow 3.1 or 3.2**
     - **Nothing**: these versions have no kill hook for deferred tasks. A
       work requirement is cancelled by the teardown (below). A pool is not
       cleaned up, and the teardown does not run because its setup failed:
       give the pool an idle shutdown (``idlePoolShutdown`` and
       ``idleNodeShutdown`` in ``ProvisionedWorkerPoolProperties``), or use
       ``deferrable=False`` for the ``ProvisionWorkerPool`` task.
   * - The worker running the task crashes, or Airflow loses track of it
     - Nothing runs ``on_kill``. A retry of ``RunWorkRequirement`` finds its
       unfinished work requirement and resumes waiting for it (see
       :ref:`safe-retries`). A retry of ``ProvisionWorkerPool`` provisions a
       *second* pool, and the first is orphaned: give pools an idle shutdown
       as the backstop.

The operators that only submit,
:class:`~yellowdog_provider.operators.yellowdog_operators.AddWorkRequirement`
and
:class:`~yellowdog_provider.operators.yellowdog_operators.AddPopulatedWorkRequirement`,
also cancel their work requirement (with abort) if killed while running.

.. _teardown-cancellation:

The teardown cancels its scope's work
-------------------------------------

Before it shuts the pool down, ``ShutdownProvisionedWorkerPool`` cancels, with
abort, the **unfinished work requirements submitted by the tasks in its
setup/teardown scope**, in every namespace that the connection's credentials
can see. This is the backstop for the cases above where nothing else cleans
up: a deferred task killed on Airflow 3.1 or 3.2, a crashed worker, or a task
killed in the moment between creating its work requirement and recording its
ID (an error while the work requirement is being submitted cancels the partly
built work requirement, so the kill is the only gap).

The teardown's *scope* is:

- its setups: its direct upstream tasks that are setups (``as_setup()``, or
  ``as_teardown(setups=...)``);
- every task that is both downstream of one of those setups and upstream of
  the teardown.

The teardown itself is not in its scope. A mapped task is covered by its
task ID, so the work requirements of all its map indices are cancelled. Task
IDs inside an Airflow ``TaskGroup`` are the fully qualified ones
(``group.task``).

Inside a **mapped task group** (``@task_group`` with ``.expand(...)``), each
teardown instance covers only its own map instance of the tasks in that
group: Airflow runs a group instance's teardown once that instance's tasks
are done, while other instances may still be running, so teardown ``[0]``
cancels the work of ``grp.w[0]`` but not of ``grp.w[1]``. Scope tasks outside
the group (for example a setup before it), and tasks in a mapped group nested
inside it, are still covered at every map index. For example, with ``s1 >> w1 >> t1.as_teardown(setups=s1)``
and ``s2 >> w2 >> t2.as_teardown(setups=s2)`` in the same DAG, ``t1``
cancels only the work of ``s1`` and ``w1``, and ``t2`` only that of ``s2``
and ``w2``, so two pools (for example a CPU pool and a GPU pool) can run
side by side in one DAG run. A task in two overlapping scopes is cancelled
by whichever teardown runs first.

``cancel_work_requirements`` chooses what is cancelled:

- ``"scope"`` (the default): the work requirements of the tasks in this
  teardown's scope, as above.
- ``"run"``: every unfinished work requirement that **this DAG run**
  submitted, from any task. Use it when a single teardown should clean up
  the whole run, and nothing else runs in parallel that it must not abort.
- ``"none"``: nothing is cancelled.

Any other value raises ``YellowDogArgumentError`` when the DAG is parsed.

.. note::

   Under ``"scope"``, a task that is in no teardown's scope, for example a
   ``RunWorkRequirement`` on an existing pool in a parallel branch, is not
   cleaned up by any teardown. It still cancels its own work requirement on
   failure, timeout or kill; what is lost is the clean-up after a crashed
   worker (or one of the other gaps above). To have a teardown cover such
   tasks, put them in its scope or use ``cancel_work_requirements="run"``.

.. warning::

   Under ``"scope"``, an operator that is not a teardown
   (``as_teardown(...)``), or a teardown with no setups, has no scope: it
   cancels nothing, logs a warning saying so, and still shuts the pool down.
   Use ``"run"`` if it should cancel the run's work.

- The work requirements are recognised by the run stamp in their ``tag``
  (below): the run tag prefix and, under ``"scope"``, the task tag prefix of
  each task in the scope (with the map index, inside a mapped task group).
  Work requirements of other DAG runs, or not submitted by the provider, are
  never touched, even if they have similar names.
- It is best-effort: a failure to look up or cancel is logged, and the
  shutdown goes ahead. Nothing is cancelled if the pool is rejected (not a
  provisioned pool).

The teardown also tolerates work that is already done: a pool that is already
shut down (for example by its idle shutdown) and a compute requirement that
is already terminating or terminated are logged and treated as success, so a
teardown, or a retry of one, does not fail the DAG run because there is
nothing left to do. With ``terminate_immediately``, the compute requirement
is terminated even if the pool's shutdown fails for another reason; the task
then fails with the shutdown error.

.. _safe-retries:

Safe retries and the run stamp
------------------------------

Airflow retries failed tasks, and you can *clear* a task to run it again. For
an operator that submits work, the danger is a duplicate (the same work run
twice, or a retry failing because the name is taken) or an orphan (an earlier
attempt left running). ``RunWorkRequirement``, ``AddPopulatedWorkRequirement``
and ``AddWorkRequirement`` avoid both by stamping every work requirement they
submit with where it came from.

The run stamp
~~~~~~~~~~~~~

The work requirement's ``tag`` is set to::

   airflow:<run digest>:<task digest>:m<map index>:c<cycle>

for example ``airflow:9e93c7577e68:d3a517bdc1f2:m-1:c0``, where:

- ``<run digest>`` (12 hex characters) identifies the DAG run: a digest of the
  DAG ID and run ID (and the :ref:`deployment ID <deployment-id>`, if set).
  ``airflow:<run digest>`` is the *run tag prefix*, which the teardown
  searches for with ``cancel_work_requirements="run"``.
- ``<task digest>`` (12 hex characters) identifies the task within the run: a
  digest of the task ID (and the deployment ID, if set), shared by every map
  index of a mapped task. ``airflow:<run digest>:<task digest>`` is the
  *task tag prefix*; the teardown searches for those of the tasks in its
  scope (the default, ``"scope"``).
- ``<map index>`` is the task instance's map index, ``-1`` if not mapped.
  With it, the tag identifies each task instance, so map indices never take
  over each other's work requirements on a retry. For a mapped
  ``RunWorkRequirement``, leave the work requirement's ``name`` unset: the
  derived names are unique per map index, whereas a fixed name is seen free
  by concurrent map indices, and all but one then fail on the duplicate.
- ``<cycle>`` is the task instance's *clear cycle* (below).

Any ``tag`` you set on the ``WorkRequirement`` is **replaced** by the stamp,
and a warning is logged.

Names
~~~~~

The work requirement is named as you supplied it. If the name is empty, a
name is derived from the DAG ID, run ID and task ID (each shortened), plus an
8-character digest, in at most 60 characters: task ``run`` of DAG
``my-dag`` in run ``manual__2026-11-11T10:00:00+00:00`` gets
``my-dag-manual__2-f25068ed-run-2152119a``. Use the ID from XCom, or the
name in the task log, to find it.

YellowDog names must be at most 60 characters, contain only lower-case
letters, digits, ``_`` or ``-``, start with a letter and end with a letter or
digit. Derived names always follow this rule (a DAG ID that does not start
with a letter is prefixed with ``x-``). A name you supply is used as given,
and YellowDog rejects it if it breaks the rule; a ``-try<N>`` copy of a valid
name is valid.

When a new copy has to be submitted (see below), it is named
``<name>-try<N>``, where ``<N>`` is the Airflow try number. A work requirement
of the same name that belongs to another task, another DAG run or nobody is
never adopted, changed or cancelled; the new one gets a different name
instead: ``<name>-try<N>`` or, if that is also taken, ``<name>-<4 hex>-try<N>``.
So **the name of the work requirement may differ from the one you
supplied**: downstream tasks should use the ID from XCom, not look it up by
name.

What a retry does
~~~~~~~~~~~~~~~~~

On every try, the operator looks for this task's earlier attempts (work
requirements with this task's stamp) and acts on the most recent one:

.. list-table::
   :header-rows: 1
   :widths: 36 64

   * - Most recent earlier attempt
     - What the operator does
   * - None
     - Submits under the name (or a different name if the name is taken).
   * - Unfinished (``RUNNING``, ``HELD``, ``FINISHING``), fully built
     - **Resumes** it: waits for it (``RunWorkRequirement``) or returns its ID,
       without submitting anything.
   * - Unfinished, but only partly built (the earlier try was killed while
       adding task groups or tasks, or failed and could not cancel it)
     - Cancels it, with abort, and submits a fresh copy.
       (``AddWorkRequirement`` does not check task groups: it returns the
       unfinished work requirement.)
   * - ``FAILED``, ``CANCELLED`` or ``CANCELLING``
     - Submits a fresh copy, ``<name>-try<N>``.
   * - ``COMPLETED``, in the current clear cycle
     - Returns its ID without submitting again: the work was done.
   * - ``COMPLETED``, before the task instance was cleared
     - Submits a fresh copy: clearing a task means "do it again".

Retries and clears
~~~~~~~~~~~~~~~~~~

An automatic retry never re-runs completed work, but **clearing a task
instance re-runs its work** as a fresh copy, matching what clearing means in
Airflow. The operator tells the two apart with the *clear cycle*: the task
instance's Airflow ``max_tries``, which Airflow raises every time it clears a
task instance and leaves unchanged on retries (verified on Airflow 3.1 and
3.4). A completed attempt counts as done only if it was submitted in the
current cycle.

Limitations:

- Airflow does not tell the task whether it succeeded before it was cleared,
  so clearing a task that *failed* after its work requirement completed also
  runs the work again.
- On Airflow 3.1, if a task instance is cleared while it is running and not
  deferred, a narrow race can record it as failed without raising
  ``max_tries``. Its next try is then treated as a retry, and a completed
  work requirement is not run again.
- On Airflow 3.1 and 3.2, a task instance cleared while it is *deferred*
  leaves its work requirement running (these versions have no kill hook for
  deferred tasks; see :ref:`cancellation`). The next try finds that
  unfinished work requirement and resumes waiting for it instead of running
  the work again.
- If the task instance has no usable ``max_tries``, cycle 0 is used (and a
  warning logged).

Operators without the run stamp
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``AddTaskGroupsToWorkRequirement``, ``AddTasksToTaskGroup`` and
``ProvisionWorkerPool`` do not stamp what they create, so a retry does the
step again: see each operator's *Retries and kill* notes in :doc:`operators`.
``ProvisionWorkerPool`` cleans up an unready pool before it fails, so a retry
after a failed wait does not leave the first pool running, but after a worker
crash it does (see :ref:`cancellation`).

.. _deployment-id:

Several Airflow environments: ``deployment_id``
------------------------------------------------

The run stamp is built from the DAG ID, run ID and task ID. Two Airflow
environments, such as staging and production, can easily have runs with the
same IDs: scheduled runs are named after their date
(``scheduled__2026-11-11T00:00:00+00:00``). If both use the same YellowDog
account, the provider cannot tell their runs apart, so:

- a retry in one environment could resume, or skip as completed, the other
  environment's work requirement; and
- a teardown in one environment could **cancel the other environment's
  work**.

To prevent this, give each Airflow environment its own deployment ID, in the
``[yellowdog]`` section of ``airflow.cfg``:

.. code-block:: ini

   [yellowdog]
   deployment_id = production

or with the environment variable ``AIRFLOW__YELLOWDOG__DEPLOYMENT_ID``. Set it
wherever tasks run (every worker) and keep it stable. The value is mixed into
both digests of the run stamp; when it is empty (the default), stamps are the
same as without it.

Changing the deployment ID while DAG runs are in progress means their earlier
work requirements are no longer recognised: a retry submits a fresh copy, and
the teardown no longer cancels the old ones. Change it when nothing is running.

You do not need a deployment ID if each Airflow environment has its own
YellowDog account (or its own Application whose permissions cover different
namespaces, since the teardown searches only the namespaces the credentials
can see).
