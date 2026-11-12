Changelog
=========

2.0.0 (unreleased)
------------------

New: provision, run, shut down
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A typical DAG is now three tasks: ``ProvisionWorkerPool(wait_until_ready=True)``
as a setup task, the new ``RunWorkRequirement`` for the work, and
``ShutdownProvisionedWorkerPool`` as the teardown. No sensors are needed. See
:doc:`workflow`.

- **RunWorkRequirement** submits a work requirement with its task groups and
  tasks, and waits until it completes, failing if it fails or is cancelled.
- **Deferral.** The waiting operators and the sensors take ``deferrable``
  (default: Airflow's ``[operators] default_deferrable``) and can hand their
  waits to Airflow's triggerer.
- **Safe retries.** Submitted work requirements carry a run stamp; a retry
  carries on with its earlier attempt instead of duplicating it or failing on
  the name, and clearing a task runs its work again.
- **Nothing left running.** Operators cancel their work requirement (aborting
  running tasks) or terminate their unready pool when they time out, fail
  while waiting, or are killed, or fail partway through a submission; the
  teardown cancels any work still running in its setup/teardown scope, so
  parallel scopes in one DAG run (for example a CPU pool and a GPU pool,
  each with its own setup and teardown) leave each other's work alone;
  inside a mapped task group, each teardown instance covers only its own map
  instance of the group's tasks.

New: YellowDog Portal links
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Operators and sensors that handle a work requirement or a worker pool show a
*YellowDog work requirement* / *YellowDog worker pool* link to the YellowDog
Portal. The Portal URL is derived from the connection's Platform API URL, or
set with the connection's optional *Portal URL* field (see :doc:`connections`).

Breaking changes (migrating from 1.x)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Apache Airflow 3.1 or later is required.** Airflow 2.x is no longer
supported (1.0.4 required Airflow 2.10 or later).

**Python 3.12, 3.13 and 3.14 are supported; Python 3.10 and 3.11 are not.**

**YellowDog SDK 15.10.0 or later is required** (1.0.4 required 15.2.0).

**Operators and sensors take keyword arguments only.** Positional arguments
(such as ``task_id`` and ``connection_id``) are no longer accepted.

.. code-block:: python

   # Before (1.0.4): positional arguments worked
   AddWorkRequirement("add_wr", "yellowdog", wr)

   # After (2.0.0)
   AddWorkRequirement(
       task_id="add_wr",
       connection_id="yellowdog",
       work_requirement=wr,
   )

``connection_id`` is now optional and defaults to ``yellowdog_default``.

**AddPopulatedWorkRequirement rejects work requirements that already have task
groups.** Supply the task groups through ``task_groups_and_tasks`` instead; a
work requirement with a preset ``taskGroups`` raises
:exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogArgumentError`.

.. code-block:: python

   # Before (1.0.4): taskGroups were sent in the initial add call, then
   # overwritten by the generated groups
   wr = WorkRequirement(namespace="ns", name="wr", taskGroups=[tg])
   AddPopulatedWorkRequirement(
       task_id="add_wr",
       work_requirement=wr,
       task_groups_and_tasks=[(tg, tasks)],
   )

   # After (2.0.0): leave taskGroups unset
   wr = WorkRequirement(namespace="ns", name="wr")
   AddPopulatedWorkRequirement(
       task_id="add_wr",
       work_requirement=wr,
       task_groups_and_tasks=[(tg, tasks)],
   )

**ShutdownProvisionedWorkerPool raises instead of returning None.** For a
worker pool of an unsupported type (not a provisioned pool) the operator now
raises :exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogException`; 1.0.4 returned ``None`` and the task
succeeded. Configured worker pools already raised. The return value is now
always the worker pool ID (``str``).

**Templated IDs that render to "None" raise an error.** When an upstream
XCom value is missing, a templated field such as
``"{{ ti.xcom_pull(task_ids='x') }}"`` renders to the string ``"None"``. 1.0.4
used it as a real ID and failed later with an obscure platform error;
2.0.0 fails immediately with a clear message, raising
:exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogArgumentError`. Empty strings are treated as
not supplied.

**A missing ID or name fails when the DAG is parsed.** Operators and sensors
that identify a resource by ID or by namespace plus name now raise
:exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogArgumentError` in the constructor if neither is supplied
(1.0.4 raised :exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogException` at run time). Sensors also reject an
empty ``target_states``.

.. code-block:: python

   # Before (1.0.4): failed at run time
   CancelWorkRequirement(task_id="cancel", connection_id="yd")

   # After (2.0.0): DAG import error; supply an ID or namespace + name
   CancelWorkRequirement(
       task_id="cancel",
       work_requirement_id="{{ ti.xcom_pull(task_ids='add_wr') }}",
   )

:exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogArgumentError` subclasses both :exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogException` and
``ValueError``, so existing ``except YellowDogException`` handlers still work.

**Sensor subclassing: target_states_names removed.** The public
``target_states_names`` attribute no longer exists on the three state sensors,
and ``YellowDogSensor.check_status`` no longer takes a ``target_states_names``
parameter (it logs the names derived from ``target_states``). This affects
only custom sensors that subclass ``YellowDogSensor`` or read the attribute.

.. code-block:: python

   # Before (1.0.4)
   self.check_status(context, obj_id, ObjectName.WORK_REQUIREMENT,
                     state, self.target_states, self.target_states_names)

   # After (2.0.0)
   self.check_status(context, obj_id, ObjectName.WORK_REQUIREMENT,
                     state, self.target_states)

**Sensors fail when the resource reaches a failure state.** Each state
sensor has a new ``failure_states`` argument. By default it is every *final*
state that is not in ``target_states``, and a sensor whose resource reaches one
of these states now fails at once with :exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogStateError` instead of
waiting (1.0.4 kept poking until ``timeout``). :exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogStateError` is an
``AirflowFailException``, so the task is not retried and ``silent_fail`` does
not suppress it. The final states are:
``COMPLETED``, ``FAILED`` and ``CANCELLED`` (work requirements); ``SHUTDOWN``
and ``TERMINATED`` (worker pools); ``TERMINATED`` (compute requirements).
Listing a failure outcome such as ``FAILED`` in ``target_states`` just to stop
waiting made the DAG run succeed when the YellowDog work failed; wait for
``COMPLETED`` only, and the sensor fails if the work fails or is cancelled.
``target_states`` and ``failure_states`` must not overlap, and both must hold
the sensor's status enum (:exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogArgumentError` otherwise).

.. code-block:: python

   # Before (1.0.4): succeeds whether the work completed or failed
   WorkRequirementStateSensor(
       task_id="wait",
       work_requirement_id=wr_id,
       target_states=[WorkRequirementStatus.COMPLETED, WorkRequirementStatus.FAILED],
   )

   # After (2.0.0): succeeds on COMPLETED, fails on FAILED or CANCELLED
   WorkRequirementStateSensor(
       task_id="wait",
       work_requirement_id=wr_id,
       target_states=[WorkRequirementStatus.COMPLETED],
   )

To keep the 1.x behaviour (wait only for the target states), pass
``failure_states=[]``. Sensors whose targets include every final state (for
example ``[SHUTDOWN, TERMINATED]`` for a worker pool) are unaffected.

**AddWorkRequirement and AddPopulatedWorkRequirement are retry-safe, so a
retry may return an existing work requirement or use a different name.**
1.0.4 always added a new work requirement under the supplied name, so a retry
(or a re-run after a clear) failed on the duplicate name, and a retry after a
part-way failure of ``AddPopulatedWorkRequirement`` left the partly built work
requirement behind. Now each submission is stamped with the DAG run and task
(see :ref:`safe-retries`), and on a retry the operator:

- returns this task's earlier work requirement's ID if it is unfinished
  (``AddPopulatedWorkRequirement``: if it is also fully built; a partly built
  one is cancelled, with abort, and replaced), or if it completed and the
  task instance has not been cleared since;
- otherwise submits a fresh copy named ``<name>-try<N>`` (``N`` being the
  Airflow try number).

A work requirement of the same name that another task or DAG run (or anyone
else) submitted is never adopted or touched: a new one is added under
``<name>-try<N>`` or ``<name>-<4 hex>-try<N>`` instead of failing. So the
work requirement's name can differ from the one supplied; downstream tasks
should use the ID from XCom.

.. code-block:: python

   # Before (1.0.4): look up the work requirement by its supplied name
   AddTaskGroupsToWorkRequirement(
       task_id="add-groups",
       namespace="ns",
       work_requirement_name="my-wr",
       task_groups=groups,
   )

   # After (2.0.0): use the ID from XCom; the name may be "my-wr-try2"
   AddTaskGroupsToWorkRequirement(
       task_id="add-groups",
       work_requirement_id="{{ ti.xcom_pull(task_ids='add-wr') }}",
       task_groups=groups,
   )

**The work requirement's tag is replaced by the run stamp.**
``RunWorkRequirement``, ``AddWorkRequirement`` and
``AddPopulatedWorkRequirement`` set the submitted work requirement's ``tag``
to ``airflow:<12 hex>:<12 hex>:m<map index>:c<cycle>``; a ``tag`` you set on the
``WorkRequirement`` is overwritten (a warning is logged). 1.0.4 sent your tag
unchanged. Record such information elsewhere, for example in the name.

**An empty work requirement name is filled in.** With ``name=""`` (or no
name) the submitting operators derive a name from the DAG, run and task IDs
(see :ref:`safe-retries`); 1.0.4 sent it with the empty name unchanged.

**Submitting operators need a task context.** 1.0.4 did not read the task
instance from the context. Now ``AddWorkRequirement`` and
``AddPopulatedWorkRequirement`` read it (``context["ti"]``) for the run
stamp, so calling ``execute()`` directly (for example in a unit
test) needs a context with a task instance; otherwise
:exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogException`
is raised.

**Submitting operators cancel their work on kill.** If an
``AddWorkRequirement`` or ``AddPopulatedWorkRequirement`` task is killed
while it runs, its work requirement is cancelled, aborting running tasks
(best effort). 1.0.4 left it running.

**ShutdownProvisionedWorkerPool first cancels the unfinished work
requirements of its scope.** By default (``cancel_work_requirements="scope"``),
before shutting the pool down it cancels, with abort, every unfinished work
requirement submitted in the same DAG run by a task in its setup/teardown
scope (its setups, and the tasks between them and the teardown), in every
namespace the credentials can see (recognised by the run stamp; other runs'
work, other scopes' work and work not submitted by the provider are never
touched); inside a mapped task group, each teardown instance covers only its
own map instance of the group's tasks. ``"run"`` cancels the whole DAG run's
instead. A ``ShutdownProvisionedWorkerPool`` that is not a teardown with
setups cancels nothing under ``"scope"`` and logs a warning (see
:ref:`teardown-cancellation`); a 1.x DAG with a plain shutdown task (not
``as_teardown``) should make it a teardown of the provisioning task
(``.as_teardown(setups=provision)``), or pass
``cancel_work_requirements="run"`` (cancel the whole run's work) or
``"none"`` (cancel nothing, as 1.0.4) to silence the warning. 1.0.4 only
shut the pool down. If several
Airflow environments share a YellowDog account, set ``[yellowdog]
deployment_id`` in each (see :ref:`deployment-id`).

.. code-block:: python

   # Before (1.0.4): shut down the pool; work requirements kept running
   ShutdownProvisionedWorkerPool(task_id="shutdown", worker_pool_id=pool_id)

   # After (2.0.0): to keep that behaviour, opt out
   ShutdownProvisionedWorkerPool(
       task_id="shutdown",
       worker_pool_id=pool_id,
       cancel_work_requirements="none",
   )

**ShutdownProvisionedWorkerPool succeeds if the pool is already shut down.**
A pool that is already shut down, or a compute requirement that is already
terminating or terminated, is treated as success (1.0.4 failed with the
platform's "invalid transition" error). With ``terminate_immediately=True``,
the compute requirement is now terminated even if the pool's shutdown fails
for another reason; the task then fails with the shutdown error (1.0.4 did not
try to terminate).

**default_deferrable applies to the provider's sensors.** The sensors now
support deferrable mode, and ``deferrable`` defaults to Airflow's
``[operators] default_deferrable`` setting. If your Airflow sets it to
``True``, the sensors now defer, which needs a running triggerer; pass
``deferrable=False`` to keep them polling. In deferrable mode
``mode="reschedule"`` is ignored, ``silent_fail`` has no effect, and a
timeout with ``never_fail=True`` skips the task (see :doc:`sensors`).

**Example DAGs have the three-task shape.** ``yd-provider-test.py`` is now
``provision-worker-pool`` (``ProvisionWorkerPool`` with
``wait_until_ready=True``, a setup task) followed by two
``RunWorkRequirement`` tasks and ``shutdown-worker-pool`` (the teardown,
which takes the pool's ID from XCom); its step-by-step operators, sensors and
``ALL_DONE`` clean-up tasks are gone. Its ``sensor_timeout_seconds`` and ``poke_interval_seconds`` params now
set the waiting operators' ``timeout`` and ``poke_interval``; a new
``deferrable`` param (default ``false``) sets their ``deferrable``; and the
pool is named ``wp-<dag_id>-<run_id>``. ``yd-quickstart.py`` runs one
``RunWorkRequirement`` instead of submitting and then waiting with a sensor.
DAGs copied from the 1.x examples keep working, but see the entries above.

**Type stub files (.pyi) are no longer shipped.** Annotations are inline and
the package ships ``py.typed``; it is checked with pyright (strict) and
``pyright --verifytypes``.

Added
~~~~~

* :class:`~yellowdog_provider.operators.yellowdog_operators.RunWorkRequirement`:
  submit a work requirement with its task groups and tasks, and wait for it
  (``target_states``, ``failure_states``, ``poke_interval``, ``timeout``,
  ``deferrable``); see :doc:`operators`.
* ``ProvisionWorkerPool`` arguments ``wait_until_ready`` (default ``False``,
  the 1.x behaviour), ``poke_interval``, ``timeout`` and ``deferrable``: wait
  until the pool is ``IDLE`` or ``RUNNING``, shutting the pool down if it
  never becomes ready.
* ``ShutdownProvisionedWorkerPool`` arguments ``wait`` (default ``False``),
  ``poke_interval``, ``timeout``, ``deferrable`` and
  ``cancel_work_requirements`` (``"scope"``, the default, ``"run"`` or
  ``"none"``; see above).
* ``deferrable`` on the three sensors, and
  :class:`~yellowdog_provider.triggers.yellowdog_triggers.YellowDogStateTrigger`,
  the trigger behind all deferred waits. On Airflow 3.3 and later (verified on
  3.4), a deferred ``RunWorkRequirement`` or ``ProvisionWorkerPool`` killed by
  a user is cleaned up by the trigger; Airflow 3.1 and 3.2 have no kill hook
  for deferred tasks (see :ref:`cancellation`).
* Waits tolerate up to three consecutive transient errors (connection
  errors, timeouts, HTTP 429 and 5xx, and the SDK's server errors); see
  :ref:`transient-errors`.
* :exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.WaitTimeoutError`,
  raised when an operator's own wait times out (retried by Airflow, unlike
  ``YellowDogStateError``).
* Hook operations for use from TaskFlow: ``get_work_requirement_state``,
  ``get_worker_pool_state``, ``get_compute_requirement_state``,
  ``submit_work_requirement``, ``add_work_requirement``,
  ``cancel_work_requirement``, ``find_unfinished_run_work_requirements``,
  ``provision_worker_pool``, ``shutdown_worker_pool``,
  ``terminate_compute_requirement`` and ``portal_base_url``, with
  :class:`~yellowdog_provider.hooks.yellowdog_hooks.SubmitResult`; and the
  utilities ``yellowdog_provider.utils.run_stamp`` (``RunStamp``,
  ``derive_name``, ``try_name``) and ``yellowdog_provider.utils.waiting``
  (``wait_for_state``, ``resolve_states``, ``is_transient_error``). See
  :doc:`taskflow`.
* The Airflow setting ``[yellowdog] deployment_id``
  (``AIRFLOW__YELLOWDOG__DEPLOYMENT_ID``), to tell apart Airflow environments
  that share a YellowDog account; see :ref:`deployment-id`.
* The submitting operators (``RunWorkRequirement``, ``AddWorkRequirement``,
  ``AddPopulatedWorkRequirement``) submit deep copies, so your
  ``WorkRequirement``, ``TaskGroup`` and ``Task`` objects are never modified:
  the run stamp and any generated name are applied to the copies.
* The connection's optional *Portal URL* field, and the *YellowDog work
  requirement* and *YellowDog worker pool* task links.
* A default connection ID, ``yellowdog_default``, used by the hook, operators
  and sensors when ``connection_id`` is not given.
* ``YellowDogHook.test_connection`` so that **Test** works in the Airflow
  connection UI, and context-manager support (``with YellowDogHook(...)``)
  that closes the client afterwards.
* ``YellowDogHook(..., connection=...)``: a hook can use an already fetched
  Airflow connection instead of looking it up (the trigger fetches the
  connection asynchronously in the triggerer's event loop and passes it to the
  hooks it uses in worker threads).
* ``TemplateCallable`` and ``Templated``, typed helpers for templated fields
  that may be supplied as callables (``yellowdog_provider.utils.templating``).
* :exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogArgumentError` (``yellowdog_provider.exceptions``).
* ``failure_states`` on ``WorkRequirementStateSensor``,
  ``WorkerPoolStateSensor`` and ``ComputeRequirementStateSensor``, and
  :exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogStateError` (``yellowdog_provider.exceptions``, also an
  ``AirflowFailException``), raised when a sensor's resource reaches a failure
  state; see :doc:`sensors`.
* Full Airflow provider metadata (``get_provider_info``: integrations, hooks,
  operators and sensors), so Airflow discovers the provider and its
  connection type.
* Complete type annotations, validated in CI.
* Example DAGs: ``yd-quickstart.py`` and a params-driven ``yd-provider-test.py``
  (connection, namespace, compute requirement template, image family, task
  type and arguments, timeout, poke interval, ``deferrable``, maximum nodes,
  and an optional ``user_data`` parameter for instance userdata).
* Restructured documentation: concepts, quick start, the three-task workflow,
  step-by-step and TaskFlow pages, connections, templating and
  troubleshooting pages, and API signatures in the reference.

Fixed
~~~~~

* ``AddPopulatedWorkRequirement`` now submits task groups in the order given.
  1.0.4 reversed the list before submitting it, to work around a platform bug
  that reversed task-group order; the platform now preserves the order, so
  with 1.0.4 groups would be created in reverse order.
* ``AddTaskGroupsToWorkRequirement`` returns the new task group IDs in the
  same order as the ``task_groups`` argument (previously in platform order).
* The hook's ``conn_name_attr`` is now ``connection_id``, matching the hook's
  constructor argument (it was ``yellowdog_conn_id``).
* Connection host handling: an empty host uses the default Platform API URL;
  a missing scheme gets ``https://``; trailing ``/`` characters are removed.
  A connection without an Application Key ID or Secret gives a clear error.
* Error messages: the missing-argument text in ``AddTasksToTaskGroup`` had a
  missing space and misleading wording; messages now name the arguments
  consistently.
* Worker pool lookups handle optional pool properties and raise a clear error
  for unknown pool types.
* The YellowDog client is closed after each operator and sensor run.

Development
~~~~~~~~~~~

Developer-facing only: ruff, pyright (strict) and ``verifytypes`` replace the
previous tooling; unit tests, opt-in live system tests and an end-to-end
Airflow test were added; CI runs on GitHub Actions (using uv); and releases are
made with the local script ``bin/release.py``. See the README and
``RELEASING.md``.
