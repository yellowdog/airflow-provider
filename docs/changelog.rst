Changelog
=========

2.0.0 (unreleased)
------------------

Version 2.0.0 is relative to 1.0.4, the last release published to PyPI.
Version 1.0.5 was never released; its changes are included here.

Breaking changes (migrating from 1.x)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Apache Airflow 3.1 or later is required.** Airflow 2.x is no longer
supported (1.0.4 required Airflow 2.10 or later).

**Python 3.12, 3.13 and 3.14 are supported; Python 3.10 and 3.11 are not.**
On Python older than 3.12, every ``yellowdog-sdk`` release caps
``urllib3<2``, which conflicts with the Apache Airflow constraints files
(which pin urllib3 2.x), so the provider cannot be installed the recommended
way on 3.10 or 3.11. Python 3.10 is also end-of-life. 1.0.4 supported
Python 3.10 to 3.12.

Before (1.0.4)::

   requires-python = ">= 3.10, < 3.13"
   apache-airflow>=2.10.0

After (2.0.0)::

   requires-python = ">=3.12, <3.15"
   apache-airflow>=3.1

The YellowDog SDK requirement is unchanged: ``yellowdog-sdk>=15.2.0``.

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
``YellowDogArgumentError``.

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
raises ``YellowDogException``; 1.0.4 returned ``None`` and the task
succeeded. Configured worker pools already raised. The return value is now
always the worker pool ID (``str``).

**Templated IDs that render to "None" raise YellowDogArgumentError.** When an
upstream XCom value is missing, a templated field such as
``"{{ ti.xcom_pull(task_ids='x') }}"`` renders to the string ``"None"``. 1.0.4
used it as a real ID and failed later with an obscure platform error;
2.0.0 fails immediately with a clear message. Empty strings are treated as
not supplied.

**A missing ID or name fails when the DAG is parsed.** Operators and sensors
that identify a resource by ID or by namespace plus name now raise
``YellowDogArgumentError`` in the constructor if neither is supplied
(1.0.4 raised ``YellowDogException`` at run time). Sensors also reject an
empty ``target_states``.

.. code-block:: python

   # Before (1.0.4): failed at run time
   CancelWorkRequirement(task_id="cancel", connection_id="yd")

   # After (2.0.0): DAG import error; supply an ID or namespace + name
   CancelWorkRequirement(
       task_id="cancel",
       work_requirement_id="{{ ti.xcom_pull(task_ids='add_wr') }}",
   )

``YellowDogArgumentError`` subclasses both ``YellowDogException`` and
``ValueError``, so existing ``except YellowDogException`` handlers still work.

**Sensor subclassing: ``target_states_names`` removed.** The public
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
of these states now fails at once with ``YellowDogStateError`` instead of
waiting (1.0.4 kept poking until ``timeout``). ``YellowDogStateError`` is an
``AirflowFailException``, so the task is not retried and ``silent_fail`` does
not suppress it. The final states are:
``COMPLETED``, ``FAILED`` and ``CANCELLED`` (work requirements); ``SHUTDOWN``
and ``TERMINATED`` (worker pools); ``TERMINATED`` (compute requirements).
Listing a failure outcome such as ``FAILED`` in ``target_states`` just to stop
waiting made the DAG run succeed when the YellowDog work failed; wait for
``COMPLETED`` only, and the sensor fails if the work fails or is cancelled.
``target_states`` and ``failure_states`` must not overlap, and both must hold
the sensor's status enum (``YellowDogArgumentError`` otherwise).

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

**Type stub files (.pyi) are no longer shipped.** Annotations are inline and
the package ships ``py.typed``; it is checked with pyright (strict) and
``pyright --verifytypes``.

Added
~~~~~

* A default connection ID, ``yellowdog_default``, used by the hook, operators
  and sensors when ``connection_id`` is not given.
* ``YellowDogHook.test_connection`` so that **Test** works in the Airflow
  connection UI, and context-manager support (``with YellowDogHook(...)``)
  that closes the client afterwards.
* ``TemplateCallable`` and ``Templated``, typed helpers for templated fields
  that may be supplied as callables (``yellowdog_provider.utils.templating``).
* ``YellowDogArgumentError`` (``yellowdog_provider.exceptions``).
* ``failure_states`` on ``WorkRequirementStateSensor``,
  ``WorkerPoolStateSensor`` and ``ComputeRequirementStateSensor``, and
  ``YellowDogStateError`` (``yellowdog_provider.exceptions``, also an
  ``AirflowFailException``), raised when a sensor's resource reaches a failure
  state; see :doc:`sensors`.
* Full Airflow provider metadata (``get_provider_info``: integrations, hooks,
  operators and sensors), so Airflow discovers the provider and its
  connection type.
* Complete type annotations, validated in CI.
* Example DAGs: ``yd-quickstart.py`` and a params-driven ``yd-provider-test.py``
  (connection, namespace, compute requirement template, image family, and an
  optional ``user_data`` parameter for instance userdata).
* Restructured documentation: concepts, quick start, connections, templating
  and troubleshooting pages, and API signatures in the reference.

Fixed
~~~~~

* ``AddPopulatedWorkRequirement`` now creates task groups in the order given.
  1.0.4 reversed the list before submitting it, so groups were created in
  reverse order.
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
