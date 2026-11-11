Sensors
=======

All sensors live in ``yellowdog_provider.sensors.yellowdog_sensors``::

   from yellowdog_provider.sensors.yellowdog_sensors import (
       ComputeRequirementStateSensor,
       WorkerPoolStateSensor,
       WorkRequirementStateSensor,
   )

Each sensor looks up one YellowDog resource on every poke and succeeds when
the resource's status is one of the ``target_states`` you give. It fails at
once if the status is one of its ``failure_states`` instead: by default, the
final states (from which the resource never moves) that are not targets.

.. contents:: On this page
   :local:
   :depth: 1

Common behaviour
----------------

**Keyword-only arguments.** Every argument must be passed by name.

**Examples** on this page assume these imports::

   from airflow.sdk import TriggerRule
   from yellowdog_client.model import (
       ComputeRequirementStatus,
       WorkerPoolStatus,
       WorkRequirementStatus,
   )

**Common parameters.** ``task_id`` (``str``) is required, as for any Airflow
task. As well as the parameters listed for each sensor, every sensor takes:

.. list-table::
   :header-rows: 1
   :widths: 24 26 18 10 10

   * - Parameter
     - Type
     - Default
     - Jinja
     - Callable
   * - ``connection_id``
     - ``str``
     - ``"yellowdog_default"``
     - yes
     - yes
   * - ``poke_interval``
     - ``float`` (seconds)
     - ``60``
     - no
     - no

and all the standard Airflow ``BaseSensorOperator`` arguments, notably:

- ``timeout``: seconds before the sensor fails with a timeout. The default
  comes from Airflow's ``[sensors] default_timeout`` setting (7 days unless
  changed), so set it explicitly.
- ``mode``: ``"poke"`` (default) holds a worker slot for the whole wait;
  ``"reschedule"`` frees it between pokes, and suits long waits with a poke
  interval of a minute or more.
- ``soft_fail``, ``silent_fail``, ``exponential_backoff``, ``max_wait``, and
  the usual ``BaseOperator`` arguments such as ``retries`` and
  ``trigger_rule``.

The provider's sensors do not support deferrable mode.

The *Jinja* and *Callable* columns have the same meaning as in
:doc:`operators`. ``target_states`` and ``failure_states`` are fixed when the
DAG is parsed.

**ID or namespace and name.** Each sensor identifies its resource either by ID
or by namespace and name, with :ref:`the same rules as the operators
<id-or-name>`: one or the other must be supplied (checked when the sensor is
constructed and again after rendering), the ID wins if both are given (with
a warning), an empty rendered value counts as not supplied, and a value that
renders to ``"None"`` raises
:class:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogArgumentError`.

**target_states** must be a non-empty list of the SDK status enum for the
resource; an empty list, or a value of another type (such as the string
``"COMPLETED"``), raises :exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogArgumentError` when the sensor is
constructed.

.. _failure-states:

**failure_states** is an optional, keyword-only list of the same status enum.
When a poke finds the resource in one of these states, the sensor pushes the
resource ID to XCom (as on any first poke) and then raises
:class:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogStateError`,
failing the task without waiting for ``timeout``. The error message names the
kind of resource, its ID, its namespace and name, its current state and the
target states; the same values are attributes of the exception
(``kind``, ``resource_id``, ``namespace``, ``name``, ``current_state``,
``target_states``).

- ``None`` (the default): every final state that is not in
  ``target_states``. The final states are:

  .. list-table::
     :header-rows: 1
     :widths: 34 66

     * - Sensor
       - Final states
     * - ``WorkRequirementStateSensor``
       - ``COMPLETED``, ``FAILED``, ``CANCELLED`` (the SDK's
         ``WorkRequirementStatus.finished`` states)
     * - ``WorkerPoolStateSensor``
       - ``SHUTDOWN``, ``TERMINATED`` (the SDK's
         ``WorkerPoolStatus.finished`` states)
     * - ``ComputeRequirementStateSensor``
       - ``TERMINATED`` (``STOPPED`` is not final: a stopped compute
         requirement can be restarted)

  So ``target_states=[WorkRequirementStatus.COMPLETED]`` fails on
  ``FAILED`` or ``CANCELLED``, while a worker pool sensor waiting for
  ``[SHUTDOWN, TERMINATED]`` has no failure states. The final-state tuples are
  available as ``WORK_REQUIREMENT_FINAL_STATES``,
  ``WORKER_POOL_FINAL_STATES`` and ``COMPUTE_REQUIREMENT_FINAL_STATES`` in
  ``yellowdog_provider.sensors.yellowdog_sensors``.
- An explicit list replaces the default; it may include non-final states
  (for example ``WorkRequirementStatus.HELD``).
- ``[]`` disables failure detection: the sensor waits only for
  ``target_states`` until ``timeout``, as in 1.x.

A state may not be in both lists: an overlap raises :exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogArgumentError`
(naming the overlapping states) when the sensor is constructed.

**Failure states and retries.** :exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogStateError` is also Airflow's
``AirflowFailException``, so Airflow fails the sensor task at once and does
not retry it, whatever its ``retries`` (a resource in a final state will not
change).

**Failure states and the fail arguments.** ``silent_fail=True`` does not
suppress a failure state: the task still fails. ``soft_fail=True`` or
``never_fail=True`` turns a failure state into a skipped task, as for a
timeout.

**XCom.** On its first poke, each sensor pushes the resolved resource ID to
XCom under the key ``return_value``. This happens regardless of
``do_xcom_push``, and means a sensor that looked a resource up by name gives
downstream tasks its ID: ``"{{ ti.xcom_pull(task_ids='<sensor_task_id>') }}"``.

**Errors during a poke.** If a poke raises (for example, the SDK's ``404``
because no resource has that name yet, or bad credentials), the sensor task
fails at once rather than waiting for the next poke; it is then retried
according to its ``retries``. If the resource may not exist yet when the
sensor starts, either start the sensor after the task that creates it, or set
``silent_fail=True`` so that errors are logged and poking continues (failure
states still fail the task; see above).

**Timeouts.** If neither a target state nor a failure state is reached
within ``timeout``, Airflow fails the sensor with ``AirflowSensorTimeout``.

WorkRequirementStateSensor
--------------------------

Waits for a work requirement to reach one of ``target_states``, failing if
it reaches one of ``failure_states``.

.. list-table::
   :header-rows: 1
   :widths: 24 26 18 10 10

   * - Parameter
     - Type
     - Default
     - Jinja
     - Callable
   * - ``target_states``
     - ``list[WorkRequirementStatus]``
     - required
     - no
     - no
   * - ``failure_states``
     - ``Sequence[WorkRequirementStatus] | None``
     - ``None`` (derived; :ref:`see above <failure-states>`)
     - no
     - no
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

``WorkRequirementStatus`` values: ``RUNNING``, ``HELD``, ``FINISHING``,
``COMPLETED``, ``FAILED``, ``CANCELLING``, ``CANCELLED``. To wait for the
work to succeed, use ``COMPLETED`` alone: the sensor then fails if the work
requirement fails or is cancelled. To wait for it to finish whatever the
outcome, use ``COMPLETED``, ``FAILED`` and ``CANCELLED`` (there are then no
failure states, and the sensor succeeds even if the work failed).

Example::

   wait = WorkRequirementStateSensor(
       task_id="wait-for-work",
       work_requirement_id="{{ ti.xcom_pull(task_ids='submit') }}",
       target_states=[WorkRequirementStatus.COMPLETED],
       poke_interval=30,
       timeout=60 * 60,
   )

.. _worker-pool-state-sensor:

WorkerPoolStateSensor
---------------------

Waits for a worker pool to reach one of ``target_states``, failing if it
reaches one of ``failure_states``.

.. list-table::
   :header-rows: 1
   :widths: 24 26 18 10 10

   * - Parameter
     - Type
     - Default
     - Jinja
     - Callable
   * - ``target_states``
     - ``list[WorkerPoolStatus]``
     - required
     - no
     - no
   * - ``failure_states``
     - ``Sequence[WorkerPoolStatus] | None``
     - ``None`` (derived; :ref:`see above <failure-states>`)
     - no
     - no
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

``WorkerPoolStatus`` values: ``PENDING``, ``CONFIGURING``, ``EMPTY``,
``IDLE``, ``RUNNING``, ``SHUTDOWN``, ``TERMINATED``.

To wait for a newly provisioned pool to be ready for work, use **both**
``IDLE`` and ``RUNNING``: ``RUNNING`` means one or more workers have claimed
work, so a ready pool with nothing to do stays at ``IDLE`` and a sensor
waiting for ``RUNNING`` alone may never succeed. By default such a sensor
fails if the pool shuts down (``SHUTDOWN`` or ``TERMINATED``) first. To wait
for a pool to go away, use ``SHUTDOWN`` and ``TERMINATED``.

Example::

   wait_for_pool = WorkerPoolStateSensor(
       task_id="wait-for-pool",
       worker_pool_id="{{ ti.xcom_pull(task_ids='provision-pool') }}",
       target_states=[WorkerPoolStatus.IDLE, WorkerPoolStatus.RUNNING],
       timeout=30 * 60,
   )

ComputeRequirementStateSensor
-----------------------------

Waits for a compute requirement to reach one of ``target_states``, failing
if it reaches one of ``failure_states`` (by default ``TERMINATED``, unless it
is a target). A
provisioned worker pool's compute requirement has the name and namespace given
in the ``ComputeRequirementTemplateUsage`` used to provision it.

.. list-table::
   :header-rows: 1
   :widths: 24 26 18 10 10

   * - Parameter
     - Type
     - Default
     - Jinja
     - Callable
   * - ``target_states``
     - ``list[ComputeRequirementStatus]``
     - required
     - no
     - no
   * - ``failure_states``
     - ``Sequence[ComputeRequirementStatus] | None``
     - ``None`` (derived; :ref:`see above <failure-states>`)
     - no
     - no
   * - ``compute_requirement_id``
     - ``str | None``
     - ``None``
     - yes
     - yes
   * - ``namespace``
     - ``str | None``
     - ``None``
     - yes
     - yes
   * - ``compute_requirement_name``
     - ``str | None``
     - ``None``
     - yes
     - yes

``ComputeRequirementStatus`` values: ``NEW``, ``PROVISIONING``, ``STARTING``,
``RUNNING``, ``STOPPING``, ``STOPPED``, ``TERMINATING``, ``TERMINATED``.

Example, confirming that instances have gone after a teardown::

   wait_for_termination = ComputeRequirementStateSensor(
       task_id="wait-for-termination",
       namespace="{{ params.namespace }}",
       compute_requirement_name="{{ params.pool_name }}",
       target_states=[ComputeRequirementStatus.TERMINATED],
       timeout=30 * 60,
       trigger_rule=TriggerRule.ALL_DONE,
   )
