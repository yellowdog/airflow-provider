Troubleshooting
===============

.. contents:: On this page
   :local:

How errors surface
------------------

The provider does not hide errors: anything that goes wrong fails the Airflow
task, with the exception in the task log, and Airflow retries the task
according to its ``retries`` setting (except for :exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogStateError`,
below). The exceptions come from the provider itself and from the YellowDog
SDK.

:class:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogException`
   The base class for errors raised by the provider itself: for example a
   connection with no key ID or secret, an attempt to shut down a worker pool
   that is not a provisioned pool, or a callable that was never rendered.

:class:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogArgumentError`
   A subclass of both :exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogException` and Python's ``ValueError``,
   raised when an operator or sensor argument is missing, malformed or
   inconsistent: neither an ID nor a complete name given, an empty
   ``target_states`` list, ``target_states`` or ``failure_states`` holding
   values of the wrong type or overlapping, an ID that rendered to
   ``"None"``, or a work requirement with preset task groups passed to
   ``AddPopulatedWorkRequirement``. When raised while the DAG file is parsed
   (missing arguments, bad ``target_states`` or ``failure_states``), it
   appears as a DAG import error in the Airflow UI instead of a task failure.

:class:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogStateError`
   A subclass of :exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogException`, raised by a sensor whose resource has
   reached one of its ``failure_states`` (by default, a final state that is
   not a target, such as ``FAILED`` or ``CANCELLED`` when waiting for
   ``COMPLETED``). The message names the resource, its current state and the
   target states. It is also Airflow's ``AirflowFailException``, so the
   sensor fails without being retried, even with ``silent_fail=True``; see
   :ref:`failure states <failure-states>`.

:class:`~yellowdog_provider.exceptions.yellowdog_exceptions.WaitTimeoutError`
   A subclass of :exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogException`, raised when an operator's own wait
   (``RunWorkRequirement``, ``ProvisionWorkerPool`` with ``wait_until_ready``,
   ``ShutdownProvisionedWorkerPool`` with ``wait``) times out. The message
   names the resource kind, ID, last status and the timeout (``Timed out
   after 3600 seconds waiting for worker pool ID '...'; last status was
   'PENDING'``); the same values are attributes (``kind``, ``resource_id``,
   ``last_state``, ``timeout``). Before raising it, ``RunWorkRequirement``
   cancels its work requirement and ``ProvisionWorkerPool`` shuts down its
   pool, so unlike ``YellowDogStateError`` it is retried according to the
   task's ``retries`` setting, and the retry starts afresh. Sensors time out
   with Airflow's ``AirflowSensorTimeout`` instead.

Errors from the YellowDog SDK
   Errors returned by the YellowDog Platform are raised by the YellowDog SDK
   and propagate unchanged. Most are ``requests.exceptions.HTTPError``, whose
   message includes the HTTP status, the URL and the response body (verified
   for 401 and 404). Validation errors, such as a duplicate task-group
   name, are raised as
   ``yellowdog_client.model.exceptions.InvalidRequestException``
   (SDK exception types subclass ``BaseCustomException``, not ``HTTPError``),
   so catch both if you handle SDK errors. The
   provider catches SDK errors only in these cases: an "invalid transition"
   response to a cancellation (the work requirement has already finished), a
   pool shutdown (already shut down) or a compute requirement termination
   (already terminating or terminated) is treated as success; while an
   operator or a deferred sensor waits, up to three consecutive
   :ref:`transient errors <transient-errors>` are retried; and clean-up
   after a failure (cancelling or terminating) is best-effort, so its own
   errors are logged and the original error is raised.

To handle provider errors yourself, for example in a subclass or in your own
task using the hook, catch :exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogException`, and SDK errors
separately.

Common problems
---------------

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Symptom
     - Likely cause
     - What to do
   * - A ``401`` or ``403`` ``HTTPError``, or an SDK
       ``NotAuthorisedException`` (``yellowdog_client.model.exceptions``), on
       every task, or a failed connection test
     - Wrong Application key ID or secret, the wrong Platform API URL, or an
       Application without permission for the namespace.
     - Check the connection's fields (see :doc:`connections`) and the
       Application's groups in the YellowDog Portal. Use the connection test.
   * - ``Connection '<id>' has no Application Key ID`` (or ``... Key Secret``)
     - The connection exists but a credential field is empty.
     - Fill in the Application Key ID and Application Key Secret.
   * - Airflow reports that the connection ID is not defined
     - No connection with that ID, for example ``yellowdog_default`` was never
       created, or a templated ``connection_id`` rendered to something
       unexpected.
     - Create the connection, or check the ``connection_id`` value in the
       task's *Rendered Template* view.
   * - ``404`` ``HTTPError`` when a resource is given by namespace and name
     - No resource with that namespace and name, often because the name was
       built differently from the task that created it, or the resource has
       not been created yet.
     - Compare the names in the task logs (``Retrieving ... '<namespace>/<name>'``)
       with the YellowDog Portal. Prefer passing IDs through XCom.
   * - ``Templated field '...' rendered to 'None'``
     - A field such as ``work_requirement_id`` used ``xcom_pull`` and the
       upstream task pushed nothing (failed, skipped, or returned ``None``).
     - Look at the upstream task first. For teardown tasks, see
       :ref:`none-ids`.
   * - ``For a <kind>, either '<id_field>' or all of ... must be supplied``
     - Neither the ID nor all the name arguments were given, or they rendered
       to empty strings.
     - Supply the ID, or every name argument.
   * - Duplicate-name error from YellowDog when a task is retried or a DAG is
       re-run
     - ``RunWorkRequirement``, ``AddPopulatedWorkRequirement`` and
       ``AddWorkRequirement`` never fail this way (see :ref:`safe-retries`),
       but other names are not managed: a compute requirement name used by
       ``ProvisionWorkerPool``, or task group names added again by a retried
       ``AddTaskGroupsToWorkRequirement``.
     - Build compute requirement names from the run, for example from
       ``run_id``, as in the :doc:`workflow` example. Prefer
       ``RunWorkRequirement`` to the step-by-step operators.
   * - A work requirement is named ``<name>-try2`` (or
       ``<name>-1a2b-try2``) instead of the name you supplied
     - A retry or a clear submitted a fresh copy, or a work requirement of
       another task or run already had the name. See :ref:`safe-retries`.
     - Nothing to fix. Downstream tasks should use the ID from XCom rather
       than the name.
   * - ``Replacing tag '...' with the run stamp '...'`` in the log
     - The ``WorkRequirement`` had a ``tag``; the submitting operators
       replace it with the run stamp.
     - Remove the tag, or record the information elsewhere (for example in
       the work requirement's name or the tasks' environment).
   * - A deferred task never finishes, then fails with a deferral timeout
     - No triggerer is running, so nothing polls the resource. Airflow fails
       the task when its own deferral timeout (the operator's ``timeout``
       plus 5 minutes; a sensor's ``timeout``) passes, without letting the
       operator clean up.
     - Run ``airflow triggerer``, or use ``deferrable=False`` (or set
       ``[operators] default_deferrable = False``). Work left running is
       cancelled by the teardown (:ref:`teardown-cancellation`).
   * - ``Transient error checking the status of ... (1/3)`` warnings
     - A connection problem, timeout, HTTP 429 or 5xx response while
       waiting. Up to three in a row are retried.
     - Nothing, unless the wait then fails; check connectivity to the
       Platform API URL.
   * - Work requirements cancelled by the teardown (``Cancelling N
       unfinished work requirement(s) of this DAG run``)
     - ``ShutdownProvisionedWorkerPool`` cancels the run's unfinished work
       requirements before shutting the pool down
       (``cancel_run_work_requirements=True``, the default).
     - Expected if the work was still running when the teardown started,
       for example after a failure elsewhere. To keep it, pass
       ``cancel_run_work_requirements=False``.
   * - Work of **another Airflow environment** cancelled, resumed or treated
       as already completed
     - Two Airflow environments use the same YellowDog account without
       distinct ``[yellowdog] deployment_id`` settings, and had runs with the
       same DAG and run IDs.
     - Set a different ``deployment_id`` in each environment; see
       :ref:`deployment-id`.
   * - Worker pools left running after a failed or killed DAG run
     - A ``ProvisionWorkerPool`` task was killed while deferred on Airflow 3.1
       or 3.2, or its worker crashed while it waited (a retry then
       provisions a second pool).
     - Give pools an idle shutdown (``idlePoolShutdown``,
       ``idleNodeShutdown``), or use ``deferrable=False`` for the
       provisioning task on those Airflow versions; see :ref:`cancellation`.
   * - Teardown task succeeds although the pool was already shut down
     - The pool shut itself down (idle shutdown) or an earlier try shut it
       down; the teardown treats this as success.
     - Nothing to fix.
   * - Sensor fails with ``AirflowSensorTimeout``
     - Neither a target state nor a failure state was reached within
       ``timeout``. Common causes: no
       worker has the task types or worker tag the task group asks for; the
       pool is still starting; ``WorkerPoolStateSensor`` waiting for
       ``RUNNING`` alone on a pool with no work (use ``IDLE`` and
       ``RUNNING``).
     - Check the resource in the YellowDog Portal. Check the run
       specification's ``taskTypes`` and ``workerTags`` against the pool.
       Increase ``timeout`` if the work is just slow.
   * - Sensor fails with :exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogStateError`
     - The resource reached a failure state, for example a work requirement
       ``FAILED`` or ``CANCELLED`` while the sensor waited for ``COMPLETED``,
       or a worker pool shut down before becoming ``IDLE``.
     - Investigate the resource in the YellowDog Portal (for work, the failed
       tasks' output). If the outcome is acceptable, add it to
       ``target_states``, or pass ``failure_states=[]`` to wait only for the
       targets. The sensor is not retried: a final state will not change.
   * - Sensor fails on its first poke instead of waiting
     - An exception during a poke (for example a ``404`` because the
       resource does not exist yet) fails the sensor straight away.
     - Run the sensor after the task that creates the resource, or set
       ``silent_fail=True``. See :doc:`sensors`.
   * - ``TypeError: ... got an unexpected keyword argument 'context'``
     - A callable whose parameters are not named exactly ``context`` and
       ``jinja_env``.
     - Rename the parameters. Annotate callables with ``TemplateCallable`` to
       catch this in a type checker; see :doc:`templating`.
   * - ``Templated field '...' has not been rendered``
     - ``execute()`` or ``poke()`` was called on an operator holding a
       callable without Airflow rendering it first, typically in a unit test.
     - Call ``render_template_fields(context)`` first, or pass plain values.
   * - ``Worker pool '...' is a configured worker pool``
     - ``ShutdownProvisionedWorkerPool`` was pointed at a configured worker
       pool, which it cannot shut down.
     - Manage configured pools outside this operator.
   * - Work requirement created with a name like ``{{ params.name }}``, or
       rejected for an invalid name
     - Jinja was placed inside an SDK object, where it is not rendered.
     - Build the object in a callable; see :doc:`templating`.

Testing the connection
----------------------

The quickest check of credentials and URL is Airflow's connection test, which
makes one authenticated request to YellowDog. It must be enabled first
(``AIRFLOW__CORE__TEST_CONNECTION=Enabled``); then use the **Test** button in
the UI, or:

.. code-block:: console

   $ airflow connections test yellowdog_default

See :doc:`connections` for details.

Logging
-------

Operators and sensors log what they do to the Airflow task log at ``INFO``
level, including:

- the resource being looked up, by ID or by ``namespace/name``, and the ID or
  name it resolved to;
- each action taken and the resulting ID (``Added work requirement ID
  '...'``, ``Provisioned worker pool ID '...'``);
- for the submitting operators, what was found from earlier attempts and
  what was done (``Work requirement '<namespace>/<name>' (ID '...'):
  submitted``, ``resumed`` or ``already completed``);
- for waits, the current status on every check (``Current status is
  'RUNNING' ... waiting for 60 seconds``) and ``Reached status '...'``;
- for sensors, the target states and the current status on every poke;
- warnings for clean-up (``Cancelling work requirement ID '...', aborting
  running tasks (...)``, ``Shutting down worker pool ID '...' and
  terminating its compute requirement (...)``);
- the Platform API URL, when it is not the default;
- warnings, for example when both an ID and a name were supplied, or when a
  cancellation was ignored because the work requirement had already finished.

Open the task instance's log in the Airflow UI to see them. To find the same
resources in the YellowDog Portal, use the IDs or names from the log.
