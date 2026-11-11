Concepts
========

The provider joins two systems, each with its own vocabulary. This page gives
a short primer on each, then shows how the provider's classes map onto them.
Skip the primer for the side you already know.

.. contents:: On this page
   :local:
   :depth: 2

Airflow primer (for YellowDog users)
------------------------------------

Apache Airflow is a workflow orchestrator. You describe a workflow in Python,
and Airflow's scheduler runs each step at the right time, in the right order,
with retries and a record of what happened. Airflow itself does not usually do
heavy computation; it tells other systems (such as YellowDog) what to do and
waits for them.

DAG
   A *Directed Acyclic Graph*: one workflow, defined in a Python file in
   Airflow's DAGs folder. A DAG has an ID, an optional schedule, and a set of
   tasks with dependencies between them (``a >> b`` means "run ``b`` after
   ``a``"). Each execution of a DAG is a *DAG run*, identified by a
   ``run_id``.

Task
   One step in a DAG. A task is an instance of an *operator* class, given a
   ``task_id`` that is unique within the DAG. Each run of a task is a *task
   instance*, which succeeds, fails, or is retried according to its
   ``retries`` setting.

Operator vs sensor
   An *operator* performs an action and finishes (for example, "submit this
   work requirement"). A *sensor* is a special operator that waits for a
   condition to become true (for example, "this work requirement has
   completed"), checking periodically.

Poke interval, timeout and mode
   A sensor checks its condition (*pokes*) every ``poke_interval`` seconds.
   If the condition is not met within ``timeout`` seconds the sensor fails.
   In the default ``mode="poke"`` the sensor occupies a worker slot for the
   whole wait; with ``mode="reschedule"`` it releases the slot between
   pokes, which is better for long waits.

XCom
   *Cross-communication*: small values passed between tasks. When an operator
   returns a value, Airflow stores it as the task's XCom under the key
   ``return_value``. A downstream task can read it with
   ``ti.xcom_pull(task_ids="...")``, typically inside a Jinja template.

Connection
   A named, stored set of credentials and endpoint details, managed by
   Airflow (UI, CLI, environment variables or a secrets backend) so that
   secrets never appear in DAG code. Each connection has a *type*; this
   provider adds the ``yellowdog`` type.

Params
   Run-time parameters declared on the DAG (``params={"namespace": "..."}``)
   with defaults that can be overridden when a run is triggered. Templates
   read them as ``{{ params.namespace }}``.

Templating
   Some operator arguments are *templated fields*: Airflow renders them with
   Jinja just before the task runs, so ``"{{ params.namespace }}"`` or
   ``"{{ ti.xcom_pull(task_ids='submit') }}"`` become real values. A
   templated field may also be given a Python callable, which Airflow calls
   to produce the value. See :doc:`templating`.

Trigger rules (for teardown)
   By default a task runs only if all its upstream tasks succeeded
   (``all_success``). Clean-up tasks, such as shutting down a worker pool,
   should run even when something upstream failed, so give them
   ``trigger_rule=TriggerRule.ALL_DONE``.

For depth, see the Airflow documentation on `DAGs
<https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html>`_,
`operators
<https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/operators.html>`_,
`sensors
<https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html>`_,
`XComs
<https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html>`_,
`connections
<https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html>`_,
`params
<https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/params.html>`_
and `templates
<https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html>`_.

YellowDog primer (for Airflow users)
------------------------------------

YellowDog is a platform for running large numbers of tasks on compute that it
provisions for you (across cloud providers and regions) or that you supply.
You describe *work* (what to run) separately from *workers* (where it can
run), and YellowDog matches the two.

Application, key and secret
   An *Application* is a non-human identity in a YellowDog account, used for
   API access. It has a *key ID* and a *key secret*, and its permissions come
   from the groups it belongs to. The provider authenticates as an
   Application; you store its key and secret in an Airflow connection.

Namespace
   A label that groups related YellowDog resources (work requirements,
   worker pools, compute requirements). Names are unique within a namespace,
   so a resource can be identified either by its YellowDog *ID* or by its
   *namespace and name*.

Work requirement, task group, task
   The unit of work submitted to YellowDog is a *work requirement*. It
   contains one or more *task groups*, and each task group contains *tasks*.
   A task is a single job: a task type plus arguments, environment
   variables and so on. Task groups let tasks with the same requirements be
   scheduled together. In the SDK these are the ``WorkRequirement``,
   ``TaskGroup`` and ``Task`` classes.

Run specification and task types
   Each task group has a ``RunSpecification`` saying what its tasks need:
   at minimum the *task types* the tasks use (``taskTypes``), and
   optionally constraints such as worker tags, instance types, regions,
   retries and timeouts. A *task type* is a name (for example ``docker``)
   that a worker advertises it can run; the worker's configuration maps the
   task type to the actual command. Each task's ``taskType`` must be one of
   its task group's ``taskTypes``.

Worker tags
   A free-form label on a worker pool's workers. Listing a tag in a run
   specification's ``workerTags`` restricts the task group to workers
   carrying that tag, which is how you direct work to a particular pool.

Worker pool
   A set of *workers* that claim task groups and execute their tasks. A
   *provisioned* worker pool is one YellowDog creates for you on cloud
   instances; a *configured* worker pool is made of machines you run
   yourself, which register with YellowDog. A pool's status is ``IDLE``
   while its workers are up but none has claimed work, and ``RUNNING`` once
   one or more workers are claimed; so a pool that is ready for work is in
   ``IDLE`` or ``RUNNING``.

Compute requirement and template
   Behind every provisioned worker pool is a *compute requirement*: the set
   of instances YellowDog provisions. It is created from a *compute
   requirement template*, which defines the instance types, providers,
   regions and so on. When provisioning, you say which template to use, the
   name and namespace for the new requirement and how many instances you
   want, in a ``ComputeRequirementTemplateUsage``. The worker pool's own
   settings (worker tag, node counts, idle shutdown timers) go in a
   ``ProvisionedWorkerPoolProperties``.

Image family
   A named family of machine images (for example, one image per cloud
   provider and region) from which instances are booted. A compute
   requirement refers to one through ``imagesId`` in the template usage, or
   in the template itself. The images determine which task types the
   workers can run.

For depth, see the `YellowDog documentation <https://docs.yellowdog.ai>`_ and
the `YellowDog Python SDK <https://pypi.org/project/yellowdog-sdk>`_.

How they fit together
---------------------

An Airflow DAG drives YellowDog through the provider's classes. Every class
talks to YellowDog through a connection; operators perform one YellowDog
action and return the affected resource's ID as their XCom; sensors wait for a
resource to reach a state.

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Provider class
     - What it does, in the terms above
   * - :class:`~yellowdog_provider.hooks.yellowdog_hooks.YellowDogHook`
     - Reads a ``yellowdog`` Airflow **connection** (an Application's key and
       secret, plus an optional Platform API URL) and returns an SDK
       ``PlatformClient``. Used by every operator and sensor; can also be used
       directly in your own tasks.
   * - :class:`~yellowdog_provider.operators.yellowdog_operators.AddWorkRequirement`
     - Submits a **work requirement**. Returns its ID.
   * - :class:`~yellowdog_provider.operators.yellowdog_operators.AddTaskGroupsToWorkRequirement`
     - Adds **task groups** (each with a **run specification**) to an
       existing work requirement. Returns the new task groups' IDs.
   * - :class:`~yellowdog_provider.operators.yellowdog_operators.AddTasksToTaskGroup`
     - Adds **tasks** to an existing task group. Returns the new tasks' IDs.
   * - :class:`~yellowdog_provider.operators.yellowdog_operators.AddPopulatedWorkRequirement`
     - Submits a work requirement together with its task groups and tasks, in
       one Airflow task. Returns the work requirement's ID.
   * - :class:`~yellowdog_provider.operators.yellowdog_operators.CancelWorkRequirement`
     - Cancels a work requirement, optionally aborting running tasks.
   * - :class:`~yellowdog_provider.operators.yellowdog_operators.ProvisionWorkerPool`
     - Provisions a **worker pool** and its **compute requirement** from a
       **compute requirement template**. Returns the worker pool's ID.
   * - :class:`~yellowdog_provider.operators.yellowdog_operators.ShutdownProvisionedWorkerPool`
     - Shuts down a provisioned worker pool, optionally terminating its
       compute requirement immediately. Typically a teardown task with
       ``trigger_rule=TriggerRule.ALL_DONE``.
   * - :class:`~yellowdog_provider.sensors.yellowdog_sensors.WorkRequirementStateSensor`
     - **Sensor**: waits for a work requirement to reach given states (for
       example ``COMPLETED``), failing if it reaches a failure state (by
       default, ``FAILED`` or ``CANCELLED``).
   * - :class:`~yellowdog_provider.sensors.yellowdog_sensors.WorkerPoolStateSensor`
     - **Sensor**: waits for a worker pool to reach given states (for example
       ``IDLE`` or ``RUNNING`` when waiting for it to be ready).
   * - :class:`~yellowdog_provider.sensors.yellowdog_sensors.ComputeRequirementStateSensor`
     - **Sensor**: waits for a compute requirement to reach given states (for
       example ``TERMINATED``).

Resources are identified either by **ID** (usually pulled from an upstream
task's **XCom**) or by **namespace and name** (often built from **params** or
the ``run_id``). Arguments that hold SDK objects are supplied either as
objects or as **callables** that build them at run time; see
:doc:`templating`.

A typical DAG:

#. ``ProvisionWorkerPool`` creates a pool whose workers carry a worker tag.
#. ``WorkerPoolStateSensor`` waits for the pool to reach ``IDLE`` or
   ``RUNNING``.
#. ``AddPopulatedWorkRequirement`` submits work whose run specification
   targets that worker tag.
#. ``WorkRequirementStateSensor`` waits for the work to finish.
#. ``ShutdownProvisionedWorkerPool`` (trigger rule ``ALL_DONE``) shuts the
   pool down whatever happened upstream.
