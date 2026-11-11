Quick start
===========

This walk-through runs one task on YellowDog from an Airflow DAG: it submits
a work requirement containing a single ``docker`` task that runs the
``hello-world`` container, then waits for the work requirement to complete.

.. contents:: On this page
   :local:

Before you start
----------------

You need:

- A working Airflow 3.1+ installation on Python 3.12–3.14 (for a local trial,
  ``airflow standalone`` is enough).
- A YellowDog **Application** key ID and secret. Create an Application in the
  YellowDog Portal and give it access to the namespace you will use.
- A YellowDog **worker pool** that is up (status ``IDLE`` or ``RUNNING``),
  whose workers support the ``docker`` task type and carry a **worker tag**
  you know. You can create one in the YellowDog Portal, or provision one from
  Airflow with
  :class:`~yellowdog_provider.operators.yellowdog_operators.ProvisionWorkerPool`
  (see :doc:`operators`, and the full example DAG described at the end of
  this page).

If these terms are new, read :doc:`concepts` first.

1. Install the provider
-----------------------

In the Python environment that runs Airflow::

   pip install --upgrade yellowdog-airflow-provider

Restart the Airflow components so they pick up the new connection type.

2. Create the connection
------------------------

Operators and sensors use the connection ID ``yellowdog_default`` unless told
otherwise, so create a connection with that ID.

**In the Airflow UI:** go to *Admin → Connections*, add a connection, and fill
in:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Field
     - Value
   * - Connection Id
     - ``yellowdog_default``
   * - Connection Type
     - ``YellowDog``
   * - Platform API URL
     - Leave empty to use ``https://api.yellowdog.ai``
   * - Application Key ID
     - Your Application's key ID
   * - Application Key Secret
     - Your Application's key secret

**Or with the CLI:**

.. code-block:: console

   $ airflow connections add yellowdog_default \
       --conn-type yellowdog \
       --conn-login '<KEY_ID>' \
       --conn-password '<KEY_SECRET>'

Other ways to create connections (environment variables, secrets backends)
are covered in :doc:`connections`.

3. Test the connection
----------------------

Airflow disables connection testing by default. To enable it, set
``test_connection = Enabled`` in the ``[core]`` section of ``airflow.cfg``, or
set the environment variable ``AIRFLOW__CORE__TEST_CONNECTION=Enabled``, and
restart Airflow. Then either press **Test** on the connection's page in the
UI, or run:

.. code-block:: console

   $ airflow connections test yellowdog_default

On success the UI shows ``Successfully connected to
https://api.yellowdog.ai`` and the CLI prints ``Connection success!``. On
failure both show the error from YellowDog (for example, a ``401`` for a wrong
key or secret); see :doc:`troubleshooting`.

4. Add the DAG
--------------

Save the following as ``yd-quickstart.py`` in your Airflow DAGs folder. (It is
also shipped in the package, as
``yellowdog_provider/example_dags/yd-quickstart.py``.)

.. literalinclude:: ../yellowdog_provider/example_dags/yd-quickstart.py
   :language: python

What it does:

- ``make_work_requirement`` and ``make_groups_and_tasks`` are *callables*.
  Airflow calls them when the ``submit`` task runs, passing the run's
  context, so they can use the run's params and ``run_id`` (see
  :doc:`templating`). The work requirement's name includes the ``run_id``,
  so every run creates a uniquely named work requirement.
- The task group's run specification asks for the ``docker`` task type and
  your worker tag, so only your pool's workers will claim it.
- ``submit``
  (:class:`~yellowdog_provider.operators.yellowdog_operators.AddPopulatedWorkRequirement`)
  creates the work requirement, its task group and task, and returns the
  work requirement's ID, which Airflow stores as the task's XCom.
- ``wait``
  (:class:`~yellowdog_provider.sensors.yellowdog_sensors.WorkRequirementStateSensor`)
  pulls that ID from XCom and checks the work requirement's status every 30
  seconds, for up to 30 minutes, until it is ``COMPLETED``. If the work
  requirement reaches ``FAILED`` or ``CANCELLED`` instead, the sensor fails
  at once with ``YellowDogStateError`` (see :ref:`failure states <failure-states>`).

5. Trigger a run
----------------

New DAGs are paused by default. Unpause the DAG and trigger it, supplying your
pool's worker tag (and, if you need a different one, the namespace):

.. code-block:: console

   $ airflow dags unpause yellowdog-quickstart
   $ airflow dags trigger yellowdog-quickstart \
       --conf '{"worker_tag": "<YOUR_WORKER_TAG>", "namespace": "airflow-quickstart"}'

Or, in the UI, unpause ``yellowdog-quickstart``, press **Trigger**, and edit
the params in the form.

What to expect
--------------

- ``submit`` succeeds within a few seconds. Its log shows the work
  requirement's namespace and name, then ``Added work requirement ID '...'``.
  The new work requirement is visible in the YellowDog Portal.
- ``wait`` logs the status on each poke (``Current status is 'RUNNING' ...``)
  until the task has run. On a pool that is already up, ``hello-world``
  finishes quickly, so the sensor usually succeeds within a few pokes.
- When the work requirement completes, ``wait`` logs ``... has reached
  status 'COMPLETED'`` and succeeds, and so does the DAG run.
- If the work fails or is cancelled, ``wait`` fails with
  ``YellowDogStateError`` (for example ``Work requirement
  'airflow-quickstart/quickstart-...' (ID '...') reached status 'FAILED',
  which is a failure state (target states: 'COMPLETED')``), and the DAG run
  fails. Look at the task's details in the YellowDog Portal to see why. To
  wait for the work to finish whatever the outcome, list ``FAILED`` and
  ``CANCELLED`` in ``target_states`` too.

If ``wait`` keeps reporting ``RUNNING``, no worker has claimed the task: check
that your pool is up, supports the ``docker`` task type, and carries exactly
the worker tag you passed. See :doc:`troubleshooting` for other problems.

Next steps
----------

- :doc:`operators` and :doc:`sensors` describe every class and argument.
- :doc:`templating` explains callables and XCom-to-ID patterns.
- The package's full example DAG,
  ``yellowdog_provider/example_dags/yd-provider-test.py``, exercises every
  operator and sensor: it provisions its own worker pool, submits work in
  several ways, waits for it, and tears everything down with ``ALL_DONE``
  trigger rules. Its behaviour is driven by DAG params (connection ID,
  namespace, compute requirement template, image family, task type and
  arguments, sensor timeout and poke interval, maximum worker pool nodes,
  userdata), whose defaults must be changed to match items in your YellowDog
  account.
