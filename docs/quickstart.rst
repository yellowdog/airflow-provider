Quick start
===========

This walk-through runs a DAG with a single Airflow task,
:class:`~yellowdog_provider.operators.yellowdog_operators.RunWorkRequirement`,
on a YellowDog worker pool that is already running. The Airflow task submits
a work requirement containing one YellowDog task (a ``docker`` task that runs
the ``hello-world`` container), then waits until the work requirement
completes. To provision the pool from the DAG as well, see :doc:`workflow`.

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
  Airflow calls them when the ``run`` task runs, passing the run's
  context, so they can use the run's params and ``run_id`` (see
  :doc:`templating`). The work requirement's name includes the ``run_id``,
  so every run creates a uniquely named work requirement.
- The task group's run specification asks for the ``docker`` task type and
  your worker tag, so only your pool's workers will claim it.
- ``run``
  (:class:`~yellowdog_provider.operators.yellowdog_operators.RunWorkRequirement`)
  creates the work requirement, its task group and task, then checks the work
  requirement's status every 30 seconds, for up to 30 minutes, until it is
  ``COMPLETED``. If the work requirement reaches ``FAILED`` or ``CANCELLED``
  instead, the task fails at once with :exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogStateError`
  (see :ref:`failure states <failure-states>`). If the wait times out or the
  task is killed, the work requirement is cancelled.

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

- ``run`` logs the work requirement's namespace and name and, a few seconds
  after it starts, its ID (``Work requirement '<namespace>/<name>' (ID
  '...'): submitted``). The new work requirement is visible in the YellowDog
  Portal, and once the task has finished the task's *YellowDog work
  requirement* link opens it (see :doc:`connections`).
- ``run`` logs the status on each check (``Current status is 'RUNNING' ...
  waiting for 30 seconds``) until the task has run. On a pool that is already
  up, ``hello-world`` finishes quickly, so the task usually succeeds within a
  few checks.
- When the work requirement completes, ``run`` logs ``Reached status
  'COMPLETED'`` and succeeds, and so does the DAG run.
- If the work fails or is cancelled, ``run`` fails with
  :exc:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogStateError` (for example ``Work requirement
  'airflow-quickstart/quickstart-...' (ID '...') reached status 'FAILED',
  which is a failure state (target states: 'COMPLETED')``), and the DAG run
  fails. Look at the task's details in the YellowDog Portal to see why. To
  wait for the work to finish whatever the outcome, list ``FAILED`` and
  ``CANCELLED`` in ``target_states`` too.

If ``run`` keeps reporting ``RUNNING``, no worker has claimed the task: check
that your pool is up, supports the ``docker`` task type, and carries exactly
the worker tag you passed. See :doc:`troubleshooting` for other problems.

Next steps
----------

- :doc:`workflow` shows the usual three-task DAG (provision a pool, run work
  on it, shut it down) and explains deferral, retries and clean-up.
- :doc:`operators` and :doc:`sensors` describe every class and argument.
- :doc:`templating` explains callables and XCom-to-ID patterns.
- The package's full example DAG,
  ``yellowdog_provider/example_dags/yd-provider-test.py``, exercises the
  workflow operators: it provisions its own worker pool (a setup task), runs
  two work requirements on it, and shuts it down (a teardown task). Its behaviour is driven by DAG params (connection ID,
  namespace, compute requirement template, image family, task type and
  arguments, timeout and poke interval, whether to defer, maximum worker pool
  nodes, userdata), whose defaults must be changed to match items in your YellowDog
  account.
