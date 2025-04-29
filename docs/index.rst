YellowDog Provider for Apache Airflow
=====================================


Welcome to the documentation for the YellowDog Provider for Apache Airflow. This package integrates the `YellowDog Platform <https://yellowdog.ai>`_ into your Apache Airflow workflows. It provides:

- A **Hook** class to allow YellowDog Application credentials for accessing the YellowDog API to be stored in the Airflow connection database
- **Operator** classes that allow creation and control of YellowDog work requirements and worker pools
- **Sensor** classes that allow the states of work requirements and worker pools to be monitored

The YellowDog Airflow Provider depends on the `YellowDog Python SDK <https://pypi.org/project/yellowdog-sdk>`_, and the provider's usage model leverages several Python classes from the SDK.

Contents
========

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   yellowdog_provider

Installation
============

Install or upgrade from PyPI, e.g.::

   pip install --upgrade yellowdog-airflow-provider


Usage
=====

The YellowDog Hook: Connecting to YellowDog
-------------------------------------------

Creating a YellowDog Connection
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The YellowDog Hook class ``YellowDogHook`` is used to contain the Platform API URL, Key ID, and Secret ID of a YellowDog Application in an Airflow Connection. Connections are used for secure interaction with the YellowDog Platform.

A YellowDog Connection has four properties:

#. An optional Platform API URL: this is the URL for the API of the YellowDog Platform; if not set, the default URL is used.
#. The Key ID of a YellowDog Application
#. The Secret Key of a YellowDog Application
#. An optional description of the Connection, such as the purpose and scope of the YellowDog Application

Connections can be created via the Airflow Portal by selecting the ``YellowDog`` Connection Type, or by using the CLI, e.g., to create a connection named ``my-yellowdog-connection``::

   % airflow connections add \
        --conn-type=yellowdog \
        --conn-host="https://api.yellowdog.ai" \
        --conn-login=<KEY_ID> \
        --conn-password=<KEY_SECRET> \
        --conn-description='My YellowDog Application' \
        my-yellowdog-connection


Here, the ``host`` property is used to set the platform API URL, ``login`` is used to set the Application Key ID, and ``password`` is used to set the Application Secret Key. As noted above, the ``host`` and ``description`` properties are optional.

Using a YellowDog Connection
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When the ``get_conn()`` method of a `YellowDogHook` object is called, it returns a YellowDog ``PlatformClient`` object that is used to identify the Platform API URL and to authenticate API calls. The ``PlatformClient`` object is used internally within the YellowDog provider classes, and can also be used directly within user code to utilise the full capabilities of the YellowDog SDK.

The YellowDog Airflow Operators
-------------------------------

The YellowDog provider includes Airflow operators for creating and managing Yellowdog work requirements and worker pools.

Work Requirements
^^^^^^^^^^^^^^^^^

Adding a Work Requirement in Multiple Operations
++++++++++++++++++++++++++++++++++++++++++++++++

- The **AddWorkRequirement** operator adds a new work requirement. It takes a YellowDog ``WorkRequirement`` object, defined in the YellowDog SDK.

- The **AddTaskGroupsToWorkRequirement** operator adds one or more task groups to a work requirement. It takes the namespace and name of a work requirement (or a work requirement ID), and a list of ``TaskGroup`` objects defined in the YellowDog SDK.

- The **AddTasksToTaskGroup** operator adds one or more to a task group. It takes the namespace and name of a work requirement, a task group name (or a task group ID), and a list of ``Task`` objects defined in the YellowDog SDK.

Adding a Work Requirement in a Single Operation
+++++++++++++++++++++++++++++++++++++++++++++++

- The **AddPopulatedWorkRequirement** operator adds a new work requirement, task groups and tasks in a single operation. It takes YellowDog ``WorkRequirement``, ``TaskGroup``, and ``Task`` objects, as defined in the YellowDog SDK.

Cancelling a Work Requirement
+++++++++++++++++++++++++++++

- The **CancelWorkRequirement** operator cancels a work requirement. It takes the namespace and name of the work requirement, or its YellowDog ID. All outstanding tasks will be cancelled, but currently executing tasks will be allowed to run to completion before the cancellation concludes unless ``abort_running_tasks`` is set to ``True``.

Worker Pools
^^^^^^^^^^^^

Provisioning a Worker Pool
++++++++++++++++++++++++++

- The **ProvisionWorkerPool** operator provisions a new worker pool. It takes a YellowDog ``ComputeRequirementTemplateUsage`` object to define the compute requirement associated with the worker pool, and an optional ``ProvisionedWorkerPool`` object that sets the properties for the worker pool. The classes for these objects are defined in the YellowDog SDK.

Shutting Down a Worker Pool
+++++++++++++++++++++++++++

- The **ShutdownWorkerPool** operator shuts down a worker pool. It takes the namespace and name of the worker pool, or its YellowDog ID. By default, when shutting down a worker pool, no additional tasks are allocated to workers but any currently executing tasks are allowed to conclude. If the ``terminate_immediately`` argument is set to ``True``, then the worker pool's compute requirement is immediately terminated and all currently executing tasks will fail.

The YellowDog Airflow Sensors
-----------------------------

- The **WorkRequirementStateSensor** is used to test when a work requirement has reached one of a defined set of states. It takes the namespace and name of the work requirement (or a work requirement ID), and a list of ``WorkRequirementStatus`` states, as defined in the YellowDog SDK.

- The **WorkerPoolStateSensor** is used to test when a worker pool has reached one of a defined set of states. It takes the namespace and name of the worker pool, or a worker pool ID, and a list of ``WorkerPoolStatus`` states, as defined in the YellowDog SDK.

- The **ComputeRequirementStateSensor** is used to test when a compute requirement has reached one of a defined set of states. It takes the namespace and name of the compute requirement, or a compute requirement ID, and a list of ``ComputeRequirementStatus`` states, as defined in the YellowDog SDK.

Exceptions
----------

The YellowDog Airflow provider components will pass most exceptions up for reporting in Airflow, or for handling in client code if the YellowDog components are subclassed.

Templating
----------

Most of the arguments passed to the YellowDog operators and sensors are templated, meaning that Airflow jinja templating can be used. Arguments of type ``str`` can be templated in situ; arguments of more complex types (e.g., a ``WorkRequirement`` object) must use the callable mechanism below to effect templating.

Any templated argument can be supplied with a callable function instead of a direct value. Supplying a ``Callable`` as the argument allows for complex objects arguments to be generated, or for more complicated processing to be invoked. If a ``Callable`` is supplied as the argument, the callable function must return an object of the required type, and it it must accept two named arguments as follows::

    def my_callable(context: Context, jinja_env: Environment)


Jinja templates can be rendered within the callable if required::

    ...
    task: BaseOperator = context["task"]
    my_rendered_data = task.render_template(my_templated_data, context, jinja_env)
    ...


As a concrete example, the example callable below generates a ``WorkRequirement`` object using templated parameters supplied for the DAG run::

    def gen_work_requirement(context: Context, jinja_env: Environment) -> WorkRequirement:
        task: BaseOperator = context["task"]
        return WorkRequirement(
            namespace=task.render_template("{{ params.namespace }}", context, jinja_env),
            name=task.render_template("{{ params.wr_name }}", context, jinja_env),
        )


Please see the `Airflow documentation <https://airflow.apache.org/docs/apache-airflow>`_ for more details on templating.
