YellowDog Provider for Apache Airflow
=====================================

The YellowDog Provider for Apache Airflow lets `Apache Airflow
<https://airflow.apache.org>`_ DAGs run work on the `YellowDog Platform
<https://yellowdog.ai>`_. Airflow decides *when* things happen and in what
order; YellowDog supplies the compute, at any scale, across clouds and on
premises.

The provider contains:

- a **Hook** (``YellowDogHook``) and a ``yellowdog`` **Connection type** that
  keep your YellowDog Application credentials in Airflow's connection store,
  with typed operations for use in your own ``@task`` functions;
- **Operators** that provision a worker pool, run work on it and shut it
  down (the three-task pattern in :doc:`workflow`), plus finer-grained
  operators that submit and cancel work requirements step by step;
- **Sensors** that wait for a work requirement, worker pool or compute
  requirement to reach one of a set of states;
- a **Trigger**, so that the operators and sensors can *defer* their waits
  to Airflow's triggerer instead of holding a worker slot;
- **Portal links** from Airflow tasks to the YellowDog Portal.

The operators that submit work requirements (``RunWorkRequirement``,
``AddWorkRequirement`` and ``AddPopulatedWorkRequirement``) are safe to
retry: a retry carries on with the earlier attempt's work instead of
duplicating it (for the other operators, see :ref:`safe-retries`). Work is
cancelled, with running tasks aborted, if a task gives up, is killed, or the
teardown of its setup/teardown scope finds it still running.

Operators and sensors take objects from the `YellowDog Python SDK
<https://pypi.org/project/yellowdog-sdk>`_ (``WorkRequirement``, ``TaskGroup``,
``Task``, ``ComputeRequirementTemplateUsage`` and so on) directly, so anything
the SDK can express can be submitted from a DAG.

Who this is for
---------------

These docs are written for Airflow users who are new to YellowDog, YellowDog
users who are new to Airflow, and people who are new to both. If either side
is unfamiliar, start with :doc:`concepts`, then work through the
:doc:`quickstart`.

Requirements
------------

- Apache Airflow **3.1** or later
- Python **3.12, 3.13 or 3.14**
- ``yellowdog-sdk`` **15.10.0** or later (installed automatically)
- A YellowDog account, and an Application (key ID and secret) with permission
  to use the namespaces your DAGs work in

Installation
------------

Install the provider into the same Python environment as Airflow (the
scheduler, the workers that run your tasks, and the API server, so that the
connection type appears in the UI)::

   pip install --upgrade yellowdog-airflow-provider

If you install Airflow with a `constraints file
<https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html>`_,
install the provider in the same ``pip`` command, or afterwards with the same
constraints, so that dependency versions stay consistent.

Check that Airflow has found the provider::

   airflow providers list | grep yellowdog

.. toctree::
   :maxdepth: 2
   :caption: Contents

   Overview <self>
   concepts
   quickstart
   workflow
   step-by-step
   taskflow
   connections
   operators
   sensors
   templating
   troubleshooting
   api
   changelog

Indices
-------

- :ref:`genindex`
- :ref:`modindex`
- :ref:`search`
