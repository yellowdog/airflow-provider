YellowDog Provider for Apache Airflow
=====================================

The YellowDog Provider for Apache Airflow lets `Apache Airflow
<https://airflow.apache.org>`_ DAGs run work on the `YellowDog Platform
<https://yellowdog.ai>`_. Airflow decides *when* things happen and in what
order; YellowDog supplies the compute, at any scale, across clouds and on
premises.

The provider contains:

- a **Hook** (``YellowDogHook``) and a ``yellowdog`` **Connection type** that
  keep your YellowDog Application credentials in Airflow's connection store;
- **Operators** that submit and cancel YellowDog work requirements, and
  provision and shut down YellowDog worker pools;
- **Sensors** that wait for a work requirement, worker pool or compute
  requirement to reach one of a set of states.

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
- ``yellowdog-sdk`` **15.2.0** or later (installed automatically)
- A YellowDog account, and an Application (key ID and secret) with permission
  to use the namespaces your DAGs work in

Python versions earlier than 3.12 are not supported: on those versions every
``yellowdog-sdk`` release requires ``urllib3<2``, which conflicts with
Airflow's constraint files.

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

   concepts
   quickstart
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
