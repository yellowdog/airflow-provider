Templating and callables
========================

Most operator and sensor arguments are Airflow *templated fields*: their final
value is worked out when the task runs, not when the DAG file is parsed. The
tables in :doc:`operators` and :doc:`sensors` show which arguments are
templated. A templated argument can be given in three ways:

.. list-table::
   :header-rows: 1
   :widths: 22 40 38

   * - You pass
     - What happens at run time
     - Use it for
   * - A Jinja string
     - Airflow renders it, for example ``"{{ params.namespace }}"``
     - IDs, names, namespaces, the connection ID
   * - A callable
     - Airflow calls it and uses the return value
     - SDK objects that depend on params, the ``run_id`` or XCom values
   * - A plain value or SDK object
     - Used as-is; strings are still rendered with Jinja
     - Fixed values

Jinja is **not** applied to strings *inside* SDK objects. For example,
``WorkRequirement(name="{{ params.name }}")`` creates a work requirement
literally named ``{{ params.name }}`` (which YellowDog will reject). To build
an object from run-time values, use a callable.

.. contents:: On this page
   :local:

Writing a callable
------------------

Airflow calls the callable with two **keyword** arguments, ``context`` and
``jinja_env``, so its parameters must have exactly those names::

   from airflow.sdk import Context
   from jinja2 import Environment
   from yellowdog_client.model import WorkRequirement


   def make_work_requirement(context: Context, jinja_env: Environment) -> WorkRequirement:
       return WorkRequirement(
           namespace=context["params"]["namespace"],
           name=context["params"]["wr_name"],
       )


   AddWorkRequirement(task_id="add-wr", work_requirement=make_work_requirement)

Pass the function itself (``make_work_requirement``), not the result of
calling it. A callable whose parameters are named differently, such as
``def make(ctx, env)``, fails when the task runs with a ``TypeError`` about an
unexpected keyword argument. A lambda works too:
``lambda context, jinja_env: ...``.

``context`` is the Airflow task context: ``context["params"]`` (the run's
params), ``context["run_id"]``, ``context["ti"]`` (the task instance),
``context["task"]`` (the operator) and so on. ``jinja_env`` is the Jinja
environment Airflow would use to render templates for the task.

Type-checking callables
-----------------------

If you type-check your DAGs (with pyright or mypy), annotate callables with
:class:`~yellowdog_provider.utils.templating.TemplateCallable`. It is a
protocol that requires the exact ``context`` and ``jinja_env`` parameter names,
so a misnamed parameter is reported by the type checker instead of failing at
run time::

   from yellowdog_provider.utils.templating import TemplateCallable

   make_wr: TemplateCallable[WorkRequirement] = make_work_requirement  # OK


   def bad(ctx: Context, env: Environment) -> WorkRequirement: ...


   make_bad: TemplateCallable[WorkRequirement] = bad  # type error

The operators' and sensors' templated parameters are annotated as
``Templated[T]`` (``T | TemplateCallable[T]``), so a misnamed callable passed
straight to an operator is also reported.

Rendering Jinja inside a callable
---------------------------------

To use Jinja inside a callable, render it with the task's ``render_template``
method, passing the ``context`` and ``jinja_env`` you were given::

   def make_work_requirement(context: Context, jinja_env: Environment) -> WorkRequirement:
       task = context["task"]
       return WorkRequirement(
           namespace=task.render_template("{{ params.namespace }}", context, jinja_env),
           name=task.render_template("wr-{{ params.suffix }}", context, jinja_env),
       )

Reading values from ``context`` directly, as in the earlier example, is
usually simpler.

Passing IDs between tasks with XCom
-----------------------------------

Operators return the ID of what they created or changed, and Airflow stores it
as the task's XCom. Pass it to a downstream ID argument with a Jinja
expression:

.. code-block:: python

   # A single ID (AddWorkRequirement, ProvisionWorkerPool, ...)
   work_requirement_id="{{ ti.xcom_pull(task_ids='add-wr') }}"

   # One ID from a list (AddTaskGroupsToWorkRequirement returns a list)
   task_group_id="{{ ti.xcom_pull(task_ids='add-groups')[0] }}"

or from a callable:

.. code-block:: python

   def first_task_group_id(context: Context, jinja_env: Environment) -> str:
       return context["ti"].xcom_pull(task_ids="add-groups")[0]

Sensors also push the ID of the resource they watched (see :doc:`sensors`), so
a sensor that looked a resource up by name can feed its ID downstream.

.. _none-ids:

When an ID renders to ``"None"``
--------------------------------

If the upstream task did not push a value (it failed, was skipped, or returned
nothing), ``xcom_pull`` returns ``None``, and Jinja renders that as the
**string** ``"None"``. Rather than send ``"None"`` to YellowDog as an ID, the
provider raises
:class:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogArgumentError`::

   Templated field 'work_requirement_id' rendered to 'None'; the upstream XCom
   value is probably missing (did the upstream task fail or return nothing?)

This most often happens in a task that runs even though an upstream task
failed, such as a clean-up task with ``trigger_rule=TriggerRule.ALL_DONE``.
Airflow *teardown* tasks (``.as_teardown(setups=...)``) are less affected,
because Airflow does not run a teardown whose setup task failed, so the
setup's XCom value is there whenever the teardown runs. The example DAG
``yd-provider-test.py`` (in the provider's GitHub repository) therefore passes the pool's ID from XCom to its
teardown. Prefer IDs to names: a name built from the run can match another
resource, and a work requirement's name can differ from the one supplied
(see :ref:`safe-retries`).

In ID and name fields, a value that renders to an empty or whitespace-only
string is treated as not supplied.

If your DAG uses ``render_template_as_native_obj=True``, a missing XCom
renders as the Python value ``None`` instead, which is treated as *not
supplied*: you then get the "either the ID or all of the names must be
supplied" error, or, if you also gave names, the lookup silently uses the
names.

.. _sdk-objects-xcom:

Why SDK objects cannot come from XCom
-------------------------------------

It would be natural to build a ``WorkRequirement`` in a TaskFlow ``@task``
and pass its result to an operator. This does not work on any supported
Airflow version (checked on 3.1 and 3.4): XCom cannot carry YellowDog SDK
objects, for three independent reasons.

- Airflow deserialises only classes on its allow-list (``[core]
  allowed_deserialization_classes``, by default ``airflow.*`` only), so SDK
  classes are refused unless every deployment adds ``yellowdog_client.*``.
- Even when allowed, most SDK model classes (``WorkRequirement``,
  ``TaskGroup``, ``Task`` and others) have fields that the platform sets
  (``id``, ``status`` and so on) and that their constructors do not accept, so
  Airflow cannot rebuild them.
- Enum values, such as ``NodeWorkerTargetType`` in
  ``ProvisionedWorkerPoolProperties``, cannot be serialised at all.

So build SDK objects where they are used: in a **callable** passed to the
operator (as on this page), which Airflow calls with the run's context when
the task runs. Upstream tasks can pass plain values (IDs, names, numbers,
lists, dicts) through XCom, and the callable can read them with
``context["ti"].xcom_pull(...)``. In your own ``@task`` functions, build the
objects inside the task (see :doc:`taskflow`).

Unrendered callables
--------------------

Airflow renders templated fields, including calling callables, just before
``execute()`` (or a sensor's ``poke()``). If ``execute()`` is called without
that step, for example directly in a unit test, a callable is still in place
and the provider raises
:class:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogException`
(``Templated field '...' has not been rendered``). In tests, call
``operator.render_template_fields(context)`` first, or pass plain values.

See also the Airflow documentation on `templates
<https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html>`_.
