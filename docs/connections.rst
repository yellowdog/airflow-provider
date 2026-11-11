Connections
===========

Every operator and sensor in the provider talks to YellowDog through an Airflow
connection of type ``yellowdog``, which holds the credentials of a YellowDog
Application. The
:class:`~yellowdog_provider.hooks.yellowdog_hooks.YellowDogHook` reads the
connection and creates the YellowDog SDK client.

.. contents:: On this page
   :local:

Connection fields
-----------------

.. list-table::
   :header-rows: 1
   :widths: 25 20 55

   * - UI label
     - Connection attribute
     - Meaning
   * - Connection Id
     - ``conn_id``
     - The name operators and sensors refer to. The default is
       ``yellowdog_default``.
   * - Connection Type
     - ``conn_type``
     - ``yellowdog`` (shown as *YellowDog* in the UI).
   * - Platform API URL
     - ``host``
     - Optional. The YellowDog Platform API URL. See below.
   * - Application Key ID
     - ``login``
     - Required. The key ID of a YellowDog Application.
   * - Application Key Secret
     - ``password``
     - Required. The key secret of the same Application.
   * - Portal URL
     - ``extra`` (``portal_url``)
     - Optional. The base URL of the YellowDog Portal, used for the *YellowDog
       work requirement* and *YellowDog worker pool* task links. See below.
   * - Description
     - ``description``
     - Optional free text, for example the purpose and scope of the
       Application.

The *schema* and *port* fields are not used and are hidden in the UI.

**Platform API URL.** The URL is normalised before use:

- empty (or not set): ``https://api.yellowdog.ai`` is used;
- no scheme (for example ``api.example.com``): ``https://`` is added;
- trailing ``/`` characters are removed.

When a URL other than the default is used, the hook logs ``Using Platform API
URL: ...`` in the task log.

**Portal URL.** Task links to the YellowDog Portal
(``https://portal.yellowdog.ai/#/work/<id>`` and ``.../#/workers/<id>``) use
the *Portal URL* if set (``https://`` is assumed if it has no scheme). Otherwise the Portal URL is the Platform API URL with
a leading ``api.`` host label replaced by ``portal.`` (for example
``https://api.yellowdog.ai`` gives ``https://portal.yellowdog.ai``). If the
Platform API host does not start with ``api.`` and no *Portal URL* is set, no
link is shown. In Airflow 3.1 the field appears in the UI only where the
Flask-AppBuilder libraries are installed; elsewhere set it in the connection's
JSON ``extra`` as ``{"portal_url": "https://..."}``.

**Portal links.** The *YellowDog work requirement* link appears on
``RunWorkRequirement``, ``AddWorkRequirement``, ``AddPopulatedWorkRequirement``
and ``WorkRequirementStateSensor`` tasks, and the *YellowDog worker pool* link
on ``ProvisionWorkerPool``, ``ShutdownProvisionedWorkerPool`` and
``WorkerPoolStateSensor`` tasks, in the task instance's details in the
Airflow UI. Each points at the resource whose ID the task pushed to XCom.
Airflow's worker computes a task's links when the task finishes or defers,
and stores them in XCom, so a link appears once the task has finished or
deferred: not while it is still polling on a worker (use ``deferrable=True``
to see the link during a long wait). No link is shown if the task pushed no
ID, the Portal URL cannot be worked out, or the connection cannot be read.

Creating a connection
---------------------

**Airflow UI.** *Admin → Connections → Add*, choose the *YellowDog* connection
type, and fill in the fields above. The connection type appears only once the
provider is installed in the environment that runs the Airflow UI.

**Airflow CLI:**

.. code-block:: console

   $ airflow connections add yellowdog_default \
       --conn-type yellowdog \
       --conn-host 'https://api.yellowdog.ai' \
       --conn-login '<KEY_ID>' \
       --conn-password '<KEY_SECRET>' \
       --conn-description 'My YellowDog Application'

``--conn-host`` and ``--conn-description`` are optional.

**Environment variable.** Airflow reads connections from variables named
``AIRFLOW_CONN_<CONN_ID>`` (the ID in upper case). The JSON form avoids having
to URL-encode the secret:

.. code-block:: bash

   $ export AIRFLOW_CONN_YELLOWDOG_DEFAULT='{
       "conn_type": "yellowdog",
       "login": "<KEY_ID>",
       "password": "<KEY_SECRET>"
     }'

The URI form also works; because of the host normalisation above, the host can
be given without a scheme:

.. code-block:: console

   $ export AIRFLOW_CONN_YELLOWDOG_DEFAULT='yellowdog://<KEY_ID>:<URL_ENCODED_SECRET>@api.yellowdog.ai'

**Secrets backend.** Connections stored in any Airflow `secrets backend
<https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html>`_
work in the same way.

Using a connection other than the default
-----------------------------------------

Every operator and sensor takes a keyword argument ``connection_id``, which
defaults to ``yellowdog_default``. It is a templated field, so it can come
from the DAG's params, for example to choose between accounts per run::

   AddWorkRequirement(
       task_id="submit",
       connection_id="{{ params.connection_id }}",
       work_requirement=make_work_requirement,
   )

``connection_id`` also accepts a callable; see :doc:`templating`.

Testing a connection
--------------------

The hook implements Airflow's connection test. It makes one lightweight,
authenticated request to YellowDog (it lists at most one namespace). The UI
shows ``Successfully connected to <URL>`` on success; the CLI prints
``Connection success!``. On failure, both show the error returned.

Airflow disables connection testing by default. Enable it with
``test_connection = Enabled`` in the ``[core]`` section of ``airflow.cfg``
(or ``AIRFLOW__CORE__TEST_CONNECTION=Enabled``), then use the **Test** button
on the connection in the UI, or:

.. code-block:: console

   $ airflow connections test yellowdog_default

A missing key ID or secret is reported as ``Connection '<id>' has no
Application Key ID`` (or ``Application Key Secret``).

Several Airflow environments sharing an account
------------------------------------------------

If two or more Airflow environments (for example staging and production)
use credentials for the same YellowDog account, set a different
``[yellowdog] deployment_id`` (environment variable
``AIRFLOW__YELLOWDOG__DEPLOYMENT_ID``) in each. Otherwise runs with the same
DAG and run IDs in the two environments cannot be told apart, and one
environment's teardown can cancel the other's work. See :ref:`deployment-id`.

Using the hook directly
-----------------------

To do anything the operators do not cover, use the hook in your own task to
get a YellowDog SDK
`PlatformClient <https://pypi.org/project/yellowdog-sdk>`_. The client is
created on the first call to
:meth:`~yellowdog_provider.hooks.yellowdog_hooks.YellowDogHook.get_conn` and
cached on the hook; use the hook as a context manager so that the client is
closed afterwards::

   from airflow.sdk import task
   from yellowdog_client.model import WorkRequirementStatus

   from yellowdog_provider.hooks.yellowdog_hooks import YellowDogHook


   @task
   def report_status(work_requirement_id: str) -> str:
       with YellowDogHook("yellowdog_default") as hook:
           client = hook.get_conn()
           wr = client.work_client.get_work_requirement_by_id(work_requirement_id)
           if wr.status != WorkRequirementStatus.COMPLETED:
               raise RuntimeError(f"Work requirement ended as {wr.status}")
           return wr.status.value

The hook also has typed methods for the operations the operators perform
(submit, wait, cancel, provision, shut down); see :doc:`taskflow`.

The hook takes ``connection_id`` (default ``yellowdog_default``) plus standard
hook keyword arguments. ``get_conn()`` raises
:class:`~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogException`
if the connection has no key ID or no secret; errors from YellowDog itself
come from the SDK.
