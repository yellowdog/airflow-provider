"""
Airflow names that live in different modules on the Airflow versions the
provider supports, without a deprecation warning on any of them.

* Airflow 3.4: ``airflow.sdk`` (``conf``) and ``airflow.sdk.exceptions``
  (the exceptions; the ``airflow.exceptions`` names are deprecated there).
* Airflow 3.1: ``airflow.configuration`` (``conf``; not exported by
  ``airflow.sdk``) and ``airflow.exceptions`` (the exceptions; absent from
  ``airflow.sdk.exceptions``).

No single static import is valid on both, so type checkers see stand-ins; at
runtime these are the installed Airflow's own objects.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:

    class AirflowSensorTimeout(Exception):
        """Static stand-in; at runtime, Airflow's AirflowSensorTimeout."""

    class AirflowSkipException(Exception):
        """Static stand-in; at runtime, Airflow's AirflowSkipException."""

    class TaskDeferred(BaseException):
        """Static stand-in; at runtime, Airflow's TaskDeferred."""

        trigger: object
        method_name: str
        kwargs: dict[str, object] | None
        timeout: object

    class _Conf:
        """Static stand-in for the parts of Airflow's ``conf`` that are used."""

        def getboolean(self, section: str, key: str, *, fallback: bool) -> bool: ...

        def get(self, section: str, key: str, *, fallback: str) -> str: ...

    conf: _Conf

else:
    try:
        from airflow.sdk import conf
    except ImportError:  # Airflow 3.1
        from airflow.configuration import conf
    try:
        from airflow.sdk.exceptions import (
            AirflowSensorTimeout,
            AirflowSkipException,
            TaskDeferred,
        )
    except ImportError:  # Airflow 3.1
        from airflow.exceptions import (
            AirflowSensorTimeout,
            AirflowSkipException,
            TaskDeferred,
        )

__all__ = ["AirflowSensorTimeout", "AirflowSkipException", "TaskDeferred", "conf"]
