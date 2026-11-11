"""
"View in YellowDog Portal" operator extra links.

Airflow 3 computes an operator link when the task instance finishes (or defers)
and stores it in XCom; the API server then serves the stored value without
touching the operator, the hook or the connection. :meth:`get_link` therefore
runs on the worker, where the connection and the XCom ``return_value`` (the
resource ID, pushed before the task waits) are available.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from airflow.sdk import BaseOperatorLink
from airflow.sdk.bases.xcom import BaseXCom

if TYPE_CHECKING:
    from airflow.sdk import BaseOperator

    from yellowdog_provider.hooks.yellowdog_hooks import YellowDogHook

XCOM_KEY = "return_value"
_API_LABEL = "api."


def portal_base_from_api_url(api_url: str) -> str | None:
    """
    Derive the Portal's base URL from a Platform API URL by replacing a leading
    ``api.`` host label with ``portal.``
    (``https://api.yellowdog.ai`` -> ``https://portal.yellowdog.ai``).

    :returns: the Portal base URL (no trailing slash), or ``None`` if the host
        has no leading ``api.`` label
    """
    parts = urlsplit(api_url.strip())
    host = parts.netloc
    if not parts.scheme or not host.lower().startswith(_API_LABEL):
        return None
    rest = host[len(_API_LABEL) :]
    if not rest:
        return None
    return f"{parts.scheme}://portal.{rest}"


class _PortalLink(BaseOperatorLink):
    """Base class: a Portal link to the resource whose ID is in XCom."""

    path_prefix: str = ""

    def get_link(self, operator: BaseOperator, *, ti_key: Any) -> str:
        """
        :returns: the Portal URL of the resource whose ID is the task's XCom
            ``return_value``, or ``""`` if there is no ID yet or the Portal URL
            cannot be determined
        """
        resource_id: Any = BaseXCom.get_value(ti_key=ti_key, key=XCOM_KEY)
        if not isinstance(resource_id, str) or resource_id in ("", "None"):
            return ""
        base = self._portal_base(operator)
        if base is None:
            return ""
        return f"{base}/#/{self.path_prefix}/{resource_id}"

    @staticmethod
    def _portal_base(operator: Any) -> str | None:
        try:
            hook: YellowDogHook = operator.get_hook()
            return hook.portal_base_url()
        except Exception:
            # A link must never fail the task: no link rather than a wrong one
            return None


class WorkRequirementPortalLink(_PortalLink):
    """Link to the work requirement in the YellowDog Portal."""

    @property
    def name(self) -> str:
        return "YellowDog work requirement"

    path_prefix = "work"


class WorkerPoolPortalLink(_PortalLink):
    """Link to the worker pool in the YellowDog Portal."""

    @property
    def name(self) -> str:
        return "YellowDog worker pool"

    path_prefix = "workers"
