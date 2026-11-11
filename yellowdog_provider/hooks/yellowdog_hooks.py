"""
Airflow Hook for the YellowDog API client.
"""

from types import TracebackType
from typing import Any

from airflow.sdk import BaseHook
from yellowdog_client.model import (
    ApiKey,
    NamespaceSearch,
    ServicesSchema,
    SliceReference,
)
from yellowdog_client.platform_client import PlatformClient

from yellowdog_provider.exceptions.yellowdog_exceptions import YellowDogException

PLATFORM_API_URL = "Platform API URL"
PLATFORM_API_URL_DEFAULT = "https://api.yellowdog.ai"
APP_KEY_ID_NAME = "Application Key ID"
APP_KEY_SECRET_NAME = "Application Key Secret"
CONNECTION_TYPE = "yellowdog"
CONN_NAME_ATTR = "connection_id"
DEFAULT_CONN_NAME = "yellowdog_default"
HOOK_NAME = "YellowDog"


class YellowDogHook(BaseHook):
    """
    Hook class that supports Application Key/Secret connections to the
    YellowDog Platform API.

    The connection's host is the Platform API URL: if empty, the default
    (``https://api.yellowdog.ai``) is used; if it has no scheme, ``https://``
    is added; trailing ``/`` characters are removed. The login and password
    are the Application Key ID and Secret. Use the hook as a context manager
    to close the client afterwards.

    :param connection_id: the Airflow connection ID; defaults to
        ``yellowdog_default``
    :type connection_id: str
    """

    conn_type = CONNECTION_TYPE
    conn_name_attr = CONN_NAME_ATTR
    default_conn_name = DEFAULT_CONN_NAME
    hook_name = HOOK_NAME

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """
        Return custom UI field behaviour for the YellowDog connection type.
        """
        return {
            "hidden_fields": ["schema", "port", "extra"],
            "relabeling": {
                "host": PLATFORM_API_URL,
                "login": APP_KEY_ID_NAME,
                "password": APP_KEY_SECRET_NAME,
            },
            "placeholders": {
                "host": PLATFORM_API_URL_DEFAULT,
            },
        }

    def __init__(self, connection_id: str = default_conn_name, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.connection_id = connection_id
        self.client: PlatformClient | None = None
        self._platform_url = PLATFORM_API_URL_DEFAULT

    @staticmethod
    def _normalise_host(host: str | None) -> str:
        host = (host or "").strip()
        if not host:
            return PLATFORM_API_URL_DEFAULT
        if "://" not in host:
            host = "https://" + host
        return host.rstrip("/")

    def get_conn(self) -> PlatformClient:
        """
        Fetch the YellowDog PlatformClient object corresponding to the
        connection ID. The client is created once and cached.

        :returns: a YellowDog PlatformClient object
        :raises ~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogException:
            if the connection lacks credentials
        """
        if self.client is None:
            connection = self.get_connection(self.connection_id)
            if not connection.login:
                raise YellowDogException(
                    f"Connection '{self.connection_id}' has no {APP_KEY_ID_NAME}"
                )
            if not connection.password:
                raise YellowDogException(
                    f"Connection '{self.connection_id}' has no {APP_KEY_SECRET_NAME}"
                )

            platform_url = self._normalise_host(connection.host)
            if platform_url != PLATFORM_API_URL_DEFAULT:
                self.log.info("Using Platform API URL: %s", platform_url)

            self._platform_url = platform_url
            self.client = PlatformClient.create(
                ServicesSchema(defaultUrl=platform_url),
                ApiKey(connection.login, connection.password),
            )

        return self.client

    def close(self) -> None:
        """Close and discard the cached client, if any."""
        if self.client is not None:
            client, self.client = self.client, None
            client.close()

    def __enter__(self) -> "YellowDogHook":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def test_connection(self) -> tuple[bool, str]:
        """
        Verify the connection with one lightweight authenticated read
        (the first namespace, if any).

        :returns: a (success, message) tuple, as used by Airflow's connection
            test
        """
        try:
            client = self.get_conn()
            client.namespaces_client.get_namespaces(NamespaceSearch()).slice(
                SliceReference(size=1)
            )
            return True, f"Successfully connected to {self._platform_url}"
        except Exception as exc:
            return False, str(exc)
        finally:
            self.close()
