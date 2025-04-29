"""
Airflow Hook for the YellowDog API client.
"""

PLATFORM_API_URL = "Platform API URL"
PLATFORM_API_URL_DEFAULT = "https://api.yellowdog.ai"
APP_KEY_ID_NAME = "Application Key ID"
APP_KEY_SECRET_NAME = "Application Key Secret"
CONNECTION_TYPE = "yellowdog"
CONN_NAME_ATTR = "yellowdog_conn_id"
HOOK_NAME = "YellowDog"

from typing import Any

from airflow.hooks.base import BaseHook
from yellowdog_client.platform_client import ApiKey, PlatformClient, ServicesSchema


class YellowDogHook(BaseHook):
    """
    Hook class that supports Application Key/Secret connections to the
    YellowDog Platform API.
    """

    conn_type = CONNECTION_TYPE
    conn_name_attr = CONN_NAME_ATTR
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

    def __init__(self, connection_id: str):
        super().__init__()
        self.connection_id = connection_id
        self.client: PlatformClient | None = None

    def get_conn(self) -> PlatformClient:
        """
        Fetch the YellowDog PlatformClient object corresponding to the
        connection ID.

        :returns: a YellowDog PlatformClient object
        :rtype: PlatformClient
        """
        if self.client is None:
            connection = self.get_connection(self.connection_id)

            platform_url = (
                PLATFORM_API_URL_DEFAULT if connection.host == "" else connection.host
            )

            if platform_url != PLATFORM_API_URL_DEFAULT:
                self.log.info(f"Using Platform API URL: {platform_url}")

            self.client = PlatformClient.create(
                ServicesSchema(defaultUrl=platform_url),
                ApiKey(connection.login, connection.password),
            )

        return self.client
