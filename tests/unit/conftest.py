from collections.abc import Iterator
from unittest.mock import MagicMock, patch

import pytest
from airflow.sdk import Connection
from yellowdog_client.platform_client import PlatformClient

from yellowdog_provider.hooks.yellowdog_hooks import YellowDogHook


@pytest.fixture
def mock_client() -> Iterator[MagicMock]:
    client = MagicMock(spec=PlatformClient)
    client.work_client = MagicMock(name="work_client")
    client.worker_pool_client = MagicMock(name="worker_pool_client")
    client.compute_client = MagicMock(name="compute_client")
    with patch.object(YellowDogHook, "get_conn", return_value=client):
        yield client


@pytest.fixture
def yd_connection() -> Iterator[Connection]:
    conn = Connection(
        conn_id="yd_test",
        conn_type="yellowdog",
        host="https://api.example.yellowdog",
        login="key-id",
        password="key-secret",
    )
    with patch.object(YellowDogHook, "get_connection", return_value=conn):
        yield conn
