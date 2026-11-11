from unittest.mock import MagicMock, patch

import pytest
from airflow.sdk import Connection
from yellowdog_client.model import (
    ApiKey,
    NamespaceSearch,
    ServicesSchema,
    SliceReference,
)

from yellowdog_provider.exceptions.yellowdog_exceptions import YellowDogException
from yellowdog_provider.hooks.yellowdog_hooks import YellowDogHook

APP_KEY_SECRET_NAME_RE = "Application Key Secret"
CREATE = "yellowdog_provider.hooks.yellowdog_hooks.PlatformClient.create"


def _conn(**kw: object) -> Connection:
    base: dict = dict(
        conn_id="yd_test",
        conn_type="yellowdog",
        host="https://api.example.yellowdog",
        login="key-id",
        password="key-secret",
    )
    base.update(kw)
    return Connection(**base)


def test_get_conn_uses_connection_fields(yd_connection: Connection) -> None:
    with patch(CREATE) as create:
        client = YellowDogHook("yd_test").get_conn()
    create.assert_called_once_with(
        ServicesSchema(defaultUrl="https://api.example.yellowdog"),
        ApiKey("key-id", "key-secret"),
    )
    assert client is create.return_value


@pytest.mark.parametrize("host", [None, "", "  "])
def test_get_conn_default_url_when_host_missing(host: str | None) -> None:
    with (
        patch.object(YellowDogHook, "get_connection", return_value=_conn(host=host)),
        patch(CREATE) as create,
    ):
        YellowDogHook("yd_test").get_conn()
    assert create.call_args.args[0] == ServicesSchema(
        defaultUrl="https://api.yellowdog.ai"
    )


@pytest.mark.parametrize(
    "host,expected",
    [
        ("api.yellowdog.ai", "https://api.yellowdog.ai"),
        ("https://test.yellowdog.tech/api/", "https://test.yellowdog.tech/api"),
        ("http://localhost:8080", "http://localhost:8080"),
    ],
)
def test_get_conn_normalises_host(host: str, expected: str) -> None:
    with (
        patch.object(YellowDogHook, "get_connection", return_value=_conn(host=host)),
        patch(CREATE) as create,
    ):
        YellowDogHook("yd_test").get_conn()
    assert create.call_args.args[0] == ServicesSchema(defaultUrl=expected)


@pytest.mark.parametrize(
    "kw,missing",
    [
        ({"login": None}, "Application Key ID"),
        ({"password": None}, "Application Key Secret"),
    ],
)
def test_get_conn_missing_credentials_raises(kw: dict, missing: str) -> None:
    with (
        patch.object(YellowDogHook, "get_connection", return_value=_conn(**kw)),
        patch(CREATE) as create,
        pytest.raises(YellowDogException, match=missing) as ei,
    ):
        YellowDogHook("yd_test").get_conn()
    assert "yd_test" in str(ei.value)
    create.assert_not_called()


def test_get_conn_cached(yd_connection: Connection) -> None:
    with patch(CREATE) as create:
        hook = YellowDogHook("yd_test")
        assert hook.get_conn() is hook.get_conn()
    create.assert_called_once()


def test_close_and_context_manager(yd_connection: Connection) -> None:
    with patch(CREATE) as create, YellowDogHook("yd_test") as h:
        h.get_conn()
    create.return_value.close.assert_called_once()
    h.close()  # no client: no-op
    create.return_value.close.assert_called_once()


def test_get_conn_uses_given_connection_without_lookup() -> None:
    conn = _conn(host="api.other.yellowdog", login="k2", password="s2")
    with (
        patch.object(
            YellowDogHook, "get_connection", side_effect=AssertionError("lookup")
        ),
        patch(CREATE) as create,
        YellowDogHook("yd_test", connection=conn) as hook,
    ):
        assert hook.get_conn() is hook.get_conn()
    create.assert_called_once_with(
        ServicesSchema(defaultUrl="https://api.other.yellowdog"),
        ApiKey("k2", "s2"),
    )
    create.return_value.close.assert_called_once()


def test_given_connection_missing_credentials_raises() -> None:
    with (
        patch.object(
            YellowDogHook, "get_connection", side_effect=AssertionError("lookup")
        ),
        patch(CREATE) as create,
        pytest.raises(YellowDogException, match=APP_KEY_SECRET_NAME_RE),
    ):
        YellowDogHook("yd_test", connection=_conn(password=None)).get_conn()
    create.assert_not_called()


def test_portal_base_url_uses_given_connection() -> None:
    conn = _conn(host="https://api.other.yellowdog")
    with patch.object(
        YellowDogHook, "get_connection", side_effect=AssertionError("lookup")
    ):
        hook = YellowDogHook("yd_test", connection=conn)
        assert hook.portal_base_url() == "https://portal.other.yellowdog"


def test_portal_url_override_without_scheme_defaults_to_https() -> None:
    conn = _conn(extra='{"portal_url": " my.portal.example/ "}')
    hook = YellowDogHook("yd_test", connection=conn)
    assert hook.portal_base_url() == "https://my.portal.example"


def test_portal_url_override_keeps_given_scheme() -> None:
    conn = _conn(extra='{"portal_url": "http://my.portal.example"}')
    hook = YellowDogHook("yd_test", connection=conn)
    assert hook.portal_base_url() == "http://my.portal.example"


def test_connection_get_hook_constructs_hook() -> None:
    hook = Connection(conn_id="yd_test", conn_type="yellowdog").get_hook()
    assert isinstance(hook, YellowDogHook)
    assert hook.connection_id == "yd_test"


def test_test_connection_success(yd_connection: Connection) -> None:
    client = MagicMock()
    with patch(CREATE, return_value=client):
        ok, msg = YellowDogHook("yd_test").test_connection()
    assert ok is True
    assert "https://api.example.yellowdog" in msg
    assert "key-secret" not in msg
    client.namespaces_client.get_namespaces.assert_called_once_with(NamespaceSearch())
    client.namespaces_client.get_namespaces.return_value.slice.assert_called_once_with(
        SliceReference(size=1)
    )
    client.close.assert_called_once()


def test_test_connection_failure(yd_connection: Connection) -> None:
    client = MagicMock()
    client.namespaces_client.get_namespaces.return_value.slice.side_effect = (
        RuntimeError("401 unauthorised")
    )
    with patch(CREATE, return_value=client):
        ok, msg = YellowDogHook("yd_test").test_connection()
    assert ok is False
    assert msg == "401 unauthorised"
    client.close.assert_called_once()


def test_ui_field_behaviour() -> None:
    b = YellowDogHook.get_ui_field_behaviour()
    assert b["hidden_fields"] == ["schema", "port", "extra"]
    assert b["relabeling"] == {
        "host": "Platform API URL",
        "login": "Application Key ID",
        "password": "Application Key Secret",
    }
    assert b["placeholders"] == {"host": "https://api.yellowdog.ai"}
