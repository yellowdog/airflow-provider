import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from airflow.sdk import Connection

from yellowdog_provider.hooks.yellowdog_hooks import YellowDogHook
from yellowdog_provider.links.yellowdog_links import (
    WorkerPoolPortalLink,
    WorkRequirementPortalLink,
    portal_base_from_api_url,
)
from yellowdog_provider.operators.yellowdog_operators import (
    AddPopulatedWorkRequirement,
    AddWorkRequirement,
    ProvisionWorkerPool,
    RunWorkRequirement,
    ShutdownProvisionedWorkerPool,
)
from yellowdog_provider.sensors.yellowdog_sensors import (
    ComputeRequirementStateSensor,
    WorkerPoolStateSensor,
    WorkRequirementStateSensor,
)

WR_ID = "ydid:workreq:000000:abcdef01-0000-0000-0000-000000000000"
POOL_ID = "ydid:wrkrpool:000000:abcdef01-0000-0000-0000-000000000000"


def test_portal_base_from_api_url() -> None:
    assert (
        portal_base_from_api_url("https://api.yellowdog.ai")
        == "https://portal.yellowdog.ai"
    )
    assert (
        portal_base_from_api_url("https://api.test.yellowdog.tech/")
        == "https://portal.test.yellowdog.tech"
    )


@pytest.mark.parametrize(
    "url", ["https://yd.example.com", "http://localhost:8080", "", "api.yellowdog.ai"]
)
def test_portal_base_none_without_api_label(url: str) -> None:
    assert portal_base_from_api_url(url) is None


def _operator(extra: dict[str, Any] | None = None, host: str | None = None) -> Any:
    conn = Connection(
        conn_id="c",
        conn_type="yellowdog",
        host=host,
        login="k",
        password="s",
        extra=None if extra is None else json.dumps(extra),
    )
    operator = MagicMock()
    operator.get_hook.return_value = YellowDogHook("c")
    operator._conn = conn
    return operator


def _link(link: Any, operator: Any, value: Any) -> str:
    with (
        patch.object(YellowDogHook, "get_connection", return_value=operator._conn),
        patch(
            "yellowdog_provider.links.yellowdog_links.BaseXCom.get_value",
            return_value=value,
        ),
    ):
        return link.get_link(operator, ti_key=MagicMock())


def test_portal_override_wins() -> None:
    operator = _operator(
        {"portal_url": "https://my.portal.example/"}, host="https://api.yellowdog.ai"
    )
    assert (
        _link(WorkRequirementPortalLink(), operator, WR_ID)
        == f"https://my.portal.example/#/work/{WR_ID}"
    )


def test_work_requirement_link_url() -> None:
    operator = _operator(host="https://api.yellowdog.ai")
    assert (
        _link(WorkRequirementPortalLink(), operator, WR_ID)
        == f"https://portal.yellowdog.ai/#/work/{WR_ID}"
    )


def test_worker_pool_link_url() -> None:
    operator = _operator(host="https://api.test.yellowdog.tech")
    assert (
        _link(WorkerPoolPortalLink(), operator, POOL_ID)
        == f"https://portal.test.yellowdog.tech/#/workers/{POOL_ID}"
    )


def test_default_connection_host_gives_default_portal() -> None:
    operator = _operator(host=None)
    assert _link(WorkerPoolPortalLink(), operator, POOL_ID).startswith(
        "https://portal.yellowdog.ai/#/workers/"
    )


def test_prefixed_extra_key_is_read() -> None:
    operator = _operator({"extra__yellowdog__portal_url": "https://p.example"})
    assert _link(WorkerPoolPortalLink(), operator, POOL_ID).startswith(
        "https://p.example/#/workers/"
    )


@pytest.mark.parametrize("value", [None, "", "None", 5])
def test_link_empty_without_xcom_id(value: Any) -> None:
    operator = _operator(host="https://api.yellowdog.ai")
    assert _link(WorkRequirementPortalLink(), operator, value) == ""


def test_link_empty_without_known_portal() -> None:
    operator = _operator(host="https://yd.example.com")
    assert _link(WorkRequirementPortalLink(), operator, WR_ID) == ""


def test_link_empty_when_connection_unreadable() -> None:
    operator = _operator(host="https://api.yellowdog.ai")
    with (
        patch.object(
            YellowDogHook, "get_connection", side_effect=RuntimeError("no conn")
        ),
        patch(
            "yellowdog_provider.links.yellowdog_links.BaseXCom.get_value",
            return_value=WR_ID,
        ),
    ):
        assert WorkRequirementPortalLink().get_link(operator, ti_key=MagicMock()) == ""


def test_link_reads_return_value_xcom() -> None:
    operator = _operator(host="https://api.yellowdog.ai")
    ti_key = MagicMock()
    with (
        patch.object(YellowDogHook, "get_connection", return_value=operator._conn),
        patch(
            "yellowdog_provider.links.yellowdog_links.BaseXCom.get_value",
            return_value=WR_ID,
        ) as get_value,
    ):
        WorkRequirementPortalLink().get_link(operator, ti_key=ti_key)
    get_value.assert_called_once_with(ti_key=ti_key, key="return_value")


def test_link_names_and_xcom_keys() -> None:
    assert WorkRequirementPortalLink().name == "YellowDog work requirement"
    assert WorkerPoolPortalLink().name == "YellowDog worker pool"
    assert WorkRequirementPortalLink().xcom_key != WorkerPoolPortalLink().xcom_key


@pytest.mark.parametrize(
    ("cls", "link"),
    [
        (RunWorkRequirement, WorkRequirementPortalLink),
        (AddWorkRequirement, WorkRequirementPortalLink),
        (AddPopulatedWorkRequirement, WorkRequirementPortalLink),
        (WorkRequirementStateSensor, WorkRequirementPortalLink),
        (ProvisionWorkerPool, WorkerPoolPortalLink),
        (ShutdownProvisionedWorkerPool, WorkerPoolPortalLink),
        (WorkerPoolStateSensor, WorkerPoolPortalLink),
    ],
)
def test_links_attached(cls: type, link: type) -> None:
    assert [type(x) for x in cls.operator_extra_links] == [link]


def test_compute_sensor_has_no_link() -> None:
    assert not ComputeRequirementStateSensor.operator_extra_links
