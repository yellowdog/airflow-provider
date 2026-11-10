from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

import pytest

from yellowdog_provider.exceptions.yellowdog_exceptions import YellowDogArgumentError
from yellowdog_provider.utils.yellowdog_utils import (
    get_compute_requirement_by_id_or_name,
    get_work_requirement_by_id_or_name,
    get_worker_pool_by_id_or_name,
    require_id_or_name,
)


@dataclass
class Kind:
    label: str
    getter: Callable[..., Any]
    client_attr: str
    by_id: str
    by_name: str
    id_field: str
    name_field: str


KINDS = [
    Kind(
        "work requirement",
        get_work_requirement_by_id_or_name,
        "work_client",
        "get_work_requirement_by_id",
        "get_work_requirement_by_name",
        "work_requirement_id",
        "work_requirement_name",
    ),
    Kind(
        "worker pool",
        get_worker_pool_by_id_or_name,
        "worker_pool_client",
        "get_worker_pool_by_id",
        "get_worker_pool_by_name",
        "worker_pool_id",
        "worker_pool_name",
    ),
    Kind(
        "compute requirement",
        get_compute_requirement_by_id_or_name,
        "compute_client",
        "get_compute_requirement_by_id",
        "get_compute_requirement_by_name",
        "compute_requirement_id",
        "compute_requirement_name",
    ),
]

kinds = pytest.mark.parametrize("kind", KINDS, ids=lambda k: k.label)


@kinds
def test_get_by_id(kind: Kind) -> None:
    client, log = MagicMock(), MagicMock()
    sub = getattr(client, kind.client_attr)
    result = kind.getter(client, log, "id-1", None, None)
    getattr(sub, kind.by_id).assert_called_once_with("id-1")
    getattr(sub, kind.by_name).assert_not_called()
    assert result is getattr(sub, kind.by_id).return_value
    log.warning.assert_not_called()


@kinds
def test_get_by_name(kind: Kind) -> None:
    client, log = MagicMock(), MagicMock()
    sub = getattr(client, kind.client_attr)
    result = kind.getter(client, log, None, "ns", "nm")
    getattr(sub, kind.by_name).assert_called_once_with("ns", "nm")
    getattr(sub, kind.by_id).assert_not_called()
    assert result is getattr(sub, kind.by_name).return_value


@kinds
def test_id_takes_precedence_and_warns(kind: Kind) -> None:
    client, log = MagicMock(), MagicMock()
    sub = getattr(client, kind.client_attr)
    kind.getter(client, log, "id-1", "ns", "nm")
    getattr(sub, kind.by_id).assert_called_once_with("id-1")
    getattr(sub, kind.by_name).assert_not_called()
    log.warning.assert_called_once()


@kinds
def test_missing_all_raises(kind: Kind) -> None:
    with pytest.raises(YellowDogArgumentError) as exc:
        kind.getter(MagicMock(), MagicMock(), None, None, None)
    for field in (kind.id_field, "namespace", kind.name_field):
        assert field in str(exc.value)


@kinds
def test_namespace_without_name_raises(kind: Kind) -> None:
    client = MagicMock()
    with pytest.raises(YellowDogArgumentError):
        kind.getter(client, MagicMock(), None, "ns", None)
    getattr(client, kind.client_attr).assert_not_called()


def test_require_id_or_name_accepts_callables() -> None:
    def gen(*, context: object, jinja_env: object) -> str:
        return "x"

    require_id_or_name("work requirement", "work_requirement_id", gen, {})
    require_id_or_name(
        "work requirement",
        "work_requirement_id",
        None,
        {"namespace": "{{ ns }}", "work_requirement_name": gen},
    )
    with pytest.raises(YellowDogArgumentError):
        require_id_or_name(
            "work requirement",
            "work_requirement_id",
            None,
            {"namespace": gen, "work_requirement_name": None},
        )
