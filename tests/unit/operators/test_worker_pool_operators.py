from typing import Any
from unittest.mock import MagicMock

import pytest
from yellowdog_client.model import (
    ComputeRequirementTemplateUsage,
    ConfiguredWorkerPool,
    ProvisionedWorkerPool,
    WorkerPool,
)

from tests.support import make_context, render
from yellowdog_provider.exceptions.yellowdog_exceptions import (
    YellowDogArgumentError,
    YellowDogException,
)
from yellowdog_provider.operators.yellowdog_operators import (
    ProvisionWorkerPool,
    ShutdownProvisionedWorkerPool,
)


def _with(obj: Any, **attrs: Any) -> Any:
    for key, value in attrs.items():
        setattr(obj, key, value)
    return obj


def _crtu() -> ComputeRequirementTemplateUsage:
    return ComputeRequirementTemplateUsage(
        templateId="tmpl-1", requirementNamespace="ns", requirementName="req"
    )


def _pool(cls: type = ProvisionedWorkerPool, **kw: Any) -> Any:
    return _with(cls(), id="wp-1", namespace="ns", name="pool", **kw)


def _run(op: Any) -> Any:
    ctx = make_context()
    render(op, ctx)
    return op.execute(ctx)


def test_provision_without_properties(mock_client: MagicMock) -> None:
    mock_client.worker_pool_client.provision_worker_pool.return_value = _pool()
    crtu = _crtu()
    op = ProvisionWorkerPool(task_id="t", compute_requirement_template_usage=crtu)
    _run(op)
    mock_client.worker_pool_client.provision_worker_pool.assert_called_once_with(
        crtu, None
    )


def test_provision_returns_id(mock_client: MagicMock) -> None:
    mock_client.worker_pool_client.provision_worker_pool.return_value = _pool()
    op = ProvisionWorkerPool(task_id="t", compute_requirement_template_usage=_crtu())
    assert _run(op) == "wp-1"


def test_provision_positional_args_rejected() -> None:
    with pytest.raises(TypeError):
        ProvisionWorkerPool("t", _crtu())  # type: ignore[misc]


def test_shutdown_by_id(mock_client: MagicMock) -> None:
    mock_client.worker_pool_client.get_worker_pool_by_id.return_value = _pool()
    op = ShutdownProvisionedWorkerPool(task_id="t", worker_pool_id="wp-1")
    assert _run(op) == "wp-1"
    mock_client.worker_pool_client.shutdown_worker_pool_by_id.assert_called_once_with(
        "wp-1"
    )
    mock_client.compute_client.terminate_compute_requirement_by_id.assert_not_called()


def test_shutdown_by_name(mock_client: MagicMock) -> None:
    mock_client.worker_pool_client.get_worker_pool_by_name.return_value = _pool()
    op = ShutdownProvisionedWorkerPool(
        task_id="t", namespace="ns", worker_pool_name="pool"
    )
    assert _run(op) == "wp-1"
    mock_client.worker_pool_client.get_worker_pool_by_name.assert_called_once_with(
        "ns", "pool"
    )


def test_shutdown_requires_id_or_name() -> None:
    with pytest.raises(YellowDogArgumentError):
        ShutdownProvisionedWorkerPool(task_id="t", namespace="ns")


def test_shutdown_terminate_immediately(mock_client: MagicMock) -> None:
    mock_client.worker_pool_client.get_worker_pool_by_id.return_value = _pool(
        computeRequirementId="cr-1"
    )
    op = ShutdownProvisionedWorkerPool(
        task_id="t", worker_pool_id="wp-1", terminate_immediately=True
    )
    _run(op)
    mock_client.compute_client.terminate_compute_requirement_by_id.assert_called_once_with(
        "cr-1"
    )


def test_shutdown_configured_pool_raises(mock_client: MagicMock) -> None:
    mock_client.worker_pool_client.get_worker_pool_by_id.return_value = _pool(
        ConfiguredWorkerPool
    )
    op = ShutdownProvisionedWorkerPool(task_id="t", worker_pool_id="wp-1")
    with pytest.raises(YellowDogException, match="configured worker pool"):
        _run(op)
    mock_client.worker_pool_client.shutdown_worker_pool_by_id.assert_not_called()


def test_shutdown_unknown_pool_type_raises(mock_client: MagicMock) -> None:
    mock_client.worker_pool_client.get_worker_pool_by_id.return_value = _pool(
        WorkerPool
    )
    op = ShutdownProvisionedWorkerPool(task_id="t", worker_pool_id="wp-1")
    with pytest.raises(YellowDogException):
        _run(op)
    mock_client.worker_pool_client.shutdown_worker_pool_by_id.assert_not_called()


def test_id_rendering_to_None_raises(mock_client: MagicMock) -> None:
    op = ShutdownProvisionedWorkerPool(
        task_id="t", worker_pool_id="{{ ti.xcom_pull(task_ids='missing') }}"
    )
    with pytest.raises(YellowDogArgumentError, match="upstream XCom"):
        _run(op)
    mock_client.worker_pool_client.get_worker_pool_by_id.assert_not_called()
