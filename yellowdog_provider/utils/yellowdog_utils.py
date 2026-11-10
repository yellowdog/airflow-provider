"""
YellowDog utility functions.
"""

from collections.abc import Callable
from typing import Protocol

from yellowdog_client import PlatformClient
from yellowdog_client.model import (
    ComputeRequirement,
    WorkerPool,
    WorkRequirement,
)

from yellowdog_provider.exceptions.yellowdog_exceptions import YellowDogArgumentError


class SupportsLog(Protocol):
    """The subset of a logger used by the lookup helpers."""

    def info(self, msg: str, /, *args: object) -> None: ...

    def warning(self, msg: str, /, *args: object) -> None: ...


def require_id_or_name(
    kind: str,
    id_field: str,
    object_id: object | None,
    name_fields: dict[str, object | None],
) -> None:
    """
    Raise YellowDogArgumentError unless the ID is supplied, or every one of the
    name fields is supplied. Values may be unrendered templates or callables.
    """
    if object_id is not None:
        return
    if all(value is not None for value in name_fields.values()):
        return
    names = ", ".join(f"'{field}'" for field in name_fields)
    raise YellowDogArgumentError(
        f"For a {kind}, either '{id_field}' or all of {names} must be supplied"
    )


def _get_by_id_or_name[T: (WorkRequirement, WorkerPool, ComputeRequirement)](
    kind: str,
    log: SupportsLog,
    id_field: str,
    object_id: str | None,
    namespace: str | None,
    name_field: str,
    name: str | None,
    get_by_id: Callable[[str], T],
    get_by_name: Callable[[str, str], T],
) -> T:
    require_id_or_name(
        kind, id_field, object_id, {"namespace": namespace, name_field: name}
    )
    result: T
    if object_id is not None:
        if namespace is not None or name is not None:
            log.warning(
                "Both '%s' and namespace/name supplied for %s; ignoring the name",
                id_field,
                kind,
            )
        log.info("Retrieving %s ID '%s'", kind, object_id)
        result = get_by_id(object_id)
        log.info("Retrieved %s '%s/%s'", kind, result.namespace, result.name)
    else:
        assert namespace is not None and name is not None  # for type checkers
        log.info("Retrieving %s '%s/%s'", kind, namespace, name)
        result = get_by_name(namespace, name)  # Throws a 404 if not found
        log.info("Retrieved %s ID '%s'", kind, result.id)
    return result


def get_work_requirement_by_id_or_name(
    client: PlatformClient,
    log: SupportsLog,
    work_requirement_id: str | None,
    namespace: str | None,
    work_requirement_name: str | None,
) -> WorkRequirement:
    """Get a work requirement by ID, or by namespace and name if no ID."""
    return _get_by_id_or_name(
        "work requirement",
        log,
        "work_requirement_id",
        work_requirement_id,
        namespace,
        "work_requirement_name",
        work_requirement_name,
        client.work_client.get_work_requirement_by_id,
        client.work_client.get_work_requirement_by_name,
    )


def get_worker_pool_by_id_or_name(
    client: PlatformClient,
    log: SupportsLog,
    worker_pool_id: str | None,
    namespace: str | None,
    worker_pool_name: str | None,
) -> WorkerPool:
    """Get a worker pool by ID, or by namespace and name if no ID."""
    return _get_by_id_or_name(
        "worker pool",
        log,
        "worker_pool_id",
        worker_pool_id,
        namespace,
        "worker_pool_name",
        worker_pool_name,
        client.worker_pool_client.get_worker_pool_by_id,
        client.worker_pool_client.get_worker_pool_by_name,
    )


def get_compute_requirement_by_id_or_name(
    client: PlatformClient,
    log: SupportsLog,
    compute_requirement_id: str | None,
    namespace: str | None,
    compute_requirement_name: str | None,
) -> ComputeRequirement:
    """Get a compute requirement by ID, or by namespace and name if no ID."""
    return _get_by_id_or_name(
        "compute requirement",
        log,
        "compute_requirement_id",
        compute_requirement_id,
        namespace,
        "compute_requirement_name",
        compute_requirement_name,
        client.compute_client.get_compute_requirement_by_id,
        client.compute_client.get_compute_requirement_by_name,
    )
