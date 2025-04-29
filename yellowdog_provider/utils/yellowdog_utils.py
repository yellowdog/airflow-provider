"""
YellowDog utility functions.
"""

from logging import Logger

from yellowdog_client import PlatformClient
from yellowdog_client.model import (
    ComputeRequirement,
    WorkerPool,
    WorkRequirement,
)

from yellowdog_provider.exceptions.yellowdog_exceptions import YellowDogException


def get_work_requirement_by_id_or_name(
    client: PlatformClient,
    log: Logger,
    work_requirement_id: str | None,
    namespace: str | None,
    work_requirement_name: str | None,
) -> WorkRequirement:
    """
    Get a work requirement object by its name or ID. If work_requirement_id is None,
    use the namespace and name.
    """

    if work_requirement_id is not None:
        log.info(f"Retrieving work requirement ID '{work_requirement_id}'")
        work_requirement: WorkRequirement = (
            client.work_client.get_work_requirement_by_id(work_requirement_id)
        )
        log.info(
            "Retrieved work requirement "
            f"'{work_requirement.namespace}/{work_requirement.name}'"
        )

    elif namespace is not None and work_requirement_name is not None:
        log.info(f"Retrieving work requirement '{namespace}/{work_requirement_name}'")
        work_requirement: WorkRequirement = (
            client.work_client.get_work_requirement_by_name(
                namespace,
                work_requirement_name,
            )
        )  # Throws a 404 if not found
        log.info("Retrieved work requirement ID" f"'{work_requirement.id}'")

    else:
        raise YellowDogException(
            f"Either 'work_requirement_id ({work_requirement_id})', "
            f"or both 'namespace' ({namespace}) and "
            f"'work_requirement_name ({work_requirement_name})' must be supplied"
        )

    return work_requirement


def get_worker_pool_by_id_or_name(
    client: PlatformClient,
    log: Logger,
    worker_pool_id: str | None,
    namespace: str | None,
    worker_pool_name: str | None,
) -> WorkerPool:
    """
    Get a worker pool object by its name or ID. If worker_pool_id is None,
    use the namespace and name.
    """

    if worker_pool_id is not None:
        log.info(f"Retrieving worker pool ID '{worker_pool_id}'")
        worker_pool: WorkerPool = client.worker_pool_client.get_worker_pool_by_id(
            worker_pool_id
        )
        log.info(
            "Retrieved worker pool " f"'{worker_pool.namespace}/{worker_pool.name}'"
        )

    elif namespace is not None and worker_pool_name is not None:
        log.info(f"Retrieving worker pool '{namespace}/{worker_pool_name}'")
        worker_pool: WorkerPool = client.worker_pool_client.get_worker_pool_by_name(
            namespace,
            worker_pool_name,
        )
        log.info("Retrieved worker pool ID" f"'{worker_pool.id}'")

    else:
        raise YellowDogException(
            f"Either 'worker_pool_id ({worker_pool_id})', "
            f"or both 'namespace' ({namespace}) and "
            f"'worker_pool_name ({worker_pool_name})' must be supplied"
        )

    return worker_pool


def get_compute_requirement_by_id_or_name(
    client: PlatformClient,
    log: Logger,
    compute_requirement_id: str | None,
    namespace: str | None,
    compute_requirement_name: str | None,
) -> ComputeRequirement:
    """
    Get a compute requirement object by its name or ID. If compute_requirement_id is None,
    use the namespace and name.
    """

    if compute_requirement_id is not None:
        log.info(f"Retrieving compute requirement ID '{compute_requirement_id}'")
        compute_requirement: ComputeRequirement = (
            client.compute_client.get_compute_requirement_by_id(compute_requirement_id)
        )
        log.info(
            "Retrieved compute requirement "
            f"'{compute_requirement.namespace}/{compute_requirement.name}'"
        )

    elif namespace is not None and compute_requirement_name is not None:
        log.info(
            f"Retrieving compute requirement '{namespace}/{compute_requirement_name}'"
        )
        compute_requirement: ComputeRequirement = (
            client.compute_client.get_compute_requirement_by_name(
                namespace,
                compute_requirement_name,
            )
        )
        log.info("Retrieved compute requirement ID" f"'{compute_requirement.id}'")

    else:
        raise YellowDogException(
            f"Either 'compute_requirement_id ({compute_requirement_id})', "
            f"or both 'namespace' ({namespace}) and "
            f"'compute_requirement_name ({compute_requirement_name})' must be supplied"
        )

    return compute_requirement
