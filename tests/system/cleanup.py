"""Find and remove YellowDog resources left behind by system tests."""

from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import Enum

from yellowdog_client.model import (
    ComputeRequirement,
    ComputeRequirementSearch,
    ComputeRequirementStatus,
    ProvisionedWorkerPool,
    WorkerPoolSearch,
    WorkerPoolSummary,
    WorkRequirementSearch,
    WorkRequirementSummary,
)
from yellowdog_client.platform_client import PlatformClient

from yellowdog_provider.utils.yellowdog_utils import SupportsLog

PROVISIONED_TYPE = ProvisionedWorkerPool.type

_COMPUTE_FINAL = {
    ComputeRequirementStatus.TERMINATING,
    ComputeRequirementStatus.TERMINATED,
}


@dataclass
class SweepReport:
    cancelled_work_requirements: list[str] = field(default_factory=list[str])
    shut_down_worker_pools: list[str] = field(default_factory=list[str])
    terminated_compute_requirements: list[str] = field(default_factory=list[str])


def name_matches(
    name: str | None, fragment: str, extra_fragments: Sequence[str]
) -> bool:
    """
    Whether ``name`` contains ``fragment`` or one of ``extra_fragments`` (empty
    extra fragments are ignored: they would match every name).
    """
    text = name or ""
    return fragment in text or any(f and f in text for f in extra_fragments)


def has_run_tag_prefix(tag: str | None, run_tag_prefixes: Sequence[str]) -> bool:
    """
    Whether ``tag`` marks a work requirement submitted by a task of one of the
    DAG runs whose ``RunStamp.run_tag_prefix()`` values are given (the tag is
    the prefix followed by ``:``).
    """
    return any((tag or "").startswith(f"{prefix}:") for prefix in run_tag_prefixes)


def find_work_requirements(
    client: PlatformClient,
    namespace: str,
    fragment: str,
    run_tag_prefixes: Sequence[str] = (),
    extra_fragments: Sequence[str] = (),
) -> list[WorkRequirementSummary]:
    """
    Live (non-finished) work requirements whose name contains ``fragment`` (or
    one of ``extra_fragments``) or whose tag carries one of
    ``run_tag_prefixes`` (work submitted by a DAG run whose derived names need
    not contain ``fragment``).
    """
    found = client.work_client.get_work_requirements(
        WorkRequirementSearch(namespaces=[namespace])
    ).list_all()
    return [
        w
        for w in found
        if (
            name_matches(w.name, fragment, extra_fragments)
            or has_run_tag_prefix(w.tag, run_tag_prefixes)
        )
        and w.status is not None
        and not w.status.finished
    ]


def find_worker_pools(
    client: PlatformClient,
    namespace: str,
    fragment: str,
    extra_fragments: Sequence[str] = (),
) -> list[WorkerPoolSummary]:
    """Live provisioned worker pools whose name contains ``fragment`` (or one
    of ``extra_fragments``)."""
    found = client.worker_pool_client.get_worker_pools(
        WorkerPoolSearch(namespaces=[namespace])
    ).list_all()
    return [
        p
        for p in found
        if name_matches(p.name, fragment, extra_fragments)
        and p.type == PROVISIONED_TYPE
        and p.status is not None
        and not p.status.finished
    ]


def find_compute_requirements(
    client: PlatformClient,
    namespace: str,
    fragment: str,
    extra_fragments: Sequence[str] = (),
) -> list[ComputeRequirement]:
    """Live compute requirements whose name contains ``fragment`` (or one of
    ``extra_fragments``)."""
    found = client.compute_client.get_compute_requirements(
        ComputeRequirementSearch(namespace=namespace)
    ).list_all()
    return [
        c
        for c in found
        if name_matches(c.name, fragment, extra_fragments)
        and c.status is not None
        and c.status not in _COMPUTE_FINAL
    ]


def _already_finishing(e: Exception) -> bool:
    """The platform rejected the request because the resource is already
    cancelling/shutting down/terminating (or finished)."""
    return "invalid transition" in str(e).lower()


def _status_name(status: Enum | None) -> str:
    return status.name if status is not None else "unknown"


def sweep(
    client: PlatformClient,
    namespace: str,
    fragment: str,
    log: SupportsLog,
    run_tag_prefixes: Sequence[str] = (),
    extra_fragments: Sequence[str] = (),
) -> SweepReport:
    """Cancel, shut down and terminate live resources whose name contains
    ``fragment`` or one of ``extra_fragments`` (work requirements also by
    ``run_tag_prefixes``; see :func:`find_work_requirements`).

    Per-item errors are logged and skipped so one failure doesn't stop the sweep.
    An "invalid transition" rejection means the resource is already finishing:
    it is logged at info level and not counted as an error.
    """
    report = SweepReport()

    for w in find_work_requirements(
        client, namespace, fragment, run_tag_prefixes, extra_fragments
    ):
        wid = str(w.id)
        try:
            client.work_client.cancel_work_requirement_by_id(wid, abort=True)
            report.cancelled_work_requirements.append(wid)
        except Exception as e:
            if _already_finishing(e):
                log.info(
                    f"Work requirement {wid} already finishing "
                    f"(status {_status_name(w.status)}): {e}"
                )
            else:
                log.warning(f"Failed to cancel work requirement {wid}: {e}")

    for p in find_worker_pools(client, namespace, fragment, extra_fragments):
        pid = str(p.id)
        try:
            client.worker_pool_client.shutdown_worker_pool_by_id(pid)
            report.shut_down_worker_pools.append(pid)
        except Exception as e:
            if _already_finishing(e):
                log.info(
                    f"Worker pool {pid} already finishing "
                    f"(status {_status_name(p.status)}): {e}"
                )
            else:
                log.warning(f"Failed to shut down worker pool {pid}: {e}")

    for c in find_compute_requirements(client, namespace, fragment, extra_fragments):
        cid = str(c.id)
        try:
            client.compute_client.terminate_compute_requirement_by_id(cid)
            report.terminated_compute_requirements.append(cid)
        except Exception as e:
            if _already_finishing(e):
                log.info(
                    f"Compute requirement {cid} already finishing "
                    f"(status {_status_name(c.status)}): {e}"
                )
            else:
                log.warning(f"Failed to terminate compute requirement {cid}: {e}")

    return report
