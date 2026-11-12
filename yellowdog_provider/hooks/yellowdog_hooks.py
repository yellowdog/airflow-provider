"""
Airflow Hook for the YellowDog API client.
"""

import copy
import hashlib
import importlib
import re
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from datetime import datetime
from types import TracebackType
from typing import Any, Literal, cast

from airflow.sdk import BaseHook, Connection
from requests.exceptions import HTTPError
from yellowdog_client.model import (
    ApiKey,
    ComputeRequirementStatus,
    ComputeRequirementTemplateUsage,
    NamespaceSearch,
    ProvisionedWorkerPool,
    ProvisionedWorkerPoolProperties,
    ServicesSchema,
    SliceReference,
    Task,
    TaskGroup,
    TaskSearch,
    WorkerPoolStatus,
    WorkRequirement,
    WorkRequirementSearch,
    WorkRequirementStatus,
)
from yellowdog_client.model.exceptions import BaseCustomException
from yellowdog_client.platform_client import PlatformClient

from yellowdog_provider.exceptions.yellowdog_exceptions import (
    YellowDogArgumentError,
    YellowDogException,
)
from yellowdog_provider.links.yellowdog_links import portal_base_from_api_url
from yellowdog_provider.utils.run_stamp import (
    INSTANCE_TAG_PREFIX_PATTERN,
    RUN_TAG_PREFIX_PATTERN,
    TASK_TAG_PREFIX_PATTERN,
    RunStamp,
    derive_name,
    try_name,
)

PLATFORM_API_URL = "Platform API URL"
PLATFORM_API_URL_DEFAULT = "https://api.yellowdog.ai"
APP_KEY_ID_NAME = "Application Key ID"
APP_KEY_SECRET_NAME = "Application Key Secret"
CONNECTION_TYPE = "yellowdog"
PORTAL_URL_NAME = "Portal URL"
PORTAL_URL_FIELD = "portal_url"
CONN_NAME_ATTR = "connection_id"
DEFAULT_CONN_NAME = "yellowdog_default"
HOOK_NAME = "YellowDog"

_UNFINISHED_WORK_REQUIREMENT_STATUSES = [
    status for status in WorkRequirementStatus if not status.finished
]
_MAX_NAME_ATTEMPTS = 16
_MAX_PROBED_TRIES = 64

_Lookups = dict[str, WorkRequirement | None]
_SUFFIX_LENGTH = 4

SubmitAction = Literal["submitted", "resumed", "already_completed"]


@dataclass(frozen=True)
class SubmitResult:
    """
    The outcome of :meth:`YellowDogHook.submit_work_requirement`.

    :ivar work_requirement_id: the ID of the work requirement to follow (new,
        or this task's earlier attempt)
    :ivar name: that work requirement's name
    :ivar action: ``"submitted"`` (a new work requirement was added),
        ``"resumed"`` (an earlier, unfinished attempt should be waited on) or
        ``"already_completed"`` (an earlier attempt completed and was not
        resubmitted)
    """

    work_requirement_id: str
    name: str
    action: SubmitAction


def attempt_tag(stamp: RunStamp, cycle: int) -> str:
    """
    The tag of a work requirement submitted by the task ``stamp`` identifies,
    in clear cycle ``cycle`` (see :meth:`YellowDogHook.submit_work_requirement`):
    ``<stamp.tag()>:c<cycle>``, i.e.
    ``airflow:<run digest>:<task digest>:m<map index>:c<cycle>``.
    """
    return f"{stamp.tag()}:c{cycle}"


def _owned_cycle(tag: str | None, stamp_tag: str) -> tuple[bool, int | None]:
    """
    Whether ``tag`` marks an attempt of the task whose stamp tag is
    ``stamp_tag`` (exactly the stamp tag, or it followed by ``:c<digits>``;
    the stamp tag ends in its own ``:m<map index>``, so ``...:m1`` never
    owns ``...:m12``),
    and the attempt's clear cycle (``None`` if not recorded).
    """
    if tag is None:
        return False, None
    match = re.fullmatch(re.escape(stamp_tag) + r"(?::c([0-9]+))?", tag)
    if match is None:
        return False, None
    cycle = match.group(1)
    return True, None if cycle is None else int(cycle)


@dataclass(frozen=True)
class _Attempt:
    """An earlier work requirement carrying this task's run stamp."""

    id: str
    name: str
    status: WorkRequirementStatus | None
    created: datetime | None
    cycle: int | None

    def sort_key(self) -> tuple[bool, float, str]:
        created = self.created.timestamp() if self.created is not None else 0.0
        return (self.created is not None, created, self.id)


def is_invalid_transition(error: BaseException) -> bool:
    """
    Whether ``error`` is the platform's rejection of a state change as an
    "invalid transition" (e.g. cancelling a finished work requirement, or
    shutting down a pool that is already shut down). The SDK raises either
    ``HTTPError`` or a ``BaseCustomException`` subclass for it.
    """
    return isinstance(error, HTTPError | BaseCustomException) and (
        "invalid transition" in str(error).lower()
    )


def _is_not_found(error: HTTPError) -> bool:
    response = error.response
    return response is not None and response.status_code == 404


def _require(value: str | None, what: str) -> str:
    if value is None:
        raise YellowDogException(f"The platform returned no {what}")
    return value


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
    :param connection: an already fetched Airflow connection to use instead
        of looking ``connection_id`` up (e.g. one fetched with
        ``await BaseHook.aget_connection(...)`` in a trigger's event loop, so
        that a worker thread never makes the lookup itself); defaults to
        ``None`` (look it up when needed)
    :type connection: airflow.sdk.Connection | None
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

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """
        Return the optional "Portal URL" connection field for Airflow versions
        that read connection fields from the hook (Airflow 3.2+ reads the
        equivalent ``conn-fields`` of the provider info instead). Empty when
        the Flask-AppBuilder form libraries are not installed.
        """
        try:
            widgets: Any = importlib.import_module("flask_appbuilder.fieldwidgets")
            babel: Any = importlib.import_module("flask_babel")
            wtforms: Any = importlib.import_module("wtforms")
        except ImportError:
            return {}
        return {
            f"extra__{CONNECTION_TYPE}__{PORTAL_URL_FIELD}": wtforms.StringField(
                babel.lazy_gettext(PORTAL_URL_NAME),
                widget=widgets.BS3TextFieldWidget(),
            )
        }

    def __init__(
        self,
        connection_id: str = default_conn_name,
        *,
        connection: Connection | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.connection_id = connection_id
        self._connection: Connection | None = connection
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

    def _get_connection(self) -> Connection:
        """The connection given to the hook, else looked up by ID."""
        if self._connection is not None:
            return self._connection
        return self.get_connection(self.connection_id)

    def portal_base_url(self) -> str | None:
        """
        The base URL of the YellowDog Portal for this connection: the
        connection's optional "Portal URL" field if set, else derived from the
        Platform API URL by replacing a leading ``api.`` host label with
        ``portal.``.

        :returns: the Portal base URL without a trailing slash, or ``None`` if
            it cannot be determined (no override and no ``api.`` host label)
        """
        connection = self._get_connection()
        extra = cast("dict[str, Any]", connection.extra_dejson)  # pyright: ignore[reportUnknownMemberType]
        for key in (PORTAL_URL_FIELD, f"extra__{CONNECTION_TYPE}__{PORTAL_URL_FIELD}"):
            override = extra.get(key)
            if isinstance(override, str) and override.strip():
                override = override.strip()
                if "://" not in override:
                    override = "https://" + override
                return override.rstrip("/")
        return portal_base_from_api_url(self._normalise_host(connection.host))

    def get_conn(self) -> PlatformClient:
        """
        Fetch the YellowDog PlatformClient object corresponding to the
        connection ID. The client is created once and cached.

        :returns: a YellowDog PlatformClient object
        :raises ~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogException:
            if the connection lacks credentials
        """
        if self.client is None:
            connection = self._get_connection()
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

    # -- state -------------------------------------------------------------

    def get_work_requirement_state(
        self, work_requirement_id: str
    ) -> WorkRequirementStatus:
        """
        Return the current status of a work requirement.

        :param work_requirement_id: the work requirement's YellowDog ID
        :raises ~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogException:
            if the platform returns no status
        """
        wr = self.get_conn().work_client.get_work_requirement_by_id(work_requirement_id)
        if wr.status is None:
            raise YellowDogException(
                f"Work requirement '{work_requirement_id}' has no status"
            )
        return wr.status

    def get_worker_pool_state(self, worker_pool_id: str) -> WorkerPoolStatus:
        """
        Return the current status of a worker pool.

        :param worker_pool_id: the worker pool's YellowDog ID
        :raises ~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogException:
            if the platform returns no status
        """
        pool = self.get_conn().worker_pool_client.get_worker_pool_by_id(worker_pool_id)
        if pool.status is None:
            raise YellowDogException(f"Worker pool '{worker_pool_id}' has no status")
        return pool.status

    def get_compute_requirement_state(
        self, compute_requirement_id: str
    ) -> ComputeRequirementStatus:
        """
        Return the current status of a compute requirement.

        :param compute_requirement_id: the compute requirement's YellowDog ID
        :raises ~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogException:
            if the platform returns no status
        """
        cr = self.get_conn().compute_client.get_compute_requirement_by_id(
            compute_requirement_id
        )
        if cr.status is None:
            raise YellowDogException(
                f"Compute requirement '{compute_requirement_id}' has no status"
            )
        return cr.status

    # -- work requirements ---------------------------------------------------

    def cancel_work_requirement(self, work_requirement_id: str, *, abort: bool) -> None:
        """
        Cancel a work requirement. If the platform rejects the cancellation as
        an invalid state transition (e.g. the work requirement has already
        finished), a warning is logged and the call succeeds; any other error
        is raised.

        :param work_requirement_id: the work requirement's YellowDog ID
        :param abort: abort running tasks rather than letting them finish
        """
        self.log.info(
            "Cancelling work requirement ID '%s' (abort running tasks: %s)",
            work_requirement_id,
            abort,
        )
        try:
            self.get_conn().work_client.cancel_work_requirement_by_id(
                work_requirement_id, abort
            )
        except (HTTPError, BaseCustomException) as e:
            # Tolerate idempotent cancellations (YEL-13327)
            if not is_invalid_transition(e):
                raise
            self.log.warning(
                "Cancellation invalid for work requirement ID '%s': %s",
                work_requirement_id,
                e,
            )

    def submit_work_requirement(
        self,
        work_requirement: WorkRequirement,
        task_groups_and_tasks: Sequence[tuple[TaskGroup, Sequence[Task]]],
        *,
        stamp: RunStamp,
        try_number: int,
        cycle: int,
    ) -> SubmitResult:
        """
        Submit a populated work requirement, safely across retries and clears.

        The work requirement is tagged :func:`attempt_tag` ``(stamp, cycle)``,
        i.e. ``<stamp.tag()>:c<cycle>`` (replacing any tag it had), and named
        ``work_requirement.name`` or, if that is empty,
        :func:`~yellowdog_provider.utils.run_stamp.derive_name`. ``cycle``
        identifies the task instance's *clear cycle*: it must change whenever
        the task instance is cleared and stay the same across its automatic
        retries (:class:`.RunWorkRequirement` uses Airflow's ``max_tries``).

        This task's earlier attempts are the work requirements in the
        namespace whose tag is ``stamp.tag()`` (no cycle recorded) or
        ``stamp.tag()`` followed by ``:c<digits>`` (any cycle). They are found
        by a search for the current cycle's tag and by looking up the names
        earlier tries would have used, in any cycle (the name, and its
        ``-try<k>`` names for recent ``k`` up to ``try_number``, with any
        suffixed variants). The most recently created one decides:

        * none: submit under the name;
        * COMPLETED in the current cycle: return it as ``"already_completed"``.
          Task counts are not checked, so a partly-built work requirement
          that the platform later marked COMPLETED is treated as completed;
        * COMPLETED in an earlier cycle, or with no cycle recorded (the task
          instance was cleared since, so the work is to be done again): submit
          a fresh copy;
        * unfinished (other than CANCELLING) and fully built, in any cycle:
          return it as ``"resumed"`` (so nothing is orphaned, e.g. by a clear
          while deferred);
        * unfinished (other than CANCELLING) but partly built (its task
          groups are not the supplied ones, by name and in order, or a group
          has fewer tasks than supplied): cancel it with abort, then submit a
          fresh copy;
        * FAILED, CANCELLING or CANCELLED: submit a fresh copy.

        A work requirement with the name but another (or no) tag is never
        adopted or touched. A fresh copy is named
        :func:`~yellowdog_provider.utils.run_stamp.try_name` ``(name,
        try_number)``; if that name is taken, a deterministic 4-hex suffix
        derived from the stamp (not the cycle) is added before ``-try<N>``.

        Submission adds the work requirement, sets the task groups in the
        supplied order, then adds each group's tasks. If any step after the
        work requirement is added fails, the partly built work requirement is
        cancelled, with abort, and the error is re-raised. The supplied
        objects are not modified.

        :param work_requirement: the work requirement, without task groups
        :param task_groups_and_tasks: (task group, tasks) pairs, in order
        :param stamp: the run stamp of the submitting task
        :param try_number: Airflow's try number, used in fresh copies' names
        :param cycle: the task instance's clear cycle (a non-negative integer)
        :returns: the work requirement to follow and what was done
        :raises .YellowDogArgumentError: if ``work_requirement`` already
            contains task groups, or ``cycle`` is negative
        """
        if work_requirement.taskGroups:
            raise YellowDogArgumentError(
                "The work requirement to submit must not already contain task "
                "groups; supply them via 'task_groups_and_tasks'"
            )
        if cycle < 0:
            raise YellowDogArgumentError(f"'cycle' must not be negative; got {cycle}")
        namespace = work_requirement.namespace
        name = work_requirement.name or derive_name(stamp)
        stamp_tag = stamp.tag()
        tag = attempt_tag(stamp, cycle)

        lookups: _Lookups = {}
        attempts = self._find_attempts(
            namespace, name, stamp_tag, tag, try_number, lookups
        )
        if not attempts:
            if self._lookup(namespace, name, lookups) is None:
                self.log.info("No earlier attempt found; submitting '%s'", name)
                return self._add_populated(
                    work_requirement, task_groups_and_tasks, name, tag
                )
            self.log.info(
                "Work requirement '%s/%s' exists but was not submitted by this "
                "task; submitting a fresh copy",
                namespace,
                name,
            )
        else:
            latest = max(attempts, key=_Attempt.sort_key)
            status = latest.status
            self.log.info(
                "Found this task's earlier attempt '%s' (ID '%s', status %s, "
                "cycle %s; current cycle %s)",
                latest.name,
                latest.id,
                None if status is None else status.value,
                latest.cycle,
                cycle,
            )
            if status == WorkRequirementStatus.COMPLETED:
                if latest.cycle == cycle:
                    # Completed is completed: task counts are not checked
                    self.log.info(
                        "Work requirement ID '%s' already completed in this "
                        "cycle; not resubmitting",
                        latest.id,
                    )
                    return SubmitResult(latest.id, latest.name, "already_completed")
                self.log.info(
                    "Work requirement ID '%s' completed before the task instance "
                    "was cleared; running the work again",
                    latest.id,
                )
            # CANCELLING counts as cancelled: it is on its way to CANCELLED
            # (e.g. cancelled by the previous try's own timeout or kill path)
            elif (
                status is not None
                and not status.finished
                and status != WorkRequirementStatus.CANCELLING
            ):
                if self._is_fully_built(latest.id, task_groups_and_tasks):
                    self.log.info("Resuming work requirement ID '%s'", latest.id)
                    return SubmitResult(latest.id, latest.name, "resumed")
                self.log.warning(
                    "Work requirement ID '%s' is only partly built", latest.id
                )
                self.cancel_work_requirement(latest.id, abort=True)

        fresh = self._fresh_name(namespace, name, try_number, stamp_tag, lookups)
        self.log.info("Submitting a fresh copy as '%s'", fresh)
        return self._add_populated(work_requirement, task_groups_and_tasks, fresh, tag)

    def add_work_requirement(
        self,
        work_requirement: WorkRequirement,
        *,
        stamp: RunStamp,
        try_number: int,
        cycle: int,
    ) -> SubmitResult:
        """
        Add a work requirement (as given, so normally without task groups),
        safely across retries and clears. It is tagged
        :func:`attempt_tag` ``(stamp, cycle)`` and named as in
        :meth:`submit_work_requirement`.

        This task's earlier attempts are found as there. If the most recently
        created is COMPLETED in the current cycle it is returned as
        ``"already_completed"``; if it is unfinished (other than CANCELLING),
        in any cycle, it is returned as ``"resumed"``. Otherwise (none, or
        failed, cancelled, or completed in an earlier cycle) the work
        requirement is added under its name or, if that is held by another
        work requirement, a fresh name as in :meth:`submit_work_requirement`.
        Unlike that method, nothing is checked about the earlier attempt's
        task groups, and nothing is cancelled.

        :raises .YellowDogArgumentError: if ``cycle`` is negative
        """
        if cycle < 0:
            raise YellowDogArgumentError(f"'cycle' must not be negative; got {cycle}")
        namespace = work_requirement.namespace
        name = work_requirement.name or derive_name(stamp)
        stamp_tag = stamp.tag()
        tag = attempt_tag(stamp, cycle)
        lookups: _Lookups = {}
        attempts = self._find_attempts(
            namespace, name, stamp_tag, tag, try_number, lookups
        )
        if attempts:
            latest = max(attempts, key=_Attempt.sort_key)
            status = latest.status
            if status == WorkRequirementStatus.COMPLETED:
                if latest.cycle == cycle:
                    return SubmitResult(latest.id, latest.name, "already_completed")
            elif (
                status is not None
                and not status.finished
                and status != WorkRequirementStatus.CANCELLING
            ):
                self.log.info("Reusing work requirement ID '%s'", latest.id)
                return SubmitResult(latest.id, latest.name, "resumed")
            use = self._fresh_name(namespace, name, try_number, stamp_tag, lookups)
        elif self._lookup(namespace, name, lookups) is None:
            use = name
        else:
            use = self._fresh_name(namespace, name, try_number, stamp_tag, lookups)

        wr = copy.deepcopy(work_requirement)
        if wr.tag and wr.tag != tag:
            self.log.warning("Replacing tag '%s' with the run stamp '%s'", wr.tag, tag)
        wr.name = use
        wr.tag = tag
        self.log.info("Adding work requirement '%s/%s'", namespace, use)
        added = self.get_conn().work_client.add_work_requirement(wr)
        wr_id = _require(added.id, "work requirement ID")
        self.log.info("Added work requirement ID '%s'", wr_id)
        return SubmitResult(wr_id, use, "submitted")

    def find_unfinished_run_work_requirements(
        self,
        namespace: str | None,
        run_tag_prefix: str,
        task_tag_prefixes: Collection[str] | None = None,
    ) -> list[str]:
        """
        Return the IDs of the unfinished work requirements whose tag starts
        with ``run_tag_prefix`` (see
        :meth:`~yellowdog_provider.utils.run_stamp.RunStamp.run_tag_prefix`),
        i.e. those submitted by any task of one DAG run, in a namespace or in
        every namespace visible to the credentials. Names are never matched.

        With ``task_tag_prefixes``, only the work requirements whose tag is
        one of the prefixes or starts with ``<prefix>:`` for one of them are
        returned. Each prefix is either a task's
        ``airflow:<run digest>:<task digest>`` (see
        :meth:`~yellowdog_provider.utils.run_stamp.RunStamp.task_tag_prefix`),
        for the work requirements of that task at every map index, or a task
        instance's ``airflow:<run digest>:<task digest>:m<map index>`` (see
        :meth:`~yellowdog_provider.utils.run_stamp.RunStamp.tag`), for those
        of that map index only (``m1`` does not match ``m12``).

        :param namespace: the namespace to search, or ``None`` for all
        :param run_tag_prefix: the DAG run's tag prefix
        :param task_tag_prefixes: restrict to these tasks of the run, or
            ``None`` for the whole run; an empty collection returns ``[]``
            without searching
        :raises .YellowDogArgumentError: if ``run_tag_prefix`` is not in the
            ``run_tag_prefix()`` format (so it can never match other runs), or
            a task prefix is not in one of those two formats or is not in the
            run
        """
        if not RUN_TAG_PREFIX_PATTERN.fullmatch(run_tag_prefix):
            raise YellowDogArgumentError(
                f"'{run_tag_prefix}' is not a DAG run tag prefix "
                "(see RunStamp.run_tag_prefix())"
            )
        starts = (run_tag_prefix + ":",)
        exact: frozenset[str] | None = None
        if task_tag_prefixes is not None:
            for task_prefix in task_tag_prefixes:
                valid = TASK_TAG_PREFIX_PATTERN.fullmatch(
                    task_prefix
                ) or INSTANCE_TAG_PREFIX_PATTERN.fullmatch(task_prefix)
                if not valid or not task_prefix.startswith(run_tag_prefix + ":"):
                    raise YellowDogArgumentError(
                        f"'{task_prefix}' is not a task tag prefix of the DAG "
                        f"run '{run_tag_prefix}' (see RunStamp.task_tag_prefix() "
                        "and RunStamp.tag())"
                    )
            if not task_tag_prefixes:
                return []
            starts = tuple(p + ":" for p in task_tag_prefixes)
            exact = frozenset(task_tag_prefixes)
        search = WorkRequirementSearch(
            namespaces=None if namespace is None else [namespace],
            statuses=list(_UNFINISHED_WORK_REQUIREMENT_STATUSES),
        )
        summaries = self.get_conn().work_client.get_work_requirements(search)
        return [
            summary.id
            for summary in summaries.list_all()
            if summary.id is not None
            and (
                (summary.tag or "").startswith(starts)
                or (exact is not None and summary.tag in exact)
            )
            and summary.status is not None
            and not summary.status.finished
        ]

    def _find_attempts(
        self,
        namespace: str,
        name: str,
        stamp_tag: str,
        tag: str,
        try_number: int,
        lookups: _Lookups,
    ) -> list[_Attempt]:
        """
        This task's earlier work requirements (tags owned per
        :func:`_owned_cycle`), from a search for the current cycle's ``tag``
        and from the names earlier tries (of any cycle) would have used.
        """
        work = self.get_conn().work_client
        attempts: dict[str, _Attempt] = {}
        # The tag filter narrows the search; ownership is checked here
        search = WorkRequirementSearch(namespaces=[namespace], tag=tag)
        for summary in work.get_work_requirements(search).list_all():
            owned, cycle = _owned_cycle(summary.tag, stamp_tag)
            if owned and summary.id is not None:
                attempts[summary.id] = _Attempt(
                    summary.id,
                    summary.name or "",
                    summary.status,
                    summary.createdTime,
                    cycle,
                )
        # Name probes, for earlier cycles' attempts and in case the tag search
        # misses: the name itself, then for each recent try k the names
        # _fresh_name would have walked through (stopping at the first free
        # one, which try k would have used)
        found = [self._lookup(namespace, name, lookups)]
        first_try = max(1, try_number - _MAX_PROBED_TRIES + 1)
        for k in range(first_try, try_number + 1):
            for candidate in self._name_candidates(name, k, stamp_tag):
                wr = self._lookup(namespace, candidate, lookups)
                if wr is None:
                    break
                found.append(wr)
        for wr in found:
            if wr is None or wr.id is None:
                continue
            owned, cycle = _owned_cycle(wr.tag, stamp_tag)
            if owned:
                attempts[wr.id] = _Attempt(
                    wr.id, wr.name, wr.status, wr.createdTime, cycle
                )
        return list(attempts.values())

    def _lookup(
        self, namespace: str, name: str, lookups: _Lookups
    ) -> WorkRequirement | None:
        """The named work requirement, or ``None`` if there is none (404)."""
        if name not in lookups:
            lookups[name] = self._get_by_name(namespace, name)
        return lookups[name]

    def _get_by_name(self, namespace: str, name: str) -> WorkRequirement | None:
        try:
            return self.get_conn().work_client.get_work_requirement_by_name(
                namespace, name
            )
        except HTTPError as e:
            if _is_not_found(e):
                return None
            raise

    @staticmethod
    def _name_candidates(name: str, try_number: int, tag: str) -> list[str]:
        """Fresh-copy names for a try, in order of preference."""
        candidates = [try_name(name, try_number)]
        for attempt in range(1, _MAX_NAME_ATTEMPTS + 1):
            suffix = hashlib.sha256(f"{tag}\x1f{attempt}".encode()).hexdigest()
            candidates.append(try_name(f"{name}-{suffix[:_SUFFIX_LENGTH]}", try_number))
        return candidates

    def _fresh_name(
        self,
        namespace: str,
        name: str,
        try_number: int,
        tag: str,
        lookups: _Lookups,
    ) -> str:
        """``try_name(name, try_number)``, or a stamp-suffixed variant if taken."""
        for candidate in self._name_candidates(name, try_number, tag):
            if self._lookup(namespace, candidate, lookups) is None:
                return candidate
            self.log.info("Name '%s/%s' is taken", namespace, candidate)
        raise YellowDogException(
            f"Could not find a free name for a fresh copy of '{namespace}/{name}'"
        )

    def _is_fully_built(
        self,
        work_requirement_id: str,
        task_groups_and_tasks: Sequence[tuple[TaskGroup, Sequence[Task]]],
    ) -> bool:
        """
        True if the work requirement's task groups are the supplied ones (by
        name, in order) and each has at least the supplied number of tasks
        (from the group's task summary or, if absent, a task search).
        """
        work = self.get_conn().work_client
        wr = work.get_work_requirement_by_id(work_requirement_id)
        groups = wr.taskGroups or []
        supplied = [group.name for group, _ in task_groups_and_tasks]
        if [group.name for group in groups] != supplied:
            self.log.info(
                "Task groups %s do not match those supplied %s",
                [group.name for group in groups],
                supplied,
            )
            return False
        for group, (_, tasks) in zip(groups, task_groups_and_tasks, strict=True):
            count = self._task_count(work_requirement_id, group, len(tasks))
            if count < len(tasks):
                self.log.info(
                    "Task group '%s' has %s of %s tasks", group.name, count, len(tasks)
                )
                return False
        return True

    def _task_count(
        self, work_requirement_id: str, group: TaskGroup, needed: int
    ) -> int:
        """The group's task count, counting (up to ``needed``) if no summary."""
        if group.taskSummary is not None:
            return group.taskSummary.taskCount
        if needed == 0:
            return 0
        search = TaskSearch(workRequirementId=work_requirement_id, taskGroupId=group.id)
        count = 0
        for _ in self.get_conn().work_client.get_tasks(search).iterate():
            count += 1
            if count >= needed:
                break
        return count

    def _add_populated(
        self,
        work_requirement: WorkRequirement,
        task_groups_and_tasks: Sequence[tuple[TaskGroup, Sequence[Task]]],
        name: str,
        tag: str,
    ) -> SubmitResult:
        """Add the work requirement, set its task groups, then add the tasks."""
        work = self.get_conn().work_client
        wr = copy.deepcopy(work_requirement)
        if wr.tag and wr.tag != tag:
            self.log.warning("Replacing tag '%s' with the run stamp '%s'", wr.tag, tag)
        wr.name = name
        wr.tag = tag
        wr.taskGroups = None

        self.log.info("Adding work requirement '%s/%s'", wr.namespace, name)
        added = work.add_work_requirement(wr)
        wr_id = _require(added.id, "work requirement ID")
        self.log.info("Added work requirement ID '%s'", wr_id)

        try:
            task_groups = [copy.deepcopy(group) for group, _ in task_groups_and_tasks]
            self.log.info(
                "Adding %s task group(s) to work requirement: %s",
                len(task_groups),
                [group.name for group in task_groups],
            )
            # The platform preserves submission order, so no reversal is needed;
            # verified by tests/system/test_components.py
            # ::test_add_populated_task_group_order
            added.taskGroups = task_groups
            added = work.update_work_requirement(added)

            for group, tasks in task_groups_and_tasks:
                self.log.info(
                    "Adding %s task(s) to task group '%s'", len(tasks), group.name
                )
                work.add_tasks_to_task_group_by_name(
                    added.namespace,
                    added.name,
                    group.name,
                    [copy.deepcopy(task) for task in tasks],
                )
        except BaseException:
            # Don't leave a half-built work requirement running: its tasks in
            # earlier groups would otherwise execute with no one tracking it
            self.log.error(
                "Submitting work requirement ID '%s' failed; cancelling it", wr_id
            )
            try:
                self.cancel_work_requirement(wr_id, abort=True)
            except Exception as cancel_error:
                self.log.warning(
                    "Could not cancel the partly submitted work requirement "
                    "ID '%s': %s",
                    wr_id,
                    cancel_error,
                )
            raise

        return SubmitResult(wr_id, name, "submitted")

    # -- worker pools --------------------------------------------------------

    def provision_worker_pool(
        self,
        usage: ComputeRequirementTemplateUsage,
        properties: ProvisionedWorkerPoolProperties | None,
    ) -> str:
        """
        Provision a worker pool.

        :param usage: the compute requirement template usage
        :param properties: the provisioned worker pool properties, if any
        :returns: the new worker pool's YellowDog ID
        """
        self.log.info(
            "Provisioning worker pool '%s/%s'",
            usage.requirementNamespace,
            usage.requirementName,
        )
        pool = self.get_conn().worker_pool_client.provision_worker_pool(
            usage, properties
        )
        pool_id = _require(pool.id, "worker pool ID")
        self.log.info("Provisioned worker pool ID '%s'", pool_id)
        return pool_id

    def terminate_compute_requirement(self, compute_requirement_id: str) -> None:
        """
        Immediately terminate a compute requirement. If the platform rejects
        the termination as an invalid state transition (e.g. it is already
        terminating or terminated), this is logged and the call succeeds;
        any other error is raised.

        :param compute_requirement_id: the compute requirement's YellowDog ID
        """
        try:
            self.get_conn().compute_client.terminate_compute_requirement_by_id(
                compute_requirement_id
            )
        except Exception as exc:
            if not is_invalid_transition(exc):
                raise
            self.log.info(
                "Compute requirement ID '%s' is already terminated or terminating (%s)",
                compute_requirement_id,
                exc,
            )

    def shutdown_worker_pool(
        self, worker_pool_id: str, *, terminate_immediately: bool
    ) -> str | None:
        """
        Shut down a provisioned worker pool and, optionally, immediately
        terminate its compute requirement (failing any running tasks). A
        pool that is already shut down, or a compute requirement that is
        already terminating or terminated (the platform rejects the step as
        an "invalid transition"), is logged and tolerated. The two steps are
        independent: if the shutdown fails otherwise, the compute requirement
        is still terminated, and then the shutdown's error is raised.

        :param worker_pool_id: the worker pool's YellowDog ID
        :param terminate_immediately: also terminate the compute requirement
        :returns: the terminated compute requirement's ID, or ``None`` if not
            terminating
        :raises ~yellowdog_provider.exceptions.yellowdog_exceptions.YellowDogException:
            if the pool is not a provisioned worker pool, or has no compute
            requirement to terminate
        """
        client = self.get_conn()
        pool = client.worker_pool_client.get_worker_pool_by_id(worker_pool_id)
        if not isinstance(pool, ProvisionedWorkerPool):
            raise YellowDogException(
                f"Worker pool '{pool.namespace}/{pool.name}' ({worker_pool_id}) "
                f"is a {type(pool).__name__}; only provisioned worker pools can "
                "be shut down"
            )
        self.log.info("Shutting down provisioned worker pool ID '%s'", worker_pool_id)
        shutdown_error: Exception | None = None
        try:
            client.worker_pool_client.shutdown_worker_pool_by_id(worker_pool_id)
        except Exception as exc:
            if is_invalid_transition(exc):
                self.log.info(
                    "Worker pool ID '%s' is already shut down (%s)", worker_pool_id, exc
                )
            elif not terminate_immediately:
                raise
            else:
                shutdown_error = exc
                self.log.warning(
                    "Failed to shut down worker pool ID '%s' (%s); still "
                    "terminating its compute requirement",
                    worker_pool_id,
                    exc,
                )
        if not terminate_immediately:
            return None
        cr_id = _require(pool.computeRequirementId, "compute requirement ID")
        self.log.info("Immediately terminating compute requirement ID '%s'", cr_id)
        self.terminate_compute_requirement(cr_id)
        if shutdown_error is not None:
            raise shutdown_error
        return cr_id
