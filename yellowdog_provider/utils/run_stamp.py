"""Run stamp (DAG run/task identity) and YellowDog name derivation."""

import hashlib
import re
from dataclasses import dataclass

from airflow.sdk import Context

from yellowdog_provider.exceptions.yellowdog_exceptions import YellowDogException
from yellowdog_provider.utils.airflow_compat import conf

MAX_NAME_LENGTH = 60
"""Maximum length of a YellowDog resource name."""

NAME_PATTERN = re.compile(r"[a-z](?:[a-z0-9_-]{0,58}[a-z0-9])?")
"""
A valid YellowDog resource name (match with ``fullmatch``): at most 60
characters, only lower-case letters, digits, ``_`` and ``-``, starting with a
letter and ending with a letter or digit.
"""

_TAG_PREFIX = "airflow"
_DIGEST_LENGTH = 12
_SHORT_DIGEST_LENGTH = 8
_NAME_FALLBACK = "x"
# Put before a sanitised name that does not start with a letter
_LEAD = "x-"
_EDGE_CHARS = "-_"

RUN_TAG_PREFIX_PATTERN = re.compile(rf"{_TAG_PREFIX}:[0-9a-f]{{{_DIGEST_LENGTH}}}")
"""Matches (with ``fullmatch``) a :meth:`RunStamp.run_tag_prefix` value."""


def _digest(*parts: str | int, length: int = _DIGEST_LENGTH) -> str:
    """Stable (cross-process) hex digest of the parts."""
    joined = "\x1f".join(str(p) for p in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:length]


def sanitise_name(raw: str) -> str:
    """
    Lower-case and reduce to ``[a-z0-9_-]``, collapsing other runs to ``-``,
    then make it start with a letter and end with a letter or digit: leading
    and trailing ``-`` and ``_`` are removed, and ``x-`` is put before a
    result that starts with a digit (``x`` if nothing is left). The result is
    not shortened.
    """
    cleaned = re.sub(r"[^a-z0-9_-]+", "-", raw.lower()).strip(_EDGE_CHARS)
    if not cleaned:
        return _NAME_FALLBACK
    return cleaned if cleaned[0].isalpha() else f"{_LEAD}{cleaned}"


def configured_deployment_id() -> str:
    """
    The Airflow deployment's ``[yellowdog] deployment_id`` setting
    (environment variable ``AIRFLOW__YELLOWDOG__DEPLOYMENT_ID``), or ``""``
    if unset.
    """
    return conf.get("yellowdog", "deployment_id", fallback="").strip()


@dataclass(frozen=True)
class RunStamp:
    """
    Identifies the DAG run and task that submitted a work requirement.

    ``deployment_id`` distinguishes Airflow deployments that share YellowDog
    credentials (and so could have DAG runs with the same DAG and run IDs);
    when non-empty, it is part of both digests (:meth:`run_tag_prefix` and
    :meth:`tag`). When empty, the stamp is the same as without it.
    """

    dag_id: str
    run_id: str
    task_id: str
    map_index: int
    deployment_id: str = ""

    @classmethod
    def from_context(cls, context: Context) -> "RunStamp":
        """
        Build a stamp from the Airflow task context (``ti`` attributes) and
        the ``[yellowdog] deployment_id`` setting (see
        :func:`configured_deployment_id`).
        """
        if "ti" not in context:
            raise YellowDogException("Task context has no task instance ('ti')")
        ti = context["ti"]
        map_index = ti.map_index
        return cls(
            dag_id=ti.dag_id,
            run_id=ti.run_id,
            task_id=ti.task_id,
            map_index=-1 if map_index is None else map_index,
            deployment_id=configured_deployment_id(),
        )

    def _deployment(self) -> tuple[str, ...]:
        """The digest parts for the deployment: none if it has no ID."""
        return (self.deployment_id,) if self.deployment_id else ()

    def run_tag_prefix(self) -> str:
        """Tag prefix shared by every work requirement of this DAG run."""
        digest = _digest(*self._deployment(), self.dag_id, self.run_id)
        return f"{_TAG_PREFIX}:{digest}"

    def tag(self) -> str:
        """Tag unique to this task (and map index) within the DAG run."""
        digest = _digest(*self._deployment(), self.task_id, self.map_index)
        return f"{self.run_tag_prefix()}:{digest}"


def _shorten(raw: str, limit: int) -> str:
    """
    Sanitise (see :func:`sanitise_name`), truncating (with an 8-hex hash of
    the original) to ``limit``. The result starts with a letter.
    """
    # Uniqueness of derived names comes from their final digest, not these parts.
    clean = sanitise_name(raw)
    if len(clean) <= limit:
        return clean
    suffix = _digest(raw, length=_SHORT_DIGEST_LENGTH)
    # clean starts with a letter, so the head is never empty
    return f"{clean[: limit - len(suffix) - 1].rstrip(_EDGE_CHARS)}-{suffix}"


def derive_name(stamp: RunStamp) -> str:
    """
    Derive a unique-per-stamp YellowDog name that is valid (matches
    :data:`NAME_PATTERN`): it starts with the (sanitised) DAG ID, which starts
    with a letter, and ends with an 8-hex digest.
    """
    unique = _digest(stamp.dag_id, stamp.run_id, stamp.task_id, stamp.map_index)
    # budget: dag(17) - run(18) - task(14) - 8-hex digest, plus 3 separators
    return "-".join(
        [
            _shorten(stamp.dag_id, 17),
            _shorten(stamp.run_id, 18),
            _shorten(stamp.task_id, 14),
            unique[:_SHORT_DIGEST_LENGTH],
        ]
    )


def try_name(name: str, try_number: int) -> str:
    """Append ``-try<N>``, keeping the result within 60 characters.

    When ``name`` is too long, characters are removed from just before its
    last 8 characters, so the tail (the uniqueness digest of a derived name,
    or the end of a user-supplied one) is always preserved. The result ends
    with a digit and starts as ``name`` does, so a valid name (see
    :data:`NAME_PATTERN`) gives a valid name.
    """
    suffix = f"-try{try_number}"
    room = MAX_NAME_LENGTH - len(suffix)
    if len(name) <= room:
        return f"{name}{suffix}"
    tail = name[-_SHORT_DIGEST_LENGTH:]
    # Keep at least the first character (a valid name's leading letter)
    head = name[: max(room - len(tail), 1)].rstrip(_EDGE_CHARS) or name[:1]
    # Only an absurdly large try number makes this longer than the limit
    return f"{head}{tail}{suffix}"[:MAX_NAME_LENGTH].rstrip(_EDGE_CHARS)
