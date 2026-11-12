import re
from unittest.mock import patch

import pytest

from tests.support import make_context
from yellowdog_provider.utils import run_stamp
from yellowdog_provider.utils.run_stamp import (
    INSTANCE_TAG_PREFIX_PATTERN,
    MAX_NAME_LENGTH,
    NAME_PATTERN,
    RUN_TAG_PREFIX_PATTERN,
    TASK_TAG_PREFIX_PATTERN,
    RunStamp,
    derive_name,
    sanitise_name,
    task_tag_prefix_for,
    try_name,
)

# YellowDog's rule: at most 60 characters of [a-z0-9_-], starting with a
# letter and ending with a letter or digit
NAME_RE = re.compile(r"^[a-z](?:[a-z0-9_-]{0,58}[a-z0-9])?$")


def _stamp(**kw: object) -> RunStamp:
    base: dict[str, object] = dict(dag_id="d", run_id="r", task_id="t", map_index=-1)
    base.update(kw)
    return RunStamp(**base)  # type: ignore[arg-type]


def test_tag_stable_and_distinct() -> None:
    assert _stamp().tag() == _stamp().tag()
    tags = {
        _stamp().tag(),
        _stamp(task_id="t2").tag(),
        _stamp(map_index=0).tag(),
        _stamp(run_id="r2").tag(),
    }
    assert len(tags) == 4
    assert len(_stamp().tag()) <= 64


def test_tag_starts_with_run_prefix() -> None:
    a, b = _stamp(task_id="x"), _stamp(task_id="y", map_index=2)
    assert a.tag().startswith(a.run_tag_prefix())
    assert a.run_tag_prefix() == b.run_tag_prefix()
    assert _stamp(run_id="other").run_tag_prefix() != a.run_tag_prefix()


def test_derive_name_sanitises_run_id() -> None:
    name = derive_name(
        _stamp(dag_id="My.DAG", run_id="manual__2026-11-11T10:00:00+00:00")
    )
    assert NAME_RE.match(name)


def test_derive_name_unique_for_long_ids() -> None:
    a = derive_name(_stamp(run_id="a" * 200 + "1"))
    b = derive_name(_stamp(run_id="a" * 200 + "2"))
    assert a != b
    assert len(a) <= MAX_NAME_LENGTH and len(b) <= MAX_NAME_LENGTH
    assert NAME_RE.match(a) and NAME_RE.match(b)


def test_derive_name_distinguishes_map_index() -> None:
    assert derive_name(_stamp(map_index=1)) != derive_name(_stamp(map_index=2))


def test_sanitise_name() -> None:
    assert sanitise_name("My.DAG Run!") == "my-dag-run"
    assert sanitise_name("...") == "x"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2026-run", "x-2026-run"),
        ("_private_", "private"),
        ("__9__", "x-9"),
        ("-_-", "x"),
        ("run_", "run"),
    ],
)
def test_sanitise_name_starts_with_letter_ends_alnum(raw: str, expected: str) -> None:
    assert sanitise_name(raw) == expected
    assert NAME_RE.match(expected)


def test_name_pattern_is_the_platform_rule() -> None:
    for good in ("a", "a1", "a-b_c9", "a" * 60):
        assert NAME_PATTERN.fullmatch(good)
    for bad in ("", "1a", "_a", "-a", "a-", "a_", "A", "a" * 61, "a.b"):
        assert not NAME_PATTERN.fullmatch(bad)


@pytest.mark.parametrize(
    "stamp",
    [
        _stamp(dag_id="2026-dag"),
        _stamp(dag_id="_private_dag"),
        _stamp(dag_id="9" * 80, run_id="_" * 30, task_id="-" * 30),
        _stamp(dag_id="d" * 16 + "_x", run_id="r" * 17 + "-x", task_id="t" * 13 + "_x"),
        _stamp(dag_id="...", run_id="...", task_id="..."),
        _stamp(dag_id="My.DAG", run_id="manual__2026-11-11T10:00:00+00:00"),
    ],
)
def test_derive_name_always_valid(stamp: RunStamp) -> None:
    name = derive_name(stamp)
    assert NAME_RE.match(name), name
    for n in (1, 2, 99, 12345):
        assert NAME_RE.match(try_name(name, n))


def test_derive_name_unchanged_for_letter_led_ids() -> None:
    # The documented example
    stamp = _stamp(
        dag_id="my-dag", run_id="manual__2026-11-11T10:00:00+00:00", task_id="run"
    )
    assert derive_name(stamp) == "my-dag-manual__2-f25068ed-run-2152119a"


@pytest.mark.parametrize(
    "name",
    [
        "a",
        "wr",
        "a" * 60,
        "a" * 51 + "-" + "b" * 8,  # the cut lands on "-"
        "a" * 50 + "__" + "b" * 8,  # the cut lands on "_"
        "a-" * 29 + "b9",
        "a" + "_" * 50 + "12345678",
    ],
)
def test_try_name_of_valid_name_is_valid(name: str) -> None:
    assert NAME_RE.match(name)
    for n in (1, 7, 42, 100, 1000, 10**6):
        result = try_name(name, n)
        assert NAME_RE.match(result), result
        assert result.endswith(f"-try{n}")
        if len(name) > MAX_NAME_LENGTH - len(f"-try{n}"):
            assert result[-len(f"-try{n}") - 8 : -len(f"-try{n}")] == name[-8:]


def test_try_name_respects_limit() -> None:
    name = try_name("a" * 60, 12)
    assert len(name) <= MAX_NAME_LENGTH
    assert name.endswith("-try12")
    assert try_name("abc", 2) == "abc-try2"


def test_from_context_mapped_task() -> None:
    mapped = RunStamp.from_context(make_context(map_index=3))
    plain = RunStamp.from_context(make_context(map_index=-1))
    assert mapped.map_index == 3 and plain.map_index == -1
    assert mapped.tag() != plain.tag()
    assert RunStamp.from_context(make_context(map_index=None)).map_index == -1


def test_try_name_preserves_trailing_digest() -> None:
    name = derive_name(_stamp(dag_id="d" * 50, run_id="r" * 50, task_id="t" * 50))
    assert len(name) == MAX_NAME_LENGTH
    for n in (7, 42, 100, 1000):
        result = try_name(name, n)
        assert len(result) <= MAX_NAME_LENGTH
        assert name[-8:] in result
        assert result.endswith(f"-try{n}")
        assert NAME_RE.match(result)


def test_try_name_unique_across_stamps() -> None:
    a = _stamp(dag_id="d" * 50, run_id="r" * 50, task_id="t" * 50 + "1")
    b = _stamp(dag_id="d" * 50, run_id="r" * 50, task_id="t" * 50 + "2")
    assert derive_name(a) != derive_name(b)
    assert try_name(derive_name(a), 100) != try_name(derive_name(b), 100)


def test_empty_deployment_id_leaves_stamp_unchanged() -> None:
    # Digests from before the deployment ID was introduced
    import hashlib

    def digest(*parts: object) -> str:
        joined = "\x1f".join(str(p) for p in parts)
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:12]

    stamp = _stamp()
    assert stamp.run_tag_prefix() == f"airflow:{digest('d', 'r')}"
    assert stamp.task_tag_prefix() == f"airflow:{digest('d', 'r')}:{digest('t')}"
    assert stamp.tag() == f"airflow:{digest('d', 'r')}:{digest('t')}:m-1"
    assert _stamp(deployment_id="").tag() == stamp.tag()


def test_deployment_id_changes_prefix_and_tag() -> None:
    plain, a, b = _stamp(), _stamp(deployment_id="a"), _stamp(deployment_id="b")
    prefixes = {plain.run_tag_prefix(), a.run_tag_prefix(), b.run_tag_prefix()}
    assert len(prefixes) == 3
    assert len({plain.tag(), a.tag(), b.tag()}) == 3
    assert a.tag().startswith(a.run_tag_prefix() + ":")
    assert RUN_TAG_PREFIX_PATTERN.fullmatch(a.run_tag_prefix())


def test_from_context_reads_deployment_id() -> None:
    with patch.object(run_stamp.conf, "get", return_value=" prod ") as get:
        stamp = RunStamp.from_context(make_context())
    get.assert_called_once_with("yellowdog", "deployment_id", fallback="")
    assert stamp.deployment_id == "prod"
    with patch.object(run_stamp.conf, "get", return_value=""):
        assert RunStamp.from_context(make_context()).deployment_id == ""


def test_tag_format_and_map_index_part() -> None:
    assert re.fullmatch(r"airflow:[0-9a-f]{12}:[0-9a-f]{12}:m-1", _stamp().tag())
    assert _stamp(map_index=12).tag().endswith(":m12")
    assert TASK_TAG_PREFIX_PATTERN.fullmatch(_stamp().task_tag_prefix())
    assert not TASK_TAG_PREFIX_PATTERN.fullmatch(_stamp().tag())
    assert not TASK_TAG_PREFIX_PATTERN.fullmatch(_stamp().run_tag_prefix())


def test_instance_tag_prefix_pattern() -> None:
    for map_index in (-1, 0, 12):
        assert INSTANCE_TAG_PREFIX_PATTERN.fullmatch(_stamp(map_index=map_index).tag())
    task = _stamp().task_tag_prefix()
    for bad in (task, f"{task}:m", f"{task}:mx", f"{task}:m1:c0", f"{task}:1"):
        assert not INSTANCE_TAG_PREFIX_PATTERN.fullmatch(bad), bad


def test_task_tag_prefix_shared_by_map_indices_not_task_ids() -> None:
    assert (
        _stamp(map_index=0).task_tag_prefix() == _stamp(map_index=1).task_tag_prefix()
    )
    assert _stamp(map_index=0).tag() != _stamp(map_index=1).tag()
    assert _stamp().task_tag_prefix() != _stamp(task_id="t2").task_tag_prefix()
    assert _stamp().tag().startswith(_stamp().task_tag_prefix() + ":")


def test_deployment_id_changes_task_tag_prefix() -> None:
    assert _stamp().task_tag_prefix() != _stamp(deployment_id="a").task_tag_prefix()


def test_task_tag_prefix_for_other_task() -> None:
    stamp = _stamp(map_index=3, deployment_id="dep")
    assert (
        task_tag_prefix_for(stamp, "other")
        == _stamp(task_id="other", deployment_id="dep").task_tag_prefix()
    )
