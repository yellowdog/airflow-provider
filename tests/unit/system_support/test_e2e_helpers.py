"""Offline tests for the e2e test's helpers (no Docker, network or platform)."""

import subprocess
from pathlib import Path
from typing import Any

import pytest

import tests.system.test_example_dag_e2e as e2e
from tests.system.config import load_config
from tests.system.test_example_dag_e2e import (
    _airflow_targets,
    _container_name,
    _dag_conf,
    _dump_diagnostics,
    _install_env,
    _parse_json,
    _poll,
    _pull_failure_message,
    _resolve_target,
    _should_remove_container,
    _target_ui_port,
    _ui_access_message,
    _ui_port_args,
)

NOISY = (
    "2026-11-10T16:27:09.167943Z [warning  ] Could not import graphviz. "
    "Rendering graph to the graphical format will not be possible. \n"
    "You might need to install the graphviz package and necessary system "
    "packages.\n"
    "Run `pip install graphviz` to attempt to install it. [py.warnings] "
    "category=UserWarning filename=/x/dag_command.py lineno=57\n"
    "2026-11-10T16:27:09.279875Z [info     ] setup plugin "
    "alembic.autogenerate.schemas [alembic.runtime.plugins] loc=plugins.py:37\n"
    '[{"dag_id": "yellowdog-provider-test", "is_paused": "True", '
    '"bundle_version": null}]\n'
)
EXPECTED = [
    {"dag_id": "yellowdog-provider-test", "is_paused": "True", "bundle_version": None}
]


def test_parses_json_after_log_noise() -> None:
    assert _parse_json(NOISY) == EXPECTED


def test_parses_clean_json() -> None:
    assert _parse_json('[{"a": 1}]\n') == [{"a": 1}]


def test_parses_clean_multiline_json() -> None:
    assert _parse_json('[\n  {"a": 1}\n]\n') == [{"a": 1}]


def test_payload_running_to_end_preferred_over_earlier_json() -> None:
    text = 'log [1] and {"a": 2} noise\n[{"dag_id": "d"}]\n'
    assert _parse_json(text) == [{"dag_id": "d"}]


def test_payload_followed_by_log_noise() -> None:
    text = '[{"dag_id": "d"}]\n2026-11-10T16:27:09Z [info     ] done\n'
    assert _parse_json(text) == [{"dag_id": "d"}]


def test_payload_after_json_log_line_and_before_trailing_noise() -> None:
    # Regression: decodable JSON inside an earlier log line must not win
    text = 'log [1] and {"a": 2} noise\n[{"dag_id": "d"}]\nWARNING trailing\n'
    assert _parse_json(text) == [{"dag_id": "d"}]


def test_no_json_raises_value_error() -> None:
    with pytest.raises(ValueError):
        _parse_json("2026-11-10T16:27:09Z [info     ] nothing here\n")


def test_port_flag_absent_when_unset() -> None:
    assert _ui_port_args(None) == []


def test_port_flag_bound_to_localhost() -> None:
    assert _ui_port_args(8081) == ["-p", "127.0.0.1:8081:8080"]


def test_container_removed_unless_started_and_kept_for_ui() -> None:
    assert _should_remove_container(None, started=True) is True
    assert _should_remove_container(None, started=False) is True
    assert _should_remove_container(8080, started=True) is False
    assert _should_remove_container(8080, started=False) is True


def test_ui_access_message() -> None:
    msg = _ui_access_message("c1", 8080)
    assert "http://localhost:8080" in msg
    # Honours a simple_auth_manager_passwords_file override, else the default
    assert "docker exec c1 sh -c" in msg
    assert "airflow config get-value core simple_auth_manager_passwords_file" in msg
    assert "/opt/airflow/simple_auth_manager_passwords.json.generated" in msg


def _resolver(label: str) -> str:
    if label == "max":
        raise OSError("network down")
    return {"min": "3.1.0"}.get(label, label)


def test_airflow_targets_resolved() -> None:
    targets = _airflow_targets(["min", "3.2.1"], _resolver)
    assert [t.version for t in targets] == ["3.1.0", "3.2.1"]
    assert [t.id for t in targets] == ["airflow-min", "airflow-3.2.1"]
    assert [t.index for t in targets] == [0, 1]
    assert targets[0].image == "apache/airflow:slim-3.1.0-python3.12"
    assert all(t.error is None for t in targets)


def test_airflow_targets_resolution_error_recorded() -> None:
    (target,) = _airflow_targets(["max"], _resolver)
    assert target.version is None
    assert target.id == "airflow-max"
    assert target.error is not None and "network down" in target.error


def test_airflow_targets_unresolved_without_resolver() -> None:
    targets = _airflow_targets(["min", "max"], None)
    assert [t.id for t in targets] == ["airflow-min", "airflow-max"]
    assert all(t.version is None and t.error is None for t in targets)


def test_resolve_target_is_lazy_and_keeps_index() -> None:
    (unresolved,) = _airflow_targets(["max"], None)
    calls: list[str] = []

    def resolver(label: str) -> str:
        calls.append(label)
        return "3.4.0"

    resolved = _resolve_target(unresolved, resolver)
    assert calls == ["max"]
    assert resolved.version == "3.4.0" and resolved.index == unresolved.index
    failed = _resolve_target(unresolved, _resolver)
    assert failed.error is not None and "network down" in failed.error


def test_pull_failure_message_names_missing_tag() -> None:
    (target,) = _airflow_targets(["max"], lambda label: "9.9.9")
    msg = _pull_failure_message(target, "manifest unknown")
    assert "apache/airflow:slim-9.9.9-python3.12" in msg
    assert "manifest unknown" in msg
    assert "9.9.9" in msg


def test_poll_retries_timed_out_cli_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(e2e.time, "sleep", lambda s: None)
    results: list[Any] = [subprocess.TimeoutExpired(["airflow"], 120), None, "done"]

    def check() -> Any:
        result = results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    assert _poll(check, timeout=60, what="x", interval=0) == "done"


def test_poll_timeout_still_raised_after_cli_timeouts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(e2e.time, "sleep", lambda s: None)

    def check() -> Any:
        raise subprocess.TimeoutExpired(["airflow"], 120)

    with pytest.raises(TimeoutError):
        _poll(check, timeout=0, what="x", interval=0)


def test_dump_diagnostics_never_raises(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def broken(*args: Any, **kwargs: Any) -> Any:
        raise subprocess.TimeoutExpired(["docker"], 120)

    monkeypatch.setattr(e2e, "_run", broken)
    _dump_diagnostics("c1", "run-1", task_states=True)
    assert "diagnostics failed" in capsys.readouterr().out.lower()


def test_dag_conf_passes_pool_size_and_poke_interval(tmp_path: Path) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        '[platform]\nkey_id = "k"\nkey_secret = "s"\n'
        '[resources]\nnamespace = "ns"\ncompute_requirement_template = "t"\n'
        'image_family = "f"\n[limits]\nmax_instances = 3\n'
        "poke_interval_seconds = 2.5\n"
    )
    conf = _dag_conf(load_config(path))
    assert conf["max_nodes"] == 3
    assert conf["poke_interval_seconds"] == 2.5


def test_container_name_and_ui_port_per_version() -> None:
    first, second = _airflow_targets(["min", "max"], lambda label: "3.4.0")
    tag = "systest-20261110-090807-abcdef"
    assert _container_name(tag, first) == f"yd-e2e-{tag}-min"
    assert _container_name(tag, second) == f"yd-e2e-{tag}-max"
    assert _target_ui_port(None, second) is None
    assert _target_ui_port(8080, first) == 8080
    assert _target_ui_port(8080, second) == 8081


def test_install_env_selects_constraints_without_sdk_pin() -> None:
    versions = {"min": "3.1.0", "max": "3.4.0"}
    low, high = _airflow_targets(["min", "max"], lambda label: versions[label])
    assert _install_env(low) == {
        "YD_CONSTRAINTS_URL": "https://raw.githubusercontent.com/apache/airflow/"
        "constraints-3.1.0/constraints-3.12.txt",
    }
    assert _install_env(high)["YD_CONSTRAINTS_URL"].endswith(
        "constraints-3.4.0/constraints-3.12.txt"
    )
