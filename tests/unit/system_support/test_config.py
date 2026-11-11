import re
from datetime import datetime
from pathlib import Path

import pytest

from tests.system.config import (
    AIRFLOW_ENV_VAR,
    DEFAULT_CONFIG_PATH,
    ConfigError,
    airflow_labels,
    config_path,
    load_config,
    new_run_tag,
    task_arguments_or_skip,
)

FULL = """
[platform]
api_url = "https://api.example.test"
key_id = "kid"
key_secret = "s3cr3t-value"

[resources]
namespace = "ns"
compute_requirement_template = "ns/template"
image_family = "ns/image"
task_type = "bash"
task_arguments = ["a", "b"]

[limits]
max_instances = 3
poke_interval_seconds = 2.5
sensor_timeout_seconds = 60

[e2e]
airflow_versions = ["min", "3.2.1"]
"""

MINIMAL = """
[platform]
key_id = "kid"
key_secret = "s3cr3t-value"

[resources]
namespace = "ns"
compute_requirement_template = "ns/template"
image_family = "ns/image"
"""


def write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "config.toml"
    path.write_text(text)
    return path


def test_load_config_full(tmp_path: Path) -> None:
    cfg = load_config(write(tmp_path, FULL))
    assert cfg.api_url == "https://api.example.test"
    assert cfg.key_id == "kid"
    assert cfg.key_secret == "s3cr3t-value"
    assert cfg.namespace == "ns"
    assert cfg.compute_requirement_template == "ns/template"
    assert cfg.image_family == "ns/image"
    assert cfg.task_type == "bash"
    assert cfg.task_arguments == ["a", "b"]
    assert cfg.max_instances == 3
    assert cfg.poke_interval_seconds == 2.5
    assert cfg.sensor_timeout_seconds == 60
    assert cfg.airflow_versions == ["min", "3.2.1"]


def test_load_config_defaults(tmp_path: Path) -> None:
    cfg = load_config(write(tmp_path, MINIMAL))
    assert cfg.api_url == "https://api.yellowdog.ai"
    assert cfg.task_type == "docker"
    assert cfg.task_arguments == ["hello-world"]
    assert cfg.max_instances == 1
    assert cfg.poke_interval_seconds == 10
    assert cfg.sensor_timeout_seconds == 900
    assert cfg.airflow_versions == ["min", "max"]


def test_load_config_missing_required(tmp_path: Path) -> None:
    path = write(
        tmp_path,
        '[platform]\nkey_secret = "s3cr3t-value"\n[resources]\nnamespace = "ns"\n',
    )
    with pytest.raises(ConfigError) as exc:
        load_config(path)
    message = str(exc.value)
    for key in (
        "platform.key_id",
        "resources.compute_requirement_template",
        "resources.image_family",
    ):
        assert key in message
    assert "platform.key_secret" not in message
    assert "s3cr3t-value" not in message


def test_config_path_env_override(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("YD_SYSTEM_TEST_CONFIG", raising=False)
    assert config_path() == DEFAULT_CONFIG_PATH
    # Anchored to tests/system, not the current directory
    repo = Path(__file__).resolve().parents[3]
    assert repo / "tests" / "system" / "config.toml" == DEFAULT_CONFIG_PATH
    monkeypatch.setenv("YD_SYSTEM_TEST_CONFIG", str(tmp_path / "x.toml"))
    assert config_path() == tmp_path / "x.toml"


def test_run_tag_format_and_length() -> None:
    tag = new_run_tag(datetime(2026, 11, 10, 9, 8, 7))
    assert re.fullmatch(r"systest-20261110-090807-[0-9a-f]{6}", tag)
    assert len(tag) <= 30
    assert new_run_tag() != new_run_tag()


def test_user_data_loaded_relative_to_config_dir(tmp_path: Path) -> None:
    (tmp_path / "userdata.sh").write_text("#!/bin/bash\necho hi\n")
    text = MINIMAL + 'user_data_file = "userdata.sh"\n'
    cfg = load_config(write(tmp_path, text))
    assert cfg.user_data == "#!/bin/bash\necho hi\n"


def test_user_data_absolute_path(tmp_path: Path) -> None:
    other = tmp_path / "elsewhere"
    other.mkdir()
    script = other / "ud.sh"
    script.write_text("abs")
    conf_dir = tmp_path / "conf"
    conf_dir.mkdir()
    text = MINIMAL + f'user_data_file = "{script.as_posix()}"\n'
    assert load_config(write(conf_dir, text)).user_data == "abs"


def test_user_data_absent_is_none(tmp_path: Path) -> None:
    assert load_config(write(tmp_path, MINIMAL)).user_data is None


def test_user_data_missing_file_raises(tmp_path: Path) -> None:
    text = MINIMAL + 'user_data_file = "nope.sh"\n'
    with pytest.raises(ConfigError) as exc:
        load_config(write(tmp_path, text))
    assert "user_data_file" in str(exc.value)
    assert str(tmp_path / "nope.sh") in str(exc.value)


@pytest.mark.parametrize("value", ["", "  \n\t\n"])
def test_blank_user_data_is_none(tmp_path: Path, value: str) -> None:
    (tmp_path / "userdata.sh").write_text(value)
    text = MINIMAL + 'user_data_file = "userdata.sh"\n'
    assert load_config(write(tmp_path, text)).user_data is None


@pytest.mark.parametrize("value", ["1", "true", '["a.sh"]'])
def test_user_data_file_must_be_string(tmp_path: Path, value: str) -> None:
    text = MINIMAL + f"user_data_file = {value}\n"
    with pytest.raises(ConfigError) as exc:
        load_config(write(tmp_path, text))
    assert "resources.user_data_file" in str(exc.value)


def test_ui_port_absent_is_none(tmp_path: Path) -> None:
    assert load_config(write(tmp_path, MINIMAL)).ui_port is None


def test_ui_port_valid(tmp_path: Path) -> None:
    text = MINIMAL + "\n[e2e]\nui_port = 8080\n"
    assert load_config(write(tmp_path, text)).ui_port == 8080


@pytest.mark.parametrize("value", ['"8080"', "0", "70000", "true", "80.5"])
def test_ui_port_invalid(tmp_path: Path, value: str) -> None:
    text = MINIMAL + f"\n[e2e]\nui_port = {value}\n"
    with pytest.raises(ConfigError) as exc:
        load_config(write(tmp_path, text))
    assert "e2e.ui_port" in str(exc.value)


def test_airflow_image_is_rejected(tmp_path: Path) -> None:
    text = MINIMAL + '\n[e2e]\nairflow_image = "apache/airflow:slim-latest"\n'
    with pytest.raises(ConfigError) as exc:
        load_config(write(tmp_path, text))
    assert "e2e.airflow_image" in str(exc.value)
    assert "airflow_versions" in str(exc.value)


@pytest.mark.parametrize(
    "value", ["[]", '"min"', '["latest"]', '["3.2"]', "[3]", '["min", 1]']
)
def test_airflow_versions_invalid(tmp_path: Path, value: str) -> None:
    text = MINIMAL + f"\n[e2e]\nairflow_versions = {value}\n"
    with pytest.raises(ConfigError) as exc:
        load_config(write(tmp_path, text))
    assert "e2e.airflow_versions" in str(exc.value)


def test_airflow_labels_from_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(AIRFLOW_ENV_VAR, raising=False)
    assert airflow_labels(["max", "3.2.1"]) == ["max", "3.2.1"]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("min", ["min"]),
        ("min max", ["min", "max"]),
        (" max,min ", ["max", "min"]),
        ("min, 3.2.1", ["min", "3.2.1"]),
        ("min min", ["min"]),
        ("  ", ["max"]),  # blank counts as unset
    ],
)
def test_airflow_labels_env_override(
    monkeypatch: pytest.MonkeyPatch, value: str, expected: list[str]
) -> None:
    monkeypatch.setenv(AIRFLOW_ENV_VAR, value)
    assert airflow_labels(["max"]) == expected


def test_airflow_labels_env_invalid(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(AIRFLOW_ENV_VAR, "min latest")
    with pytest.raises(ConfigError) as exc:
        airflow_labels(["min"])
    assert AIRFLOW_ENV_VAR in str(exc.value)
    assert "latest" in str(exc.value)


def test_failing_and_long_task_arguments_docker_defaults(tmp_path: Path) -> None:
    cfg = load_config(write(tmp_path, MINIMAL))  # task_type defaults to docker
    assert cfg.task_type == "docker"
    assert cfg.failing_task_arguments == ("alpine", "false")
    assert cfg.long_task_arguments == ("alpine", "sleep", "900")


def test_failing_and_long_task_arguments_bash_defaults(tmp_path: Path) -> None:
    cfg = load_config(write(tmp_path, MINIMAL + 'task_type = "bash"\n'))
    assert cfg.failing_task_arguments == ("-c", "exit 1")
    assert cfg.long_task_arguments == ("-c", "sleep 900")


def test_failing_and_long_task_arguments_unknown_type_unset(tmp_path: Path) -> None:
    cfg = load_config(write(tmp_path, MINIMAL + 'task_type = "custom"\n'))
    assert cfg.failing_task_arguments is None
    assert cfg.long_task_arguments is None
    with pytest.raises(pytest.skip.Exception, match="no default for task type"):
        task_arguments_or_skip(cfg, "failing_task_arguments")
    with pytest.raises(
        pytest.skip.Exception, match=re.escape("resources.long_task_arguments")
    ):
        task_arguments_or_skip(cfg, "long_task_arguments")


@pytest.mark.parametrize("task_type", ["bash", "docker", "custom"])
def test_explicit_task_arguments_win(tmp_path: Path, task_type: str) -> None:
    text = (
        MINIMAL
        + f'task_type = "{task_type}"\n'
        + 'failing_task_arguments = ["f"]\nlong_task_arguments = ["l", "m"]\n'
    )
    cfg = load_config(write(tmp_path, text))
    assert cfg.failing_task_arguments == ("f",)
    assert task_arguments_or_skip(cfg, "long_task_arguments") == ["l", "m"]


def test_failing_and_long_task_arguments_configured(tmp_path: Path) -> None:
    text = (
        MINIMAL + 'failing_task_arguments = ["x", "y"]\nlong_task_arguments = ["z"]\n'
    )
    cfg = load_config(write(tmp_path, text))
    assert cfg.failing_task_arguments == ("x", "y")
    assert cfg.long_task_arguments == ("z",)


@pytest.mark.parametrize("key", ["failing_task_arguments", "long_task_arguments"])
@pytest.mark.parametrize("value", ["[]", '"false"', "[1]"])
def test_task_arguments_lists_invalid(tmp_path: Path, key: str, value: str) -> None:
    with pytest.raises(ConfigError, match=f"resources.{key}"):
        load_config(write(tmp_path, MINIMAL + f"{key} = {value}\n"))
