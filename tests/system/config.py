"""Configuration for the live-platform system tests."""

import os
import re
import secrets
import tomllib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from tests.system.versions import is_label

# Anchored to this directory so the tests can run from any working directory
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "config.toml"
CONFIG_PATH_ENV_VAR = "YD_SYSTEM_TEST_CONFIG"
# Space/comma-separated Airflow version labels; overrides e2e.airflow_versions
AIRFLOW_ENV_VAR = "YD_SYSTEM_TEST_AIRFLOW"
DEFAULT_AIRFLOW_VERSIONS = ["min", "max"]


class ConfigError(Exception):
    """The system-test configuration is missing or invalid."""


@dataclass(frozen=True)
class SystemTestConfig:
    api_url: str
    key_id: str
    key_secret: str
    namespace: str
    compute_requirement_template: str
    image_family: str
    task_type: str
    task_arguments: list[str]
    max_instances: int
    poke_interval_seconds: float
    sensor_timeout_seconds: float
    airflow_versions: list[str]
    user_data: str | None
    ui_port: int | None = None


def config_path() -> Path:
    override = os.environ.get(CONFIG_PATH_ENV_VAR)
    return Path(override) if override else DEFAULT_CONFIG_PATH


def load_config(path: Path) -> SystemTestConfig:
    with path.open("rb") as f:
        data = tomllib.load(f)

    def section(name: str) -> dict[str, Any]:
        value = data.get(name, {})
        return value if isinstance(value, dict) else {}

    platform, resources = section("platform"), section("resources")
    limits, e2e = section("limits"), section("e2e")

    required = {
        "platform": ["key_id", "key_secret"],
        "resources": ["namespace", "compute_requirement_template", "image_family"],
    }
    missing = [
        f"{name}.{key}"
        for name, keys in required.items()
        for key in keys
        if key not in section(name)
    ]
    if missing:
        raise ConfigError(f"Missing required keys in {path}: {', '.join(missing)}")

    user_data: str | None = None
    if "user_data_file" in resources:
        user_data_file = resources["user_data_file"]
        if not isinstance(user_data_file, str):
            raise ConfigError(
                f"resources.user_data_file: must be a path string, "
                f"got {user_data_file!r}"
            )
        user_data_path = path.parent / Path(user_data_file)
        try:
            user_data = user_data_path.read_text(encoding="utf-8")
        except OSError as e:
            raise ConfigError(
                f"resources.user_data_file: cannot read {user_data_path}: {e}"
            ) from e
        # A blank script means no userdata, as in the example DAG
        if not user_data.strip():
            user_data = None

    ui_port = e2e.get("ui_port")
    if ui_port is not None and (
        isinstance(ui_port, bool)
        or not isinstance(ui_port, int)
        or not 1 <= ui_port <= 65535
    ):
        raise ConfigError(
            f"e2e.ui_port: must be an integer from 1 to 65535, got {ui_port!r}"
        )

    if "airflow_image" in e2e:
        raise ConfigError(
            "e2e.airflow_image has been replaced by e2e.airflow_versions "
            '(a list of "min", "max" or X.Y.Z Airflow versions)'
        )
    airflow_versions = e2e.get("airflow_versions", DEFAULT_AIRFLOW_VERSIONS)
    if (
        not isinstance(airflow_versions, list)
        or not airflow_versions
        or not all(isinstance(v, str) and is_label(v) for v in airflow_versions)
    ):
        raise ConfigError(
            'e2e.airflow_versions: must be a non-empty list of "min", "max" or '
            f"X.Y.Z, got {airflow_versions!r}"
        )

    return SystemTestConfig(
        api_url=platform.get("api_url", "https://api.yellowdog.ai"),
        key_id=platform["key_id"],
        key_secret=platform["key_secret"],
        namespace=resources["namespace"],
        compute_requirement_template=resources["compute_requirement_template"],
        image_family=resources["image_family"],
        task_type=resources.get("task_type", "docker"),
        task_arguments=list(resources.get("task_arguments", ["hello-world"])),
        max_instances=limits.get("max_instances", 1),
        poke_interval_seconds=limits.get("poke_interval_seconds", 10),
        sensor_timeout_seconds=limits.get("sensor_timeout_seconds", 900),
        airflow_versions=list(airflow_versions),
        user_data=user_data,
        ui_port=ui_port,
    )


def airflow_labels(configured: list[str]) -> list[str]:
    """
    The Airflow version labels to test: ``YD_SYSTEM_TEST_AIRFLOW`` (space or
    comma separated) if set and non-blank, else ``configured``. Duplicates are
    dropped, keeping the first occurrence.
    """
    override = os.environ.get(AIRFLOW_ENV_VAR, "").strip()
    labels = re.split(r"[\s,]+", override) if override else configured
    bad = [label for label in labels if not is_label(label)]
    if bad:
        raise ConfigError(
            f"{AIRFLOW_ENV_VAR}: invalid Airflow version label(s) {bad}; "
            'use "min", "max" or X.Y.Z'
        )
    return list(dict.fromkeys(labels))


def new_run_tag(now: datetime | None = None) -> str:
    """Return a unique tag, ``systest-YYYYMMDD-HHMMSS-<6 hex>`` (30 chars)."""
    now = now or datetime.now()
    return f"systest-{now:%Y%m%d-%H%M%S}-{secrets.token_hex(3)}"
