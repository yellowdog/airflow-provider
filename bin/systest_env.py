#!/usr/bin/env python3
"""Create or refresh the system-test venv for an Airflow version label (stdlib only).

``python bin/systest_env.py min|max|X.Y.Z`` makes ``.venv-systest-<label>`` (uv,
Python 3.12) with ``apache-airflow==<version>`` and the provider (``-e '.[dev]'``)
installed against Airflow's constraints. ``yellowdog-sdk`` is never pinned, so
the latest release is used for every label. A stamp file records what was
installed, including the latest ``yellowdog-sdk`` on PyPI, so an unchanged venv
(whose interpreter still exists) is not reinstalled, while a new Airflow or
``yellowdog-sdk`` release, or a ``pyproject.toml`` change, rebuilds it. Progress
and the resolved versions go to stderr; the venv path is printed on stdout.
"""

import argparse
import hashlib
import json
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import airflow_versions

ROOT = airflow_versions.ROOT
PYTHON = airflow_versions.DEFAULT_PYTHON
STAMP_NAME = ".systest-stamp.json"
REPORTED_PACKAGES = (
    "apache-airflow",
    "apache-airflow-task-sdk",
    "yellowdog-sdk",
    "urllib3",
)

Runner = Callable[[list[str]], str]


def venv_path(label: str, root: Path = ROOT) -> Path:
    return root / f".venv-systest-{label}"


def install_spec(label: str, root: Path = ROOT) -> dict[str, Any]:
    """What the venv for ``label`` should contain; compared with the stamp."""
    pyproject = root / "pyproject.toml"
    airflow = airflow_versions.resolve(label, pyproject)
    packages = [f"apache-airflow=={airflow}", "-e", ".[dev]"]
    return {
        "python": PYTHON,
        "packages": packages,
        "constraints": airflow_versions.constraints_url(airflow, PYTHON),
        # Not pinned, but recorded so a new release rebuilds the venv
        "yellowdog_sdk": airflow_versions.latest_sdk(),
        "pyproject_sha256": hashlib.sha256(pyproject.read_bytes()).hexdigest(),
    }


def read_stamp(venv: Path) -> dict[str, Any] | None:
    try:
        stamp = json.loads((venv / STAMP_NAME).read_text())
    except (OSError, ValueError):
        return None
    return stamp if isinstance(stamp, dict) else None


def installed_versions(venv: Path, run: Runner) -> dict[str, str]:
    code = (
        "import importlib.metadata as m, json\n"
        f"print(json.dumps({{p: m.version(p) for p in {list(REPORTED_PACKAGES)!r}}}))"
    )
    return json.loads(run([str(venv / "bin" / "python"), "-c", code]))


def ensure_env(label: str, run: Runner, root: Path = ROOT) -> Path:
    """Create or refresh the venv for ``label`` unless its stamp is current."""
    venv = venv_path(label, root)
    spec = install_spec(label, root)
    stamp = read_stamp(venv)
    python = venv / "bin" / "python"
    if stamp is not None and stamp.get("spec") == spec and python.exists():
        _log(f"{venv.name} is up to date")
        versions: dict[str, str] = stamp["versions"]
    else:
        (venv / STAMP_NAME).unlink(missing_ok=True)
        run(["uv", "venv", "--clear", "--python", PYTHON, str(venv)])
        run(
            [
                "uv",
                "pip",
                "install",
                "--python",
                str(python),
                *spec["packages"],
                "--constraint",
                spec["constraints"],
            ]
        )
        versions = installed_versions(venv, run)
        (venv / STAMP_NAME).write_text(
            json.dumps({"spec": spec, "versions": versions}, indent=2) + "\n"
        )
    _log(f"{venv.name}: " + ", ".join(f"{p} {v}" for p, v in versions.items()))
    return venv


def _log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def default_runner(cmd: list[str]) -> str:
    """Run ``cmd`` in the repo root; its stderr passes through, stdout is returned."""
    _log("+ " + " ".join(cmd))
    return subprocess.run(
        cmd, cwd=ROOT, check=True, stdout=subprocess.PIPE, text=True
    ).stdout


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("label", help="min, max or an explicit X.Y.Z version")
    args = parser.parse_args(argv)
    if not airflow_versions.is_label(args.label):
        parser.error(f"invalid label {args.label!r}: use min, max or X.Y.Z")
    try:
        venv = ensure_env(args.label, default_runner)
    except (
        subprocess.CalledProcessError,
        airflow_versions.VersionError,
        OSError,  # e.g. PyPI unreachable
    ) as e:
        _log(f"systest_env: {e}")
        return 1
    print(venv)
    return 0


if __name__ == "__main__":
    sys.exit(main())
