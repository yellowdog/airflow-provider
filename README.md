# YellowDog Provider for Apache Airflow

The YellowDog Provider for Apache Airflow provides:

- Operators for the usual three-task DAG: provision a YellowDog Worker Pool (a setup task that waits until the pool is ready), run Work Requirements on it (`RunWorkRequirement` submits and waits until the work completes), and shut the pool down (a teardown task that also cancels the unfinished work of its setup/teardown scope)
- Finer-grained operators to submit, extend and cancel Work Requirements step by step
- Sensor classes to monitor the states of YellowDog Work Requirements, Worker Pools and Compute Requirements
- Deferrable waits: operators and sensors can hand their waits to Airflow's triggerer instead of holding a worker slot
- Safe retries: submitted work is stamped with its DAG run and task, so a retry carries on with the earlier attempt rather than duplicating it, and work is cancelled (aborting running tasks) when a task gives up or is killed
- A Hook that keeps YellowDog Application credentials in Airflow Connections, with typed operations for TaskFlow `@task` functions
- Links from Airflow tasks to the YellowDog Portal

If more than one Airflow environment uses the same YellowDog account, set a distinct `[yellowdog] deployment_id` (`AIRFLOW__YELLOWDOG__DEPLOYMENT_ID`) in each; see the documentation.

## Documentation

The main documentation is found at: https://docs.yellowdog.ai/airflow-provider/index.html

## Requirements

Requires Apache Airflow 3.1 or later, Python 3.12, 3.13 or 3.14, and the [YellowDog SDK](https://pypi.org/project/yellowdog-sdk) (v15.10.0 or later, installed automatically).

Upgrading from 1.x? See the [changelog and migration guide](docs/changelog.rst).

## Installation

Install from PyPI as package `yellowdog-airflow-provider`, e.g.:

```commandline
pip install -U yellowdog-airflow-provider
```

## Usage

Please see the documentation at https://docs.yellowdog.ai/airflow-provider/index.html.

## Development

Create a virtual environment, then install the package with its development dependencies:

```commandline
make install
```

Useful targets:

- `make format` — format and auto-fix (ruff)
- `make check` — lint, type-check (pyright strict + `pyright --verifytypes`), unit tests and the docs build; run this before committing
- `make test` — unit tests only (`tests/unit`, `tests/release`)
- `make docs` — build the Sphinx docs (warnings are errors)

CI (GitHub Actions) runs the unit tests on the minimum supported Airflow (the `apache-airflow` floor in `pyproject.toml`, with the `yellowdog-sdk` floor) with Python 3.12 and 3.13, and on the latest Airflow with Python 3.12 and 3.14. It type-checks at both ends too: the minimum Airflow (with the `yellowdog-sdk` floor) on Python 3.12 and the latest Airflow on Python 3.14. Lint, the docs build and the package build run once, on Python 3.14. [bin/airflow_versions.py](bin/airflow_versions.py) is the single source for these versions (`python bin/airflow_versions.py min|max|sdk-min`).

### System tests (live YellowDog platform)

The system tests run against a real YellowDog platform, create (and clean up) real resources, and may incur cost. They are opt-in and never run in CI or by `make check`.

Copy [tests/system/config.example.toml](tests/system/config.example.toml) to `tests/system/config.toml` (git-ignored; or point `YD_SYSTEM_TEST_CONFIG` at another file) and fill in your credentials, namespace, compute requirement template and image family. An optional `user_data_file` supplies instance userdata for the test worker pools. To watch the e2e run in the Airflow UI, set `ui_port` under `[e2e]`: the UI is published on `localhost:<ui_port>` (bound to 127.0.0.1), the test prints how to get the login, and the container is kept afterwards (the test prints the `docker rm -f` command to remove it). Then:

```commandline
make system-test          # component tests against the live platform
make e2e-test             # full end-to-end test of the example DAG under Airflow
make system-test-cleanup  # sweep up any resources left behind by failed runs
```

By default both live test targets run at the lowest **and** highest supported Airflow: `min` (the `apache-airflow` floor in `pyproject.toml`) and `max` (the latest Airflow on PyPI). Live tests always use the latest `yellowdog-sdk` at both versions; only CI's min rows pin the `yellowdog-sdk` floor. Select versions with `AIRFLOW` on the make command line, e.g. `make system-test AIRFLOW=min` or `make e2e-test AIRFLOW="max 3.2.1"` (labels `min`, `max` or an explicit `X.Y.Z`, separated by spaces or commas). An `AIRFLOW` variable exported in your shell is ignored, so it can't silently change both targets.

- `make system-test` runs the component tests once per version, each in its own uv venv (`.venv-systest-<label>`, Python 3.12, installed with Airflow's constraints by [bin/systest_env.py](bin/systest_env.py) and reused until `pyproject.toml` changes or a new `yellowdog-sdk` release (or, for `max`, Airflow release) appears on PyPI). Every version runs even if another fails; the target fails if any did. The pytest header shows the Airflow and `yellowdog-sdk` versions in use.
- `make e2e-test` runs the e2e tests once per version in `apache/airflow:slim-<version>-python3.12`, each version with its own container: the example DAG with every wait deferred (must succeed), with failing work (the run must fail and its teardown succeed), and, on Airflow 3.3 and later, a deferred task marked failed through the REST API (its work requirement must be cancelled by the trigger). Failing and long-running work use `failing_task_arguments` and `long_task_arguments` under `[resources]`, which default by `task_type` (`bash` and `docker`; see the example config). For any other task type, set them, or the tests that need them (in both targets) are skipped. Without `AIRFLOW`, the versions come from `airflow_versions` under `[e2e]` in the config (default `["min", "max"]`); `YD_SYSTEM_TEST_AIRFLOW` also overrides it when running pytest directly. With `ui_port` set, the n-th version's UI is on `ui_port + n`. Versions are resolved when each version's container starts; if a new Airflow release has no `slim-<version>-python3.12` image on Docker Hub yet, the test fails naming the missing tag (choose an explicit version until the image is published).

### Starting a Local Airflow Instance

The script [start-airflow-container.sh](bin/start-airflow-container.sh) will start a local container running Airflow, including installing the YellowDog Airflow provider and creating YellowDog Connections.

Run this script from the top-level repo. directory, i.e.:

```commandline
bin/start-airflow-container.sh
```

Please inspect the script for instructions on its customisation and use.

### Example DAGs

The example DAGs are in [yellowdog_provider/example_dags](yellowdog_provider/example_dags), and are loaded automatically by the Airflow container above:

- [yd-quickstart.py](yellowdog_provider/example_dags/yd-quickstart.py) — a minimal DAG: one `RunWorkRequirement` on an existing worker pool
- [yd-provider-test.py](yellowdog_provider/example_dags/yd-provider-test.py) — the three-task pattern: provisions a worker pool (setup), runs two work requirements on it, and shuts it down (teardown); configured through DAG params (connection, namespace, compute requirement template, image family, task type and arguments, `sensor_timeout_seconds`, `poke_interval_seconds`, `max_nodes`, `deferrable`, optional `user_data`) when triggering the DAG

### Releasing

See [RELEASING.md](RELEASING.md). Releases are made from `main` with `make release` (`bin/release.py`).
