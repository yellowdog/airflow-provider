"""Configure an isolated, quiet Airflow before anything imports it."""

import os
import tempfile

os.environ["AIRFLOW_HOME"] = tempfile.mkdtemp(prefix="airflow-home-")
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"

pytest_plugins = ["pytester"]
