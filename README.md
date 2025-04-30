# YellowDog Provider for Apache Airflow

The YellowDog Provider for Apache Airflow provides:

- A Hook to support YellowDog Application credentials as Airflow Connections
- Operator classes to create and manage YellowDog Work Requirements and Worker Pools
- Sensor classes to monitor the states of YellowDog Work Requirements and Worker Pools

## Documentation

The main documentation is found at: https://docs.yellowdog.ai/airflow-provider/index.html

## Requirements

Requires Apache Airflow v2.10.0 or later, Python 3.10 or later, and the [YellowDog SDK](https://pypi.org/project/yellowdog-sdk).

## Installation

Install from PyPI as package `yellowdog-airflow-provider`, e.g.:

```commandline
pip install -U yellowdog-airflow-provider
```

## Usage

Please see the documentation at https://docs.yellowdog.ai/airflow-provider/index.html.

## Testing and Experimentation

### Starting a Local Airflow Instance

The script [start-airflow-container.sh](bin/start-airflow-container.sh) will start a local container running Airflow, including installing the YellowDog Airflow provider and creating YellowDog Connections.

Run this script from the top-level repo. directory, i.e.:

```commandline
bin/start-airflow-command.sh
```

Please inspect the script for instructions on its customisation and use.

### Running an Example DAG

There's an example DAG that exercises all the YellowDog Provider components at [yd-provider-test.py](yellowdog_provider/example_dags/yd-provider-test.py). The DAG's directory is automatically loaded by the Airflow container above.

Inspect/update the variables at beginning of the DAG module to customise the DAG for your environment.
