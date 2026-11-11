# YellowDog Provider for Apache Airflow

The YellowDog Provider for Apache Airflow supports seamless integration of the [YellowDog Platform](https://yellowdog.ai) with Apache Airflow.

- Provision a worker pool, run work requirements on it and shut it down, as three Airflow tasks (setup, work, teardown)
- Deferrable waits that free worker slots while YellowDog does the work
- Safe retries, and cancellation of work (aborting running tasks) when a task gives up or is killed
- Sensors for work requirement, worker pool and compute requirement states
- A Hook for YellowDog credentials in Airflow Connections, usable from TaskFlow tasks, and links to the YellowDog Portal

Documentation is found at: https://docs.yellowdog.ai/airflow-provider/index.html.

Requires Apache Airflow 3.1 or later and Python 3.12, 3.13 or 3.14. Upgrading from 1.x? See the [changelog in the documentation](https://docs.yellowdog.ai/airflow-provider/changelog.html).
