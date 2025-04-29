#!/usr/bin/env bash

# Starts an Airflow server under Docker
# Installs the YellowDog Airflow Provider
# Adds YellowDog Connections

# Requires Bash and Docker running. To run under Windows, use WSL.

# Run this from the main 'airflow-provider-yellowdog' directory
# i.e., bin/start-airflow-container.sh

# The added YellowDog Connections depend on finding the 'YD_VAR_APP_'
# environment variables for the Application keys and secrets.
# Adjust as required for your installation.

# Once the container completes its startup, point your browser at:
#   http://localhost:8080
# and login using username/password = admin/admin

docker run --rm -it -p 8080:8080 \
    -v "$(pwd)/yellowdog_provider/example_dags/":/opt/airflow/dags \
    -v "$(pwd)":/opt/airflow/airflow-provider-yellowdog \
    --name airflow-provider-yellowdog \
    --entrypoint=/bin/bash \
    apache/airflow:latest-python3.12 \
    -c "cp -r /opt/airflow/airflow-provider-yellowdog /tmp && \
        pip install -U /tmp/airflow-provider-yellowdog && \
        pip install apache-airflow-providers-fab && \
        export AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager && \
        ( airflow db migrate && \
          airflow users create --username admin --password admin --firstname \
              YellowDog --lastname User --role Admin --email admin@example.org && \
          airflow connections add \
            --conn-type=yellowdog \
            --conn-host='https://api.yellowdog.ai' \
            --conn-login=$YD_VAR_APP_KEY_DEMO \
            --conn-password=$YD_VAR_APP_SECRET_DEMO \
            --conn-description='Production Platform > Account: demo > Application: yd-demo' \
            prod-demo-yd-demo && \
          airflow connections add \
            --conn-type=yellowdog \
            --conn-host='https://api.yellowdog.ai' \
            --conn-login=$YD_VAR_APP_KEY_YELLOWDOG \
            --conn-password=$YD_VAR_APP_SECRET_YELLOWDOG \
            --conn-description='Production Platform > Account: yellowdog > Application: yd-demo' \
            prod-yellowdog-yd-demo && \
          airflow connections add \
            --conn-type=yellowdog \
            --conn-host='https://test.yellowdog.tech/api' \
            --conn-login=$YD_VAR_APP_KEY_TEST \
            --conn-password=$YD_VAR_APP_SECRET_TEST \
            --conn-description='Test Platform > Account: yellowdog > Application: yd-demo' \
            test-yellowdog-yd-demo \
        ); \
        airflow api-server & \
        airflow triggerer & \
        airflow dag-processor & \
        airflow scheduler"
