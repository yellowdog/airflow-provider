from typing import Any

__version__ = "2.0.0.dev0"


def get_provider_info() -> dict[str, Any]:
    from yellowdog_provider.hooks import _connection_ui as ui

    return {
        "package-name": "yellowdog-airflow-provider",
        "versions": [__version__],
        "name": "YellowDog",
        "description": "YellowDog provider for Apache Airflow",
        "connection-types": [
            {
                "connection-type": ui.CONNECTION_TYPE,
                "hook-class-name": (
                    "yellowdog_provider.hooks.yellowdog_hooks.YellowDogHook"
                ),
                "hook-name": "YellowDog",
                # Read by Airflow 3.2+ (older versions use the hook's methods)
                "conn-fields": {
                    ui.PORTAL_URL_FIELD: {
                        "label": ui.PORTAL_URL_NAME,
                        "description": ui.PORTAL_URL_DESCRIPTION,
                        "schema": {"type": ["string", "null"]},
                    },
                },
                "ui-field-behaviour": ui.ui_field_behaviour(provider_info=True),
            },
        ],
        "extra-links": [
            "yellowdog_provider.links.yellowdog_links.WorkRequirementPortalLink",
            "yellowdog_provider.links.yellowdog_links.WorkerPoolPortalLink",
        ],
        "integrations": [
            {
                "integration-name": "YellowDog",
                "external-doc-url": "https://docs.yellowdog.ai/airflow-provider/index.html",
                "tags": ["software"],
            },
        ],
        "hooks": [
            {
                "integration-name": "YellowDog",
                "python-modules": ["yellowdog_provider.hooks.yellowdog_hooks"],
            },
        ],
        "operators": [
            {
                "integration-name": "YellowDog",
                "python-modules": ["yellowdog_provider.operators.yellowdog_operators"],
            },
        ],
        "sensors": [
            {
                "integration-name": "YellowDog",
                "python-modules": ["yellowdog_provider.sensors.yellowdog_sensors"],
            },
        ],
        "triggers": [
            {
                "integration-name": "YellowDog",
                "python-modules": ["yellowdog_provider.triggers.yellowdog_triggers"],
            },
        ],
    }
