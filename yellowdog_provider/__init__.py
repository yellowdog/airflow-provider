from typing import Any

__version__ = "2.0.0.dev0"


def get_provider_info() -> dict[str, Any]:
    return {
        "package-name": "yellowdog-airflow-provider",
        "versions": [__version__],
        "name": "YellowDog",
        "description": "YellowDog provider for Apache Airflow",
        "connection-types": [
            {
                "connection-type": "yellowdog",
                "hook-class-name": (
                    "yellowdog_provider.hooks.yellowdog_hooks.YellowDogHook"
                ),
                "hook-name": "YellowDog",
                # Read by Airflow 3.2+ (older versions use the hook's methods)
                "conn-fields": {
                    "portal_url": {
                        "label": "Portal URL",
                        "description": (
                            "Optional. Base URL of the YellowDog Portal, used for "
                            "task links. Default: the Platform API URL with a "
                            "leading 'api.' replaced by 'portal.'."
                        ),
                        "schema": {"type": ["string", "null"]},
                    },
                },
                "ui-field-behaviour": {
                    "hidden-fields": ["schema", "port"],
                    "relabeling": {
                        "host": "Platform API URL",
                        "login": "Application Key ID",
                        "password": "Application Key Secret",
                    },
                    "placeholders": {"host": "https://api.yellowdog.ai"},
                },
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
