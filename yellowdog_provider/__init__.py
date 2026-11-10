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
            },
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
    }
