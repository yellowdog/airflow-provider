__version__ = "1.0.0"


def get_provider_info():
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
    }
