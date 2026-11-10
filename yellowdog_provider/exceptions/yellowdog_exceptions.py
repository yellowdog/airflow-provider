"""YellowDog Airflow provider exceptions."""


class YellowDogException(Exception):
    """Base class for errors raised by the YellowDog provider."""


class YellowDogArgumentError(YellowDogException, ValueError):
    """An operator or sensor argument is missing, malformed or inconsistent."""
