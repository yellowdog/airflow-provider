"""
The YellowDog connection type's form fields, shared by the provider info
(read by Airflow 3.2+) and :class:`.YellowDogHook` (read by Airflow 3.1). This
module imports nothing, so that ``get_provider_info()`` stays cheap.
"""

from typing import Any

PLATFORM_API_URL = "Platform API URL"
PLATFORM_API_URL_DEFAULT = "https://api.yellowdog.ai"
APP_KEY_ID_NAME = "Application Key ID"
APP_KEY_SECRET_NAME = "Application Key Secret"
CONNECTION_TYPE = "yellowdog"
PORTAL_URL_NAME = "Portal URL"
PORTAL_URL_FIELD = "portal_url"
PORTAL_URL_DESCRIPTION = (
    "Optional. Base URL of the YellowDog Portal, used for task links. Default: "
    "the Platform API URL with a leading 'api.' replaced by 'portal.'."
)

# The raw JSON 'extra' is hidden: its one setting, the Portal URL, has its own
# field
HIDDEN_FIELDS = ["schema", "port", "extra"]
RELABELING = {
    "host": PLATFORM_API_URL,
    "login": APP_KEY_ID_NAME,
    "password": APP_KEY_SECRET_NAME,
}
PLACEHOLDERS = {"host": PLATFORM_API_URL_DEFAULT}


def ui_field_behaviour(*, provider_info: bool) -> dict[str, Any]:
    """
    The form behaviour, with the provider info's keys (``hidden-fields``) or
    the hook's (``hidden_fields``).
    """
    sep = "-" if provider_info else "_"
    return {
        f"hidden{sep}fields": list(HIDDEN_FIELDS),
        "relabeling": dict(RELABELING),
        "placeholders": dict(PLACEHOLDERS),
    }
