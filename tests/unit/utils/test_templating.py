import pytest

from yellowdog_provider.exceptions.yellowdog_exceptions import (
    YellowDogArgumentError,
    YellowDogException,
)
from yellowdog_provider.utils.templating import resolve, resolve_optional_str


def test_resolve_returns_plain_value():
    assert resolve("abc", "name") == "abc"


def test_resolve_rejects_unrendered_callable():
    with pytest.raises(YellowDogException, match="my_field"):
        resolve(lambda *, context, jinja_env: "x", "my_field")


@pytest.mark.parametrize("value", ["", "  ", None])
def test_resolve_optional_str_empty_is_none(value):
    assert resolve_optional_str(value, "f") is None


def test_resolve_optional_str_strips():
    assert resolve_optional_str("  abc ", "f") == "abc"


def test_resolve_optional_str_literal_none_raises():
    with pytest.raises(
        YellowDogArgumentError, match=r"work_requirement_id.*upstream XCom"
    ):
        resolve_optional_str("None", "work_requirement_id")


def test_argument_error_is_value_error():
    assert issubclass(YellowDogArgumentError, ValueError)
    assert issubclass(YellowDogArgumentError, YellowDogException)
