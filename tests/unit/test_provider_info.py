import importlib
import importlib.resources
import json

import jsonschema

from yellowdog_provider import get_provider_info
from yellowdog_provider.hooks.yellowdog_hooks import YellowDogHook


def test_provider_info_matches_airflow_schema():
    schema_path = importlib.resources.files("airflow") / "provider_info.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    jsonschema.validate(get_provider_info(), schema)


def test_provider_info_lists_all_modules():
    info = get_provider_info()
    expected = {
        "hooks": "yellowdog_provider.hooks.yellowdog_hooks",
        "operators": "yellowdog_provider.operators.yellowdog_operators",
        "sensors": "yellowdog_provider.sensors.yellowdog_sensors",
        "triggers": "yellowdog_provider.triggers.yellowdog_triggers",
    }
    for key, module in expected.items():
        assert {m for h in info[key] for m in h["python-modules"]} == {module}


def test_provider_info_lists_trigger_module():
    modules = [m for t in get_provider_info()["triggers"] for m in t["python-modules"]]
    assert modules == ["yellowdog_provider.triggers.yellowdog_triggers"]
    for module in modules:
        importlib.import_module(module)


def test_connection_type_hook_class_importable():
    path = get_provider_info()["connection-types"][0]["hook-class-name"]
    module_name, _, class_name = path.rpartition(".")
    assert getattr(importlib.import_module(module_name), class_name) is YellowDogHook


def test_provider_info_lists_extra_links():
    links = get_provider_info()["extra-links"]
    assert links == [
        "yellowdog_provider.links.yellowdog_links.WorkRequirementPortalLink",
        "yellowdog_provider.links.yellowdog_links.WorkerPoolPortalLink",
    ]
    for path in links:
        module_name, _, class_name = path.rpartition(".")
        assert hasattr(importlib.import_module(module_name), class_name)


def test_provider_info_declares_portal_url_field():
    conn = get_provider_info()["connection-types"][0]
    assert "portal_url" in conn["conn-fields"]
    assert (
        conn["ui-field-behaviour"]["relabeling"]
        == (YellowDogHook.get_ui_field_behaviour()["relabeling"])
    )
