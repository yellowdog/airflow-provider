import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any


def _load() -> ModuleType:
    path = Path(__file__).parents[2] / "bin" / "verify_types.py"
    spec = importlib.util.spec_from_file_location("verify_types", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["verify_types"] = module
    spec.loader.exec_module(module)
    return module


verify_types = _load()


def _symbol(
    name: str, errors: list[str] | None = None, known: bool = True
) -> dict[str, Any]:
    return {
        "name": name,
        "isTypeKnown": known,
        "isTypeAmbiguous": False,
        "diagnostics": [
            {"severity": "error", "message": m, "file": "x.py"} for m in errors or []
        ]
        + [{"severity": "warning", "message": "No docstring found", "file": "x.py"}],
    }


def _report(*symbols: dict[str, Any]) -> dict[str, Any]:
    return {
        "generalDiagnostics": [],
        "typeCompleteness": {
            "completenessScore": 0.9,
            "pyTypedPath": "/p/py.typed",
            "symbols": list(symbols),
        },
    }


_good_report = _report


def test_all_known_passes() -> None:
    result = verify_types.evaluate(_report(_symbol("pkg.f"), _symbol("pkg.C")))
    assert result.failures == []
    assert result.exempted == []


def test_metaclass_unknown_on_operator_is_exempt() -> None:
    op = _symbol("pkg.Op", ["Type of metaclass unknown"], known=False)
    sub = _symbol(
        "pkg.SubOp",
        [
            "Type of metaclass unknown",
            'Type of base class "pkg.Op" is partially unknown',
        ],
        known=False,
    )
    result = verify_types.evaluate(_report(op, sub))
    assert result.failures == []
    assert result.exempted == ["pkg.Op", "pkg.SubOp"]


def test_metaclass_plus_other_unknown_fails() -> None:
    op = _symbol(
        "pkg.Op",
        ["Type of metaclass unknown", "Type of parameter is unknown"],
        known=False,
    )
    result = verify_types.evaluate(_report(op))
    assert result.failures == ["pkg.Op: Type of parameter is unknown"]


def test_unknown_param_type_on_function_fails() -> None:
    fn = _symbol("pkg.f", ['Type annotation for parameter "x" is missing'], known=False)
    result = verify_types.evaluate(_report(fn))
    assert len(result.failures) == 1
    assert result.failures[0].startswith("pkg.f:")


def test_base_class_unknown_without_exempt_root_fails() -> None:
    sub = _symbol("pkg.Sub", ['Type of base class "other.Base" is partially unknown'])
    assert verify_types.evaluate(_report(sub)).failures


def test_zero_symbols_fails() -> None:
    report = _report()
    assert (
        "pyright reported no exported symbols" in verify_types.evaluate(report).failures
    )


def test_general_error_diagnostic_fails() -> None:
    report = _good_report(_symbol("pkg.f"))
    report["generalDiagnostics"] = [{"severity": "error", "message": "boom"}]
    assert verify_types.evaluate(report).failures == ["pyright error: boom"]


def test_general_warning_diagnostic_ignored() -> None:
    report = _good_report(_symbol("pkg.f"))
    report["generalDiagnostics"] = [{"severity": "warning", "message": "meh"}]
    assert verify_types.evaluate(report).failures == []


def test_unresolved_package_output_fails() -> None:
    # Real pyright output when the package cannot be found (no PYTHONPATH)
    report: dict[str, Any] = {
        "generalDiagnostics": [
            {"file": "", "severity": "error", "message": "No py.typed file found"}
        ],
        "typeCompleteness": {"completenessScore": 0, "symbols": []},
    }
    failures = verify_types.evaluate(report).failures
    assert "pyright error: No py.typed file found" in failures
    assert "py.typed file / package not resolved by pyright" in failures
    assert "pyright reported no exported symbols" in failures
