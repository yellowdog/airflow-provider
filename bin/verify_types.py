"""Type-completeness gate for the published package.

Runs ``pyright --verifytypes`` and fails on any unknown/ambiguous symbol, with
one exemption. Airflow's ``BaseOperator``/``BaseSensorOperator`` hierarchy has a
metaclass that pyright cannot resolve, so every subclass (the YellowDogOperator
and YellowDogSensor hierarchies) is reported as "Type of metaclass unknown", and
its own subclasses as "Type of base class ... is partially unknown". This cannot
be fixed from this repository, so those diagnostics are exempted - by message
text, so new operators and sensors are covered automatically. Anything else on
those symbols (or any other symbol) still fails the gate.
"""

import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from typing import Any

PACKAGE = "yellowdog_provider"
METACLASS_MESSAGE = "Type of metaclass unknown"
BASE_CLASS_PATTERN = re.compile(r'^Type of base class "(.+)" is partially unknown$')


@dataclass
class Result:
    score: float
    exempted: list[str] = field(default_factory=lambda: [])
    failures: list[str] = field(default_factory=lambda: [])


def _error_messages(symbol: dict[str, Any]) -> list[str]:
    return [
        d["message"] for d in symbol.get("diagnostics", []) if d["severity"] == "error"
    ]


def evaluate(report: dict[str, Any]) -> Result:
    """Apply the exemption rules to pyright's ``--verifytypes`` JSON output."""
    completeness: dict[str, Any] = report.get("typeCompleteness", {})
    symbols: list[dict[str, Any]] = completeness.get("symbols", [])
    result = Result(score=completeness.get("completenessScore", 0.0))

    # Guard against passing vacuously when pyright could not analyse the package
    # (e.g. "No py.typed file found" gives zero symbols and a score of 0).
    for diag in report.get("generalDiagnostics", []):
        if diag.get("severity") == "error":
            result.failures.append(f"pyright error: {diag.get('message')}")
    if not completeness.get("pyTypedPath"):
        result.failures.append("py.typed file / package not resolved by pyright")
    if not symbols:
        result.failures.append("pyright reported no exported symbols")

    # Symbols whose metaclass is unknown (Airflow operator/sensor subclasses)
    exempt_roots = {
        s["name"] for s in symbols if METACLASS_MESSAGE in _error_messages(s)
    }

    for symbol in symbols:
        name: str = symbol["name"]
        messages = _error_messages(symbol)
        unexplained = [m for m in messages if not _is_exempt(m, exempt_roots)]
        if unexplained:
            result.failures.extend(f"{name}: {m}" for m in unexplained)
        elif messages:
            result.exempted.append(name)
        elif not symbol.get("isTypeKnown", True) or symbol.get("isTypeAmbiguous"):
            result.failures.append(f"{name}: type is unknown or ambiguous")
    return result


def _is_exempt(message: str, exempt_roots: set[str]) -> bool:
    if message == METACLASS_MESSAGE:
        return True
    match = BASE_CLASS_PATTERN.match(message)
    return match is not None and match.group(1) in exempt_roots


def run_pyright() -> dict[str, Any]:
    """Run ``pyright --verifytypes`` and return its parsed JSON report."""
    # PYTHONPATH=. because verifytypes can't resolve finder-style editable installs
    env = {**os.environ, "PYTHONPATH": "."}
    proc = subprocess.run(
        ["pyright", "--verifytypes", PACKAGE, "--ignoreexternal", "--outputjson"],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    return json.loads(proc.stdout)


def main() -> int:
    result = evaluate(run_pyright())
    print(f"Type completeness score: {result.score:.1%}")
    print(
        f"Exempted (Airflow operator metaclass unknown to pyright): "
        f"{len(result.exempted)}"
    )
    for name in result.exempted:
        print(f"  {name}")
    if result.failures:
        print(f"FAILED: {len(result.failures)} unexpected type problem(s)")
        for failure in result.failures:
            print(f"  {failure}")
        return 1
    print("OK: no unknown types beyond the exempted Airflow metaclass cases")
    return 0


if __name__ == "__main__":
    sys.exit(main())
